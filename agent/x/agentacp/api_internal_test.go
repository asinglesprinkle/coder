package agentacp

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	acp "github.com/coder/acp-go-sdk"
	"github.com/coder/coder/v2/agent/agentchat"
	"github.com/coder/coder/v2/agent/agenttoolcall"
	"github.com/coder/coder/v2/codersdk/workspacesdk"
	"github.com/coder/coder/v2/testutil"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func dialTestStream(ctx context.Context, t *testing.T, streamURL string) *websocket.Conn {
	t.Helper()
	conn, response, err := websocket.Dial(ctx, streamURL, nil)
	require.NoError(t, err)
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	return conn
}

func TestAPIAndStreamReplay(t *testing.T) {
	t.Parallel()
	m, dir, _ := newTestManager(t, fakeHarnessModeBoth)
	toolCalls := agenttoolcall.New(m.clock, func(context.Context, uuid.UUID, uuid.UUID) {})
	handler := agentchat.Middleware(toolCalls.Middleware(http.StripPrefix("/api/v0/acp", NewAPI(m).Routes())))
	server := httptest.NewServer(handler)
	defer server.Close()
	ctx := testutil.Context(t, testutil.WaitLong)
	type testResponse struct {
		StatusCode int
		Body       io.Reader
	}
	call := func(method, path string, body any, header string, toolIDs ...uuid.UUID) testResponse {
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		req, err := http.NewRequestWithContext(ctx, method, server.URL+"/api/v0/acp"+path, bytes.NewReader(raw))
		require.NoError(t, err)
		req.Header.Set(workspacesdk.CoderChatIDHeader, header)
		if len(toolIDs) != 0 {
			req.Header.Set(workspacesdk.CoderToolCallIDHeader, toolIDs[0].String())
		}
		res, err := server.Client().Do(req)
		require.NoError(t, err)
		defer res.Body.Close()
		bodyBytes, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		return testResponse{StatusCode: res.StatusCode, Body: bytes.NewReader(bodyBytes)}
	}
	for _, header := range []string{"", "invalid", uuid.Nil.String()} {
		require.Equal(t, http.StatusOK, call("GET", "/harnesses", nil, header).StatusCode)
	}
	req := workspacesdk.ACPCreateSessionRequest{HarnessSlug: "fake", WorkingDirectory: dir}
	chatID, createID := uuid.NewString(), uuid.New()
	res := call("POST", "/sessions", req, chatID, createID)
	require.Equal(t, http.StatusOK, res.StatusCode)
	var info workspacesdk.ACPSession
	require.NoError(t, json.NewDecoder(res.Body).Decode(&info))
	res = call("POST", "/sessions", req, chatID, createID)
	require.Equal(t, http.StatusOK, res.StatusCode)
	var duplicate workspacesdk.ACPSession
	require.NoError(t, json.NewDecoder(res.Body).Decode(&duplicate))
	require.Equal(t, info.ID, duplicate.ID)
	q := url.Values{"harness_slug": {info.ID.HarnessSlug}, "working_directory": {info.ID.WorkingDirectory}, "session_id": {info.ID.SessionID}}
	path := func(suffix string) string { return "/sessions/session" + suffix + "?" + q.Encode() }
	for _, header := range []string{"", "invalid", uuid.Nil.String(), uuid.NewString()} {
		res := call("GET", path(""), nil, header)
		require.Equal(t, http.StatusOK, res.StatusCode)
		var read workspacesdk.ACPSessionResponse
		require.NoError(t, json.NewDecoder(res.Body).Decode(&read))
		require.Equal(t, info.ID, read.Session.ID)
		require.Equal(t, info.Cursor, read.Session.Cursor)
	}
	require.Equal(t, http.StatusOK, call("GET", path(""), nil, "").StatusCode)
	require.Equal(t, http.StatusNotFound, call("GET", path("/wait"), nil, "").StatusCode)
	streamURL := strings.Replace(server.URL, "http://", "ws://", 1) + "/api/v0/acp" + path("/stream")
	conn := dialTestStream(ctx, t, streamURL)
	var last uint64
	for {
		var event workspacesdk.ACPEvent
		require.NoError(t, wsjson.Read(ctx, conn, &event))
		require.GreaterOrEqual(t, event.Cursor.Seq, last)
		last = event.Cursor.Seq
		if event.Kind == workspacesdk.ACPEventKindSnapshot {
			require.Equal(t, workspacesdk.ACPSessionStatusIdle, event.Session.Status)
			break
		}
	}
	_, _, err := conn.Read(ctx)
	require.Equal(t, websocket.StatusNormalClosure, websocket.CloseStatus(err))
	message := workspacesdk.ACPMessageRequest{ID: uuid.New(), Text: "hello"}
	require.Equal(t, http.StatusOK, call("POST", path("/messages"), message, chatID, message.ID).StatusCode)
	require.Equal(t, http.StatusOK, call("POST", path("/messages"), message, chatID, message.ID).StatusCode)
	conn = dialTestStream(ctx, t, streamURL)
	baseline := true
	userMessages := 0
	var updates []string
	var completed workspacesdk.ACPSession
	for {
		var event workspacesdk.ACPEvent
		require.NoError(t, wsjson.Read(ctx, conn, &event))
		if event.Kind == workspacesdk.ACPEventKindSnapshot {
			baseline = false
			completed = *event.Session
			if sessionFinished(completed) {
				break
			}
			continue
		}
		require.Greater(t, event.Cursor.Seq, last)
		last = event.Cursor.Seq
		if event.Kind == workspacesdk.ACPEventKindUserMessage {
			userMessages++
		}
		if event.Update != nil {
			updates = append(updates, string(event.Update))
		}
		if !baseline && event.Kind == workspacesdk.ACPEventKindStatus && sessionFinished(*event.Session) {
			completed = *event.Session
			break
		}
	}
	_, _, err = conn.Read(ctx)
	require.Equal(t, websocket.StatusNormalClosure, websocket.CloseStatus(err))
	require.Equal(t, 1, userMessages)
	require.Equal(t, workspacesdk.ACPSessionStatusIdle, completed.Status)
	require.Contains(t, strings.Join(updates, "\n"), "answer:hello:default")
	res = call("GET", path("")+"&epoch="+completed.Cursor.Epoch.String()+"&after=0", nil, "")
	require.Equal(t, http.StatusOK, res.StatusCode)
	var result workspacesdk.ACPSessionResponse
	require.NoError(t, json.NewDecoder(res.Body).Decode(&result))
	require.Equal(t, "answer:hello:default", result.AssistantResponse)
	require.Equal(t, completed.Cursor, result.Session.Cursor)
	res = call("GET", "/sessions", nil, "")
	require.Equal(t, http.StatusOK, res.StatusCode)
	var listed []workspacesdk.ACPSession
	require.NoError(t, json.NewDecoder(res.Body).Decode(&listed))
	require.Len(t, listed, 1)
	require.Equal(t, info.ID, listed[0].ID)
	_ = conn.CloseNow()
	replayURL := streamURL + "&epoch=" + result.Session.Cursor.Epoch.String() + "&after=0"
	conn = dialTestStream(ctx, t, replayURL)
	var event workspacesdk.ACPEvent
	require.NoError(t, wsjson.Read(ctx, conn, &event))
	require.Equal(t, uint64(1), event.Cursor.Seq)
	require.Equal(t, http.StatusMethodNotAllowed, call("DELETE", path(""), nil, "").StatusCode)
	require.Equal(t, http.StatusOK, call("GET", path(""), nil, "").StatusCode)
}

func TestHarnessFailureKeepsDiagnosticsOutOfAPI(t *testing.T) {
	t.Parallel()
	m, dir, _ := newTestManager(t, fakeHarnessModeNone)
	info := createTestSession(t, m, dir)
	ctx := testutil.Context(t, testutil.WaitLong)
	server := httptest.NewServer(NewAPI(m).Routes())
	defer server.Close()
	_, err := m.Message(ctx, info.ID, workspacesdk.ACPMessageRequest{ID: uuid.New(), Text: "exit-secret"})
	require.NoError(t, err)
	failed, err := awaitTestSession(ctx, m, info.ID)
	require.NoError(t, err)
	require.Equal(t, workspacesdk.ACPSessionStatusError, failed.Session.Status)
	q := url.Values{"harness_slug": {info.ID.HarnessSlug}, "working_directory": {dir}, "session_id": {info.ID.SessionID}}
	for _, path := range []string{"/sessions", "/sessions/session?" + q.Encode()} {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+path, nil)
		require.NoError(t, err)
		res, err := server.Client().Do(req)
		require.NoError(t, err)
		raw, err := io.ReadAll(res.Body)
		_ = res.Body.Close()
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode)
		require.NotContains(t, string(raw), "runtime-secret")
		require.Contains(t, string(raw), "see workspace agent logs")
	}
	m.mu.Lock()
	cfg := m.configs["fake"]
	cfg.command = "printf 'credential=%s\\n' 'setup-secret' >&2; exit 1"
	m.configs["fake"] = cfg
	m.mu.Unlock()
	raw, err := json.Marshal(workspacesdk.ACPCreateSessionRequest{HarnessSlug: "fake", WorkingDirectory: dir})
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/sessions", bytes.NewReader(raw))
	require.NoError(t, err)
	res, err := server.Client().Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	raw, err = io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
	require.NotContains(t, string(raw), "setup-secret")
	require.Contains(t, string(raw), "see workspace agent logs")
}

func TestAPIStreamCompletion(t *testing.T) {
	t.Parallel()
	for _, ending := range []string{"idle", "failed_baseline", "completed", "failed_live", "disconnected", "historical_status"} {
		t.Run(ending, func(t *testing.T) {
			t.Parallel()
			status := workspacesdk.ACPSessionStatusRunning
			if ending == "idle" || ending == "failed_baseline" {
				status = workspacesdk.ACPSessionStatusIdle
			}
			m, s, _, _ := newTransportSession(t, status)
			ctx := testutil.Context(t, testutil.WaitLong)
			if ending == "failed_baseline" {
				require.NoError(t, s.adapterError(ctx, io.ErrUnexpectedEOF))
			}
			if ending == "historical_status" {
				s.mu.Lock()
				s.info.Status = workspacesdk.ACPSessionStatusIdle
				s.appendEventLocked(workspacesdk.ACPEventKindStatus, "", nil)
				s.info.Status = workspacesdk.ACPSessionStatusRunning
				s.appendEventLocked(workspacesdk.ACPEventKindStatus, "", nil)
				s.mu.Unlock()
			}
			baseline, err := s.Read(ctx, nil)
			require.NoError(t, err)
			server := httptest.NewServer(NewAPI(m).Routes())
			defer server.Close()
			query := url.Values{"harness_slug": {s.id.HarnessSlug}, "working_directory": {s.id.WorkingDirectory}, "session_id": {s.id.SessionID}}
			streamURL := strings.Replace(server.URL, "http://", "ws://", 1) + "/sessions/session/stream?" + query.Encode()
			conn := dialTestStream(ctx, t, streamURL)
			for _, expected := range baseline.Events {
				var event workspacesdk.ACPEvent
				require.NoError(t, wsjson.Read(ctx, conn, &event))
				require.Equal(t, expected, event)
			}
			var snapshot workspacesdk.ACPEvent
			require.NoError(t, wsjson.Read(ctx, conn, &snapshot))
			require.Equal(t, workspacesdk.ACPEventKindSnapshot, snapshot.Kind)
			require.Equal(t, baseline.Session, *snapshot.Session)
			switch ending {
			case "completed", "historical_status":
				notification := sessionNotification(s.id, "final output")
				require.NoError(t, s.ProcessACPUpdate(ctx, ACPUpdate{Notification: &notification}))
				require.NoError(t, s.ProcessACPUpdate(ctx, ACPUpdate{Prompt: &acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, Turn: 1}))
			case "failed_live":
				require.NoError(t, s.adapterError(ctx, io.ErrUnexpectedEOF))
			case "disconnected":
				require.NoError(t, m.Close())
			}
			var live []workspacesdk.ACPEvent
			for {
				var event workspacesdk.ACPEvent
				err = wsjson.Read(ctx, conn, &event)
				if err != nil {
					break
				}
				live = append(live, event)
			}
			if ending == "disconnected" {
				require.Equal(t, websocket.StatusTryAgainLater, websocket.CloseStatus(err))
			} else {
				require.Equal(t, websocket.StatusNormalClosure, websocket.CloseStatus(err))
			}
			switch ending {
			case "completed", "historical_status", "failed_live":
				require.Len(t, live, 2)
				require.Equal(t, workspacesdk.ACPEventKindStatus, live[1].Kind)
				if ending != "failed_live" {
					require.Contains(t, string(live[0].Update), "final output")
					require.Equal(t, workspacesdk.ACPSessionStatusIdle, live[1].Session.Status)
				} else {
					require.Equal(t, workspacesdk.ACPEventKindError, live[0].Kind)
					require.Equal(t, workspacesdk.ACPSessionStatusError, live[1].Session.Status)
				}
			default:
				require.Empty(t, live)
			}
		})
	}
}
