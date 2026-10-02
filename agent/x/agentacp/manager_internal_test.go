package agentacp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"cdr.dev/slog/v3/sloggers/slogtest"
	acp "github.com/coder/acp-go-sdk"
	"github.com/coder/coder/v2/agent/agentexec"
	"github.com/coder/coder/v2/agent/usershell"
	"github.com/coder/coder/v2/codersdk/workspacesdk"
	"github.com/coder/coder/v2/testutil"
	"github.com/coder/quartz"
)

type testEnv struct {
	usershell.SystemEnvInfo
	home string
}

func (e testEnv) HomeDir() (string, error) { return e.home, nil }

func newTestManager(t *testing.T, mode fakeHarnessMode) (manager *Manager, directory, configsDirectory string) {
	t.Helper()
	home, dir := t.TempDir(), t.TempDir()
	m := NewManager(context.Background(), Options{
		Logger:     slogtest.Make(t, nil),
		Clock:      quartz.NewMock(t),
		Execer:     agentexec.DefaultExecer,
		Filesystem: afero.NewOsFs(),
		EnvInfo:    testEnv{home: home},
		WorkingDir: func() string { return dir },
	})
	t.Cleanup(func() { require.NoError(t, m.Close()) })
	configDir := filepath.Join(home, ".coder", "acp")
	require.NoError(t, os.MkdirAll(configDir, 0o700))
	writeConfig(t, filepath.Join(configDir, "fake.json"), map[string]any{"command": fakeCommand(t, mode)})
	require.NoError(t, m.Reload(testutil.Context(t, testutil.WaitLong)))
	require.Len(t, m.Catalog(), 1)
	require.Empty(t, m.Catalog()[0].Error)
	return m, dir, configDir
}

func writeConfig(t *testing.T, path string, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, raw, 0o600))
}

func newTransportSession(t *testing.T, status workspacesdk.ACPSessionStatus) (*Manager, *SessionFSM, <-chan []byte, *io.PipeWriter) {
	t.Helper()
	m := NewManager(context.Background(), Options{Clock: quartz.NewMock(t), Filesystem: afero.NewOsFs(), Logger: slogtest.Make(t, nil)})
	t.Cleanup(func() { require.NoError(t, m.Close()) })
	output, peer := io.Pipe()
	t.Cleanup(func() { _ = peer.Close() })
	writer := recordingWriter{writes: make(chan []byte, 512)}
	p := &process{stdin: writer, stdout: output, cancel: func() {}, done: make(chan struct{}), clock: m.clock}
	close(p.done)
	id := workspacesdk.ACPSessionID{HarnessSlug: "test", WorkingDirectory: t.TempDir(), SessionID: uuid.NewString()}
	info := workspacesdk.ACPSession{ID: id, Status: status, Cursor: workspacesdk.ACPCursor{Epoch: uuid.New()}}
	s := newSessionFSM(m.ctx, workspacesdk.ACPCreateSessionRequest{HarnessSlug: id.HarnessSlug, WorkingDirectory: id.WorkingDirectory}, info, p, m.run)
	s.mu.Lock()
	s.turn = 1
	s.mu.Unlock()
	p.conn = acp.NewClientSideConnection(&client{session: s}, p, output)
	m.sessions[id] = s
	m.instances = append(m.instances, s)
	return m, s, writer.writes, peer
}

func createTestSession(t *testing.T, m *Manager, dir string) workspacesdk.ACPSession {
	t.Helper()
	info, err := m.Create(context.Background(), workspacesdk.ACPCreateSessionRequest{HarnessSlug: "fake", WorkingDirectory: dir, Config: map[string]string{"model": "other"}})
	require.NoError(t, err)
	require.Equal(t, workspacesdk.ACPSessionStatusIdle, info.Status)
	return info
}

func TestSessionLifecycle(t *testing.T) {
	t.Parallel()
	m, dir, _ := newTestManager(t, fakeHarnessModeBoth)

	info := createTestSession(t, m, dir)
	ctx := testutil.Context(t, testutil.WaitLong)
	message := workspacesdk.ACPMessageRequest{ID: uuid.New(), Text: "hello"}
	result, err := m.Message(ctx, info.ID, message)
	require.NoError(t, err)
	require.Equal(t, workspacesdk.ACPMessageOutcomePrompt, result.Outcome)
	wait, err := awaitTestSession(ctx, m, info.ID)
	require.NoError(t, err)
	require.Equal(t, "answer:hello:other", wait.AssistantResponse)
	require.Equal(t, workspacesdk.ACPSessionStatusIdle, wait.Session.Status)
	require.Equal(t, workspacesdk.ACPEventKindUserMessage, wait.Events[0].Kind)
	var updates []string
	for _, event := range wait.Events {
		updates = append(updates, string(event.Update))
	}
	require.Contains(t, strings.Join(updates, "\n"), "thinking")
	require.Equal(t, workspacesdk.ACPEventKindStatus, wait.Events[len(wait.Events)-1].Kind)
	require.Len(t, m.List(), 1)
	baseline, events, unsubscribe, err := m.Subscribe(ctx, info.ID, &wait.Session.Cursor)
	require.NoError(t, err)
	require.Len(t, baseline, 1)
	select {
	case _, open := <-events:
		require.False(t, open)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	unsubscribe()
	_, err = m.Message(ctx, info.ID, workspacesdk.ACPMessageRequest{ID: uuid.New(), Text: "hold"})
	require.NoError(t, err)
	awaitTestOutput(ctx, t, m, info.ID, "started")
	current, err := m.Read(ctx, info.ID, nil)
	require.NoError(t, err)
	_, err = m.Message(ctx, info.ID, workspacesdk.ACPMessageRequest{ID: uuid.New(), Text: "follow-up"})
	require.ErrorIs(t, err, ErrConflict)
	read, err := m.Read(ctx, info.ID, nil)
	require.NoError(t, err)
	require.Equal(t, current, read)
	_, err = m.Interrupt(ctx, info.ID)
	require.NoError(t, err)
	wait, err = awaitTestSession(ctx, m, info.ID)
	require.NoError(t, err)
	require.Equal(t, string(acp.StopReasonCancelled), wait.Session.StopReason)
	_, err = m.Interrupt(ctx, info.ID)
	require.ErrorIs(t, err, ErrConflict)
	require.NoError(t, m.Close())
}

func TestRequiredDirectoryAndOverrides(t *testing.T) {
	t.Parallel()
	m, dir, _ := newTestManager(t, fakeHarnessModeBoth)
	file := filepath.Join(dir, "file")
	require.NoError(t, os.WriteFile(file, nil, 0o600))
	for _, directory := range []string{"", ".", "relative", filepath.Join(dir, "absent"), file} {
		req := workspacesdk.ACPCreateSessionRequest{HarnessSlug: "fake", WorkingDirectory: directory}
		_, err := m.Create(context.Background(), req)
		require.ErrorIs(t, err, ErrInvalid)
		_, err = m.Read(context.Background(), workspacesdk.ACPSessionID{HarnessSlug: req.HarnessSlug, WorkingDirectory: directory, SessionID: "native"}, nil)
		require.ErrorIs(t, err, ErrInvalid)
	}
	_, err := m.Create(context.Background(), workspacesdk.ACPCreateSessionRequest{HarnessSlug: "fake", WorkingDirectory: dir, Config: map[string]string{"model": "unknown"}})
	require.ErrorIs(t, err, ErrInvalid)
	info := createTestSession(t, m, dir)
	require.NotEmpty(t, info.ID.SessionID)
}

func TestSubscriberCancellation(t *testing.T) {
	t.Parallel()
	m, dir, _ := newTestManager(t, fakeHarnessModeBoth)

	info := createTestSession(t, m, dir)
	ctx := testutil.Context(t, testutil.WaitLong)
	_, err := m.Message(ctx, info.ID, workspacesdk.ACPMessageRequest{ID: uuid.New(), Text: "hold"})
	require.NoError(t, err)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = awaitTestSession(canceled, m, info.ID)
	require.ErrorIs(t, err, context.Canceled)
	_, events, unsubscribe, err := m.Subscribe(context.Background(), info.ID, nil)
	require.NoError(t, err)
	defer unsubscribe()
	s, err := m.get(ctx, info.ID)
	require.NoError(t, err)
	// Reader cancellation must not cancel the active turn.
	unsubscribe()
	for event := range events {
		require.Equal(t, workspacesdk.ACPEventKindUpdate, event.Kind)
	}
	_, _, stop, err := m.Subscribe(ctx, info.ID, nil)
	require.NoError(t, err)
	stop()
	_, _, err = s.Watch(ctx, nil)
	require.NoError(t, err)
	read, err := m.Read(context.Background(), info.ID, nil)
	require.NoError(t, err)
	require.Equal(t, workspacesdk.ACPSessionStatusRunning, read.Session.Status)
	awaitTestOutput(ctx, t, m, info.ID, "started")
	_, err = m.Interrupt(ctx, info.ID)
	require.NoError(t, err)
	stopped, err := awaitTestSession(ctx, m, info.ID)
	require.NoError(t, err)
	require.Equal(t, string(acp.StopReasonCancelled), stopped.Session.StopReason)
	require.NoError(t, m.Close())
}

func TestConcurrentSessionsAndProcessFailure(t *testing.T) {
	t.Parallel()
	m, dir, _ := newTestManager(t, fakeHarnessModeNone)

	ctx := testutil.Context(t, testutil.WaitLong)
	type outcome struct {
		response workspacesdk.ACPSessionResponse
		err      error
	}
	outcomes := make(chan outcome, 3)
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() {
			info, err := m.Create(ctx, workspacesdk.ACPCreateSessionRequest{HarnessSlug: "fake", WorkingDirectory: dir, Config: map[string]string{"model": "other"}})
			if err == nil {
				_, err = m.Message(ctx, info.ID, workspacesdk.ACPMessageRequest{ID: uuid.New(), Text: "hello"})
			}
			var result workspacesdk.ACPSessionResponse
			if err == nil {
				result, err = awaitTestSession(ctx, m, info.ID)
			}
			outcomes <- outcome{response: result, err: err}
		})
	}
	wg.Wait()
	close(outcomes)
	for result := range outcomes {
		require.NoError(t, result.err)
		require.Equal(t, "answer:hello:other", result.response.AssistantResponse)
	}
	info := createTestSession(t, m, dir)
	_, err := m.Message(ctx, info.ID, workspacesdk.ACPMessageRequest{ID: uuid.New(), Text: "hold"})
	require.NoError(t, err)
	_, err = m.Message(ctx, info.ID, workspacesdk.ACPMessageRequest{ID: uuid.New(), Text: "follow-up"})
	require.ErrorIs(t, err, ErrConflict)
	awaitTestOutput(ctx, t, m, info.ID, "started")
	_, err = m.Interrupt(ctx, info.ID)
	require.NoError(t, err)
	_, err = awaitTestSession(ctx, m, info.ID)
	require.NoError(t, err)
	_, err = m.Message(ctx, info.ID, workspacesdk.ACPMessageRequest{ID: uuid.New(), Text: "exit"})
	require.NoError(t, err)
	result, err := awaitTestSession(ctx, m, info.ID)
	require.NoError(t, err)
	require.Equal(t, workspacesdk.ACPSessionStatusError, result.Session.Status)
	require.NotEmpty(t, result.Session.Error)
	require.False(t, errors.Is(err, ErrInvalid))
}

func TestAcceptedWorkAndShutdown(t *testing.T) {
	t.Parallel()
	m, dir, _ := newTestManager(t, fakeHarnessModeBoth)

	// Canceling the caller cannot cancel accepted setup or prompts.
	req := workspacesdk.ACPCreateSessionRequest{HarnessSlug: "fake", WorkingDirectory: dir}
	caller, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := m.Create(caller, req)
	require.ErrorIs(t, err, context.Canceled)
	ctx := testutil.Context(t, testutil.WaitLong)
	info, err := m.Create(ctx, req)
	require.NoError(t, err)
	caller, cancel = context.WithCancel(ctx)
	_, err = m.Message(caller, info.ID, workspacesdk.ACPMessageRequest{ID: uuid.New(), Text: "hello"})
	require.NoError(t, err)
	cancel()
	result, err := awaitTestSession(ctx, m, info.ID)
	require.NoError(t, err)
	require.Equal(t, "answer:hello:default", result.AssistantResponse)
	s, err := m.get(ctx, info.ID)
	require.NoError(t, err)
	p := s.proc
	require.NoError(t, m.Close())
	select {
	case <-p.done:
	default:
		t.Fatal("harness was not reaped")
	}
	require.NoError(t, m.Close())
}

func TestReadConcurrentWithShutdown(t *testing.T) {
	t.Parallel()
	m, dir, _ := newTestManager(t, fakeHarnessModeBoth)

	ctx := testutil.Context(t, testutil.WaitLong)
	info := createTestSession(t, m, dir)
	_, err := m.Message(ctx, info.ID, workspacesdk.ACPMessageRequest{ID: uuid.New(), Text: "hello"})
	require.NoError(t, err)
	before, err := awaitTestSession(ctx, m, info.ID)
	require.NoError(t, err)
	var wg sync.WaitGroup
	errorsCh := make(chan error, 10)
	for range 10 {
		wg.Go(func() {
			_, readErr := m.Read(context.Background(), info.ID, &before.Session.Cursor)
			errorsCh <- readErr
		})
	}
	require.NoError(t, m.Close())
	wg.Wait()
	close(errorsCh)
	for readErr := range errorsCh {
		if readErr != nil {
			require.True(t, errors.Is(readErr, ErrNotFound) || errors.Is(readErr, ErrUnavailable), "%v", readErr)
		}
	}
}

func awaitTestSession(ctx context.Context, m *Manager, id workspacesdk.ACPSessionID) (workspacesdk.ACPSessionResponse, error) {
	s, err := m.get(ctx, id)
	if err != nil {
		return workspacesdk.ACPSessionResponse{}, err
	}
	result, err := s.Wait(ctx)
	// Existing integration tests inspect the retained error in the snapshot.
	if result.Session.Status == workspacesdk.ACPSessionStatusError {
		return result, nil
	}
	return result, err
}

func awaitTestOutput(ctx context.Context, t *testing.T, m *Manager, id workspacesdk.ACPSessionID, text string) {
	t.Helper()
	s, err := m.get(ctx, id)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	r, events, err := s.Watch(ctx, nil)
	require.NoError(t, err)
	output := r.AssistantResponse
	for !strings.Contains(output, text) {
		event := testutil.RequireReceive(ctx, t, events)
		if event.Kind != workspacesdk.ACPEventKindUpdate {
			continue
		}
		var update acp.SessionUpdate
		require.NoError(t, json.Unmarshal(event.Update, &update))
		if chunk := update.AgentMessageChunk; chunk != nil && chunk.Content.Text != nil {
			output += chunk.Content.Text.Text
		}
	}
}
