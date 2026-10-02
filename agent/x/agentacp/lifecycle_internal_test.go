package agentacp

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/coder/coder/v2/codersdk/workspacesdk"
	"github.com/coder/coder/v2/testutil"
)

func TestMessageWriteFailureCanRetry(t *testing.T) {
	t.Parallel()
	m, dir, _ := newTestManager(t, fakeHarnessModeBoth)
	info := createTestSession(t, m, dir)
	ctx := testutil.Context(t, testutil.WaitLong)
	s, err := m.get(ctx, info.ID)
	require.NoError(t, err)
	p := s.proc
	p.stdin = failingWriter{p.stdin}
	message := workspacesdk.ACPMessageRequest{ID: uuid.New(), Text: "retry after broken input"}
	_, err = m.Message(ctx, info.ID, message)
	require.ErrorContains(t, err, "broken harness input")
	_, err = m.Message(ctx, info.ID, message)
	require.NoError(t, err)
	result, err := awaitTestSession(ctx, m, info.ID)
	require.NoError(t, err)
	require.Equal(t, "answer:"+message.Text+":other", result.AssistantResponse)
}

func TestSuccessfulRecoveryAllowsAnotherImmediateRecovery(t *testing.T) {
	t.Parallel()
	m, dir, _ := newTestManager(t, fakeHarnessModeBoth)
	info := createTestSession(t, m, dir)
	ctx := testutil.Context(t, testutil.WaitLong)
	for _, text := range []string{"first", "second"} {
		_, err := m.Message(ctx, info.ID, workspacesdk.ACPMessageRequest{ID: uuid.New(), Text: "exit"})
		require.NoError(t, err)
		failed, err := awaitTestSession(ctx, m, info.ID)
		require.NoError(t, err)
		require.Equal(t, workspacesdk.ACPSessionStatusError, failed.Session.Status)
		_, err = m.Message(ctx, info.ID, workspacesdk.ACPMessageRequest{ID: uuid.New(), Text: text})
		require.NoError(t, err)
		completed, err := awaitTestSession(ctx, m, info.ID)
		require.NoError(t, err)
		require.Equal(t, "answer:"+text+":other", completed.AssistantResponse)
	}
}
