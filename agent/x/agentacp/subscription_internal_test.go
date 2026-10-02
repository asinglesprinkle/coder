package agentacp

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	acp "github.com/coder/acp-go-sdk"
	"github.com/coder/coder/v2/codersdk/workspacesdk"
	"github.com/coder/coder/v2/testutil"
)

func TestSubscriptionPreservesCompletedTurn(t *testing.T) {
	t.Parallel()
	m, s, _, _ := newTransportSession(t, workspacesdk.ACPSessionStatusRunning)
	ctx := testutil.Context(t, testutil.WaitLong)
	// Historical idle events must not complete a subscription.
	s.mu.Lock()
	s.info.Status = workspacesdk.ACPSessionStatusIdle
	s.appendEventLocked(workspacesdk.ACPEventKindStatus, "", nil)
	s.info.Status = workspacesdk.ACPSessionStatusRunning
	s.appendEventLocked(workspacesdk.ACPEventKindStatus, "", nil)
	s.mu.Unlock()
	baseline, err := s.Read(ctx, nil)
	require.NoError(t, err)
	_, subscribed, cancel, err := m.Subscribe(ctx, s.id, nil)
	require.NoError(t, err)
	defer cancel()
	for _, text := range []string{"final ", "output"} {
		notification := sessionNotification(s.id, text)
		require.NoError(t, s.ProcessACPUpdate(ctx, ACPUpdate{Notification: &notification}))
	}
	require.NoError(t, s.ProcessACPUpdate(ctx, ACPUpdate{Prompt: &acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, Turn: 1}))
	completed, err := s.Read(ctx, nil)
	require.NoError(t, err)
	_, err = m.Message(ctx, s.id, workspacesdk.ACPMessageRequest{ID: uuid.New(), Text: "next turn"})
	require.NoError(t, err)
	current, err := s.Read(ctx, nil)
	require.NoError(t, err)
	require.NotEqual(t, completed.Session.Cursor.Epoch, current.Session.Cursor.Epoch)

	// Drain only after the FSM has discarded the old transcript.
	for _, expected := range completed.Events[len(baseline.Events):] {
		require.Equal(t, expected, testutil.RequireReceive(ctx, t, subscribed))
	}
	requireClosed(ctx, t, subscribed)
}

func TestSubscribeCompletion(t *testing.T) {
	t.Parallel()
	for _, status := range []workspacesdk.ACPSessionStatus{
		workspacesdk.ACPSessionStatusIdle, workspacesdk.ACPSessionStatusError,
		workspacesdk.ACPSessionStatusRunning, workspacesdk.ACPSessionStatusInterrupting,
	} {
		t.Run(string(status), func(t *testing.T) {
			t.Parallel()
			m, s, _, _ := newTransportSession(t, status)
			ctx := testutil.Context(t, testutil.WaitLong)
			baseline, events, cancel, err := m.Subscribe(ctx, s.id, nil)
			require.NoError(t, err)
			defer cancel()
			require.Len(t, baseline, 1)
			require.Equal(t, workspacesdk.ACPEventKindSnapshot, baseline[0].Kind)
			require.Equal(t, status, baseline[0].Session.Status)
			if status == workspacesdk.ACPSessionStatusRunning || status == workspacesdk.ACPSessionStatusInterrupting {
				notification := sessionNotification(s.id, "complete")
				require.NoError(t, s.ProcessACPUpdate(ctx, ACPUpdate{Notification: &notification}))
				require.NoError(t, s.ProcessACPUpdate(ctx, ACPUpdate{Prompt: &acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, Turn: 1}))
				require.Equal(t, workspacesdk.ACPEventKindUpdate, testutil.RequireReceive(ctx, t, events).Kind)
				terminal := testutil.RequireReceive(ctx, t, events)
				require.Equal(t, workspacesdk.ACPEventKindStatus, terminal.Kind)
				require.Equal(t, workspacesdk.ACPSessionStatusIdle, terminal.Session.Status)
			}
			requireClosed(ctx, t, events)
		})
	}
}
