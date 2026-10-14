package chatstate_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"

	"cdr.dev/slog/v3/sloggers/slogtest"
	"github.com/coder/coder/v2/coderd/database"
	"github.com/coder/coder/v2/coderd/database/dbauthz"
	"github.com/coder/coder/v2/coderd/database/dbgen"
	"github.com/coder/coder/v2/coderd/database/dbtestutil"
	coderdpubsub "github.com/coder/coder/v2/coderd/pubsub"
	"github.com/coder/coder/v2/coderd/rbac"
	"github.com/coder/coder/v2/coderd/x/chatd/chatprompt"
	"github.com/coder/coder/v2/coderd/x/chatd/chatstate"
	"github.com/coder/coder/v2/codersdk"
	"github.com/coder/coder/v2/testutil"
)

// testFixture bundles the resources every integration test needs:
// a database, a publisher recorder, a user/org/model triple, and
// helper accessors. It is intentionally NOT a generic chatd test
// fixture; tests outside this package should not depend on it.
type testFixture struct {
	DB database.Store
	// SQLDB is the raw handle behind DB, for test-only writes that have
	// no production query, such as disabling an automation.
	SQLDB *sql.DB
	Pub   *recordingPubsub
	User  database.User
	Org   database.Organization
	Model database.ChatModelConfig
}

func newTestFixture(t *testing.T) *testFixture {
	t.Helper()
	db, _, sqlDB := dbtestutil.NewDBWithSQLDB(t)
	user := dbgen.User(t, db, database.User{})
	org := dbgen.Organization(t, db, database.Organization{})
	dbgen.OrganizationMember(t, db, database.OrganizationMember{
		UserID:         user.ID,
		OrganizationID: org.ID,
	})
	dbgen.ChatProvider(t, db, database.ChatProvider{
		Provider:    "openai",
		DisplayName: "openai",
		BaseUrl:     "http://example.invalid",
	})
	model := dbgen.ChatModelConfig(t, db, database.ChatModelConfig{
		OrganizationID: org.ID,
		IsDefault:      true,
	})
	pub := newRecordingPubsub()
	return &testFixture{
		DB:    db,
		SQLDB: sqlDB,
		Pub:   pub,
		User:  user,
		Org:   org,
		Model: model,
	}
}

// readChat re-reads the chat from the database. Tests use this to
// verify post-transition state because transition results no longer
// carry the chat snapshot.
func (f *testFixture) readChat(ctx context.Context, t *testing.T, chatID uuid.UUID) database.Chat {
	t.Helper()
	chat, err := f.DB.GetChatByID(ctx, chatID)
	require.NoError(t, err)
	return chat
}

// classify reads the chat plus queue cardinality and returns the
// execution state.
func (f *testFixture) classify(ctx context.Context, t *testing.T, chatID uuid.UUID) chatstate.ExecutionState {
	t.Helper()
	chat := f.readChat(ctx, t, chatID)
	count, err := f.DB.CountChatQueuedMessages(ctx, chatID)
	require.NoError(t, err)
	return chatstate.ClassifyExecutionState(chat, count > 0, true)
}

// recordingPubsub captures every Publish call so tests can assert on
// the chatstate notifications without needing a live subscriber. The
// mutex makes it safe to use from concurrent tests that race multiple
// goroutines through the same publisher (see TestConcurrentUpdatesSerializeOnChatRow).
type recordingPubsub struct {
	mu       sync.Mutex
	channels []string
	payloads [][]byte
}

func newRecordingPubsub() *recordingPubsub { return &recordingPubsub{} }

func (r *recordingPubsub) Publish(channel string, payload []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.channels = append(r.channels, channel)
	r.payloads = append(r.payloads, slices.Clone(payload))
	return nil
}

// expectChatUpdate finds the most recent chat:update message on the
// per-chat channel and asserts that it has snapshot_version == want.
func (r *recordingPubsub) expectChatUpdate(t *testing.T, chatID uuid.UUID, wantSnapshot int64) {
	t.Helper()
	channel := coderdpubsub.ChatStateUpdateChannel(chatID)
	for i := len(r.channels) - 1; i >= 0; i-- {
		if r.channels[i] != channel {
			continue
		}
		var msg coderdpubsub.ChatStateUpdateMessage
		require.NoError(t, json.Unmarshal(r.payloads[i], &msg))
		require.Equal(t, wantSnapshot, msg.SnapshotVersion)
		return
	}
	t.Fatalf("no chat:update on %s", channel)
}

func (r *recordingPubsub) hasOwnership() bool {
	for _, c := range r.channels {
		if c == coderdpubsub.ChatStateOwnershipChannel {
			return true
		}
	}
	return false
}

func userTextMessage(text string, createdBy uuid.UUID, modelConfigID uuid.UUID) chatstate.Message {
	parts := []codersdk.ChatMessagePart{codersdk.ChatMessageText(text)}
	raw, err := chatprompt.MarshalParts(parts)
	if err != nil {
		panic(err)
	}
	return chatstate.Message{
		Role:           database.ChatMessageRoleUser,
		Content:        raw,
		Visibility:     database.ChatMessageVisibilityBoth,
		ContentVersion: chatprompt.CurrentContentVersion,
		CreatedBy:      uuid.NullUUID{UUID: createdBy, Valid: true},
		ModelConfigID:  uuid.NullUUID{UUID: modelConfigID, Valid: true},
	}
}

// createTestChat is the standard "fresh R0 chat" helper used by other
// tests. It exercises CreateChat itself.
func createTestChat(t *testing.T, f *testFixture) chatstate.CreateChatResult {
	t.Helper()
	ctx := testutil.Context(t, testutil.WaitShort)
	res, err := chatstate.CreateChat(ctx, f.DB, f.Pub, chatstate.CreateChatInput{
		OrganizationID:    f.Org.ID,
		OwnerID:           f.User.ID,
		LastModelConfigID: f.Model.ID,
		Title:             "test",
		ClientType:        database.ChatClientTypeApi,
		InitialStatus:     database.ChatStatusRunning,
		InitialMessages: []chatstate.Message{
			userTextMessage("hello", f.User.ID, f.Model.ID),
		},
	})
	require.NoError(t, err)
	return res
}

func TestChatMachine_Update_RejectsMissingChat(t *testing.T) {
	t.Parallel()
	f := newTestFixture(t)
	ctx := testutil.Context(t, testutil.WaitShort)
	m := chatstate.NewChatMachine(f.DB, f.Pub, uuid.New())
	err := m.Update(ctx, func(tx *chatstate.Tx, store database.Store) error { return nil })
	require.ErrorIs(t, err, chatstate.ErrChatNotFound)
	require.Empty(t, f.Pub.channels)
}

// chatReadCounts counts chat reads issued on transactional handles.
type chatReadCounts struct {
	mu          sync.Mutex
	getChatByID int
	countQueued int
}

type chatReadCountingStore struct {
	database.Store
	counts *chatReadCounts
	inTx   bool
}

func (s *chatReadCountingStore) InTx(fn func(database.Store) error, opts *database.TxOptions) error {
	return s.Store.InTx(func(tx database.Store) error {
		return fn(&chatReadCountingStore{Store: tx, counts: s.counts, inTx: true})
	}, opts)
}

func (s *chatReadCountingStore) GetChatByID(ctx context.Context, id uuid.UUID) (database.Chat, error) {
	if s.inTx {
		s.counts.mu.Lock()
		s.counts.getChatByID++
		s.counts.mu.Unlock()
	}
	return s.Store.GetChatByID(ctx, id)
}

func (s *chatReadCountingStore) CountChatQueuedMessages(ctx context.Context, id uuid.UUID) (int64, error) {
	if s.inTx {
		s.counts.mu.Lock()
		s.counts.countQueued++
		s.counts.mu.Unlock()
	}
	return s.Store.CountChatQueuedMessages(ctx, id)
}

// Reads under the transition lock directly lengthen lock hold time, so
// validation must use the row the lock returned.
func TestChatMachine_Update_ValidatesFromLockedRow(t *testing.T) {
	t.Parallel()
	f := newTestFixture(t)
	ctx := testutil.Context(t, testutil.WaitShort)
	created := createTestChat(t, f)

	counts := &chatReadCounts{}
	m := chatstate.NewChatMachine(&chatReadCountingStore{Store: f.DB, counts: counts}, f.Pub, created.Chat.ID)
	require.NoError(t, m.Update(ctx, func(tx *chatstate.Tx, _ database.Store) error {
		if _, _, err := tx.Current(); err != nil {
			return err
		}
		_, err := tx.FinishTurn(chatstate.FinishTurnInput{})
		return err
	}))

	require.Equal(t, 1, counts.getChatByID, "only the publication read")
	require.Equal(t, 2, counts.countQueued, "the post-lock count and the publication read")
}

func TestChatMachine_Update_CurrentMatchesDatabase(t *testing.T) {
	t.Parallel()
	f := newTestFixture(t)
	ctx := testutil.Context(t, testutil.WaitShort)
	created := createTestChat(t, f)

	m := chatstate.NewChatMachine(f.DB, f.Pub, created.Chat.ID)
	require.NoError(t, m.Update(ctx, func(tx *chatstate.Tx, store database.Store) error {
		locked, state, err := tx.Current()
		require.NoError(t, err)
		read, err := store.GetChatByID(ctx, created.Chat.ID)
		require.NoError(t, err)
		require.Equal(t, read, locked, "the locked row must match an in-transaction read")
		require.Equal(t, created.Chat.SnapshotVersion+1, locked.SnapshotVersion)
		require.Equal(t, chatstate.StateR0, state)

		if _, err := tx.FinishTurn(chatstate.FinishTurnInput{}); err != nil {
			return err
		}
		after, state, err := tx.Current()
		require.NoError(t, err)
		require.Equal(t, database.ChatStatusWaiting, after.Status, "Current must observe the transition's write")
		require.Equal(t, chatstate.StateW, state)
		return nil
	}))
}

func TestChatMachine_UpdateReturning(t *testing.T) {
	t.Parallel()

	t.Run("ReturnsCommittedRow", func(t *testing.T) {
		t.Parallel()
		f := newTestFixture(t)
		ctx := testutil.Context(t, testutil.WaitShort)
		created := createTestChat(t, f)

		counts := &chatReadCounts{}
		m := chatstate.NewChatMachine(&chatReadCountingStore{Store: f.DB, counts: counts}, f.Pub, created.Chat.ID)
		updated, err := m.UpdateReturning(ctx, func(tx *chatstate.Tx, _ database.Store) error {
			_, err := tx.FinishTurn(chatstate.FinishTurnInput{})
			return err
		})
		require.NoError(t, err)
		require.Equal(t, 1, counts.getChatByID, "the returned row is the publication read")

		committed := f.readChat(ctx, t, created.Chat.ID)
		require.Equal(t, committed, updated)
		require.Equal(t, database.ChatStatusWaiting, updated.Status)
		require.Equal(t, created.Chat.SnapshotVersion+1, updated.SnapshotVersion)
	})

	t.Run("CallbackErrorReturnsNoRow", func(t *testing.T) {
		t.Parallel()
		f := newTestFixture(t)
		ctx := testutil.Context(t, testutil.WaitShort)
		created := createTestChat(t, f)

		m := chatstate.NewChatMachine(f.DB, f.Pub, created.Chat.ID)
		wantErr := xerrors.New("callback failed")
		updated, err := m.UpdateReturning(ctx, func(tx *chatstate.Tx, _ database.Store) error {
			if _, err := tx.FinishTurn(chatstate.FinishTurnInput{}); err != nil {
				return err
			}
			return wantErr
		})
		require.ErrorIs(t, err, wantErr)
		require.Zero(t, updated)
		require.Equal(t, created.Chat.SnapshotVersion, f.readChat(ctx, t, created.Chat.ID).SnapshotVersion)
	})
}

func TestChatMachine_Lock_DoesNotBumpSnapshot(t *testing.T) {
	t.Parallel()
	f := newTestFixture(t)
	ctx := testutil.Context(t, testutil.WaitShort)
	created := createTestChat(t, f)
	m := chatstate.NewChatMachine(f.DB, f.Pub, created.Chat.ID)

	before := f.readChat(ctx, t, created.Chat.ID)
	publishedBefore := len(f.Pub.channels)

	require.NoError(t, m.Lock(ctx, func(_ database.Store) error {
		return nil
	}))
	after := f.readChat(ctx, t, created.Chat.ID)
	require.Equal(t, before.SnapshotVersion, after.SnapshotVersion)
	require.Equal(t, publishedBefore, len(f.Pub.channels), "Lock must not publish")
}

func TestChatMachine_ReadSnapshot_DoesNotBumpSnapshot(t *testing.T) {
	t.Parallel()
	f := newTestFixture(t)
	ctx := testutil.Context(t, testutil.WaitShort)
	created := createTestChat(t, f)
	m := chatstate.NewChatMachine(f.DB, f.Pub, created.Chat.ID)

	before := f.readChat(ctx, t, created.Chat.ID)
	publishedBefore := len(f.Pub.channels)

	var called bool
	require.NoError(t, m.ReadSnapshot(func(_ database.Store) error {
		called = true
		return nil
	}))
	require.True(t, called, "ReadSnapshot must invoke the callback")
	after := f.readChat(ctx, t, created.Chat.ID)
	require.Equal(t, before.SnapshotVersion, after.SnapshotVersion)
	require.Equal(t, publishedBefore, len(f.Pub.channels), "ReadSnapshot must not publish")
}

func TestChatMachine_ReadSnapshot_PropagatesMissingChat(t *testing.T) {
	t.Parallel()
	f := newTestFixture(t)
	ctx := testutil.Context(t, testutil.WaitShort)
	chatID := uuid.New()
	m := chatstate.NewChatMachine(f.DB, f.Pub, chatID)
	err := m.ReadSnapshot(func(store database.Store) error {
		_, err := store.GetChatByID(ctx, chatID)
		return err
	})
	require.ErrorIs(t, err, sql.ErrNoRows)
	require.Empty(t, f.Pub.channels)
}

func TestMessageRevisionTrigger_AssignsRevisionFromSnapshot(t *testing.T) {
	t.Parallel()
	f := newTestFixture(t)
	ctx := testutil.Context(t, testutil.WaitShort)
	created := createTestChat(t, f) // snapshot 1, history_version 1 via trigger

	// CommitStep an assistant message; it should land with revision = chat.snapshot_version after the bump.
	m := chatstate.NewChatMachine(f.DB, f.Pub, created.Chat.ID)
	var step chatstate.CommitStepResult
	require.NoError(t, m.Update(ctx, func(tx *chatstate.Tx, store database.Store) error {
		assistant := userTextMessage("assistant", f.User.ID, f.Model.ID)
		assistant.Role = database.ChatMessageRoleAssistant
		var err error
		step, err = tx.CommitStep(chatstate.CommitStepInput{
			Messages: []chatstate.Message{assistant},
		})
		return err
	}))
	require.Len(t, step.InsertedMessages, 1)
	after := f.readChat(ctx, t, created.Chat.ID)
	// The Update call bumps snapshot_version once before the trigger
	// runs, so the new revision should equal the bumped snapshot.
	require.Equal(t, after.SnapshotVersion, step.InsertedMessages[0].Revision)
	require.Equal(t, after.SnapshotVersion, after.HistoryVersion)
	require.Equal(t, int64(0), after.GenerationAttempt, "trigger resets generation_attempt to 0")
}

func TestQueueVersionTrigger_AdvancesOnInsert(t *testing.T) {
	t.Parallel()
	f := newTestFixture(t)
	ctx := testutil.Context(t, testutil.WaitShort)
	created := createTestChat(t, f) // queue_version starts at 0

	m := chatstate.NewChatMachine(f.DB, f.Pub, created.Chat.ID)
	require.NoError(t, m.Update(ctx, func(tx *chatstate.Tx, store database.Store) error {
		_, err := tx.SendMessage(chatstate.SendMessageInput{
			Message:      userTextMessage("queue", f.User.ID, f.Model.ID),
			BusyBehavior: chatstate.BusyBehaviorQueue,
			MaxQueueSize: codersdk.DefaultChatMaxQueuedMessagesPerChat,
		})
		return err
	}))
	after := f.readChat(ctx, t, created.Chat.ID)
	require.Equal(t, after.SnapshotVersion, after.QueueVersion)
	require.Greater(t, after.QueueVersion, int64(0))
}

func TestQueueVersionTrigger_StableForNonQueueMutations(t *testing.T) {
	t.Parallel()
	f := newTestFixture(t)
	ctx := testutil.Context(t, testutil.WaitShort)
	created := createTestChat(t, f)
	m := chatstate.NewChatMachine(f.DB, f.Pub, created.Chat.ID)

	require.NoError(t, m.Update(ctx, func(tx *chatstate.Tx, store database.Store) error {
		assistant := userTextMessage("assistant", f.User.ID, f.Model.ID)
		assistant.Role = database.ChatMessageRoleAssistant
		_, err := tx.CommitStep(chatstate.CommitStepInput{
			Messages: []chatstate.Message{assistant},
		})
		return err
	}))
	// queue_version must remain unchanged from initial 0.
	require.Equal(t, int64(0), f.readChat(ctx, t, created.Chat.ID).QueueVersion)
}

// TestUpdateFlushesBufferedPublicationsAfterCommit verifies that
// ChatMachine.Update owns the PublishBuffer lifecycle: nothing
// reaches the inner publisher until after the transaction commits,
// and at commit the buffered chat:update is forwarded.
func TestUpdateFlushesBufferedPublicationsAfterCommit(t *testing.T) {
	t.Parallel()
	f := newTestFixture(t)
	ctx := testutil.Context(t, testutil.WaitShort)
	created := createTestChat(t, f)

	channel := coderdpubsub.ChatStateUpdateChannel(created.Chat.ID)
	baseline := countChannel(f.Pub.channels, channel)

	m := chatstate.NewChatMachine(f.DB, f.Pub, created.Chat.ID)

	// During the callback, no new chat:update for this chat may have
	// reached the inner publisher because the buffer holds it.
	require.NoError(t, m.Update(ctx, func(_ *chatstate.Tx, _ database.Store) error {
		require.Equal(t, baseline, countChannel(f.Pub.channels, channel),
			"inner publisher saw chat:update before transaction committed")
		return nil
	}))

	require.Equal(t, baseline+1, countChannel(f.Pub.channels, channel),
		"exactly one new chat:update reached the inner publisher after commit")
}

func countChannel(channels []string, channel string) int {
	c := 0
	for _, ch := range channels {
		if ch == channel {
			c++
		}
	}
	return c
}

// TestChatMachine_Update_AuthorizesFromCachedChatRBAC covers that dbauthz
// authorizes a transition's chat reads and writes from the RBAC object
// Update caches from the locked row, and the lock itself from a
// caller-cached object.
func TestChatMachine_Update_AuthorizesFromCachedChatRBAC(t *testing.T) {
	t.Parallel()

	finishTurn := func(tx *chatstate.Tx, _ *testFixture) error {
		_, err := tx.FinishTurn(chatstate.FinishTurnInput{})
		return err
	}
	for _, tc := range []struct {
		name        string
		callerCache bool
		// setup runs on the raw store before the measured Update.
		setup      func(t *testing.T, f *testFixture, m *chatstate.ChatMachine)
		transition func(tx *chatstate.Tx, f *testFixture) error
		wantStatus database.ChatStatus
		// Only Update's publication read always reads the chat; the
		// lock's authorization read depends on the caller's cache.
		wantReads int
	}{
		{name: "NoCallerCache", transition: finishTurn, wantStatus: database.ChatStatusWaiting, wantReads: 2},
		{name: "CallerCache", callerCache: true, transition: finishTurn, wantStatus: database.ChatStatusWaiting, wantReads: 1},
		{
			// Acquire upserts the heartbeat and the publication checks the
			// fresh lease.
			name:        "Acquire",
			callerCache: true,
			transition: func(tx *chatstate.Tx, _ *testFixture) error {
				_, err := tx.Acquire(chatstate.AcquireInput{WorkerID: uuid.New(), RunnerID: uuid.New()})
				return err
			},
			wantStatus: database.ChatStatusRunning,
			wantReads:  1,
		},
		{
			// Sending from waiting cancels the pending tool call, which
			// reads the chat's messages under the lock.
			name:        "SendMessageCancelsToolCall",
			callerCache: true,
			setup: func(t *testing.T, f *testFixture, m *chatstate.ChatMachine) {
				commitAssistantToolCall(t, f, m, nonDynamicAssistantToolCallMessage(t, f.Model.ID, "call_"+uuid.NewString()))
				landInW(t, f, m)
			},
			transition: func(tx *chatstate.Tx, f *testFixture) error {
				res, err := tx.SendMessage(chatstate.SendMessageInput{
					Message:      userTextMessage("after-cancel", f.User.ID, f.Model.ID),
					BusyBehavior: chatstate.BusyBehaviorQueue,
					MaxQueueSize: codersdk.DefaultChatMaxQueuedMessagesPerChat,
				})
				if err == nil && len(res.InsertedMessages) != 2 {
					return xerrors.Errorf("want synthetic cancel and user message, got %d", len(res.InsertedMessages))
				}
				return err
			},
			wantStatus: database.ChatStatusRunning,
			wantReads:  1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newTestFixture(t)
			created := createTestChat(t, f)
			if tc.setup != nil {
				tc.setup(t, f, chatstate.NewChatMachine(f.DB, f.Pub, created.Chat.ID))
			}
			counts := &chatReadCounts{}
			authzDB := dbauthz.New(
				&chatReadCountingStore{Store: f.DB, counts: counts},
				rbac.NewStrictCachingAuthorizer(prometheus.NewRegistry()),
				slogtest.Make(t, nil),
				nil,
			)
			//nolint:gocritic // Transitions run as chatd.
			ctx := dbauthz.AsChatd(testutil.Context(t, testutil.WaitShort))
			if tc.callerCache {
				var err error
				ctx, err = dbauthz.WithChatRBAC(ctx, dbauthz.CacheableChatRBAC(created.Chat))
				require.NoError(t, err)
			}

			m := chatstate.NewChatMachine(authzDB, f.Pub, created.Chat.ID)
			require.NoError(t, m.Update(ctx, func(tx *chatstate.Tx, _ database.Store) error {
				cached, ok := dbauthz.ChatRBACFromContext(tx.Ctx())
				require.True(t, ok)
				require.Equal(t, created.Chat.ID.String(), cached.ID)
				return tc.transition(tx, f)
			}))
			require.Equal(t, tc.wantReads, counts.getChatByID)
			require.Equal(t, tc.wantStatus, f.readChat(ctx, t, created.Chat.ID).Status)
		})
	}
}

// CreateChat authorizes its chat-scoped writes from the inserted row, so
// only the post-insert reload reads the chat.
func TestCreateChat_AuthorizesFromInsertedChat(t *testing.T) {
	t.Parallel()
	f := newTestFixture(t)
	counts := &chatReadCounts{}
	authzDB := dbauthz.New(
		&chatReadCountingStore{Store: f.DB, counts: counts},
		rbac.NewStrictCachingAuthorizer(prometheus.NewRegistry()),
		slogtest.Make(t, nil),
		nil,
	)
	//nolint:gocritic // Chat creation runs as chatd here.
	ctx := dbauthz.AsChatd(testutil.Context(t, testutil.WaitShort))
	res, err := chatstate.CreateChat(ctx, authzDB, f.Pub, chatstate.CreateChatInput{
		OrganizationID:    f.Org.ID,
		OwnerID:           f.User.ID,
		LastModelConfigID: f.Model.ID,
		Title:             "test",
		ClientType:        database.ChatClientTypeApi,
		InitialStatus:     database.ChatStatusRunning,
		InitialMessages: []chatstate.Message{
			userTextMessage("hello", f.User.ID, f.Model.ID),
		},
	})
	require.NoError(t, err)
	require.Len(t, res.InitialMessages, 1)
	require.Equal(t, 1, counts.getChatByID)
}

// fakeLocalAcquirer records LocalAcquirer calls. StartAcquired captures
// the committed row and the publications made so far, so tests can check
// it runs after commit and publication.
type fakeLocalAcquirer struct {
	db      database.Store
	pub     *recordingPubsub
	decline bool
	// prepare, when set, runs at the start of PrepareAcquire.
	prepare func(ctx context.Context, store database.Store) error

	mu       sync.Mutex
	prepares int
	inputs   []chatstate.AcquireInput
	starts   []fakeLocalStart
}

type fakeLocalStart struct {
	chatID    uuid.UUID
	input     chatstate.AcquireInput
	committed database.Chat
	published []string
}

func (a *fakeLocalAcquirer) PrepareAcquire(ctx context.Context, store database.Store, _ database.Chat) (chatstate.AcquireInput, bool, error) {
	a.mu.Lock()
	a.prepares++
	a.mu.Unlock()
	if a.prepare != nil {
		if err := a.prepare(ctx, store); err != nil {
			return chatstate.AcquireInput{}, false, err
		}
	}
	if a.decline {
		return chatstate.AcquireInput{}, false, nil
	}
	input := chatstate.AcquireInput{WorkerID: uuid.New(), RunnerID: uuid.New()}
	a.mu.Lock()
	a.inputs = append(a.inputs, input)
	a.mu.Unlock()
	return input, true, nil
}

func (a *fakeLocalAcquirer) StartAcquired(chatID uuid.UUID, input chatstate.AcquireInput) {
	committed, _ := a.db.GetChatByID(context.Background(), chatID)
	a.pub.mu.Lock()
	published := slices.Clone(a.pub.channels)
	a.pub.mu.Unlock()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.starts = append(a.starts, fakeLocalStart{chatID: chatID, input: input, committed: committed, published: published})
}

func (a *fakeLocalAcquirer) counts() (prepares, starts int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.prepares, len(a.starts)
}

// requireLocallyAcquired checks that a started the chat once, after the
// commit that made input its owner and after the owned chat:update.
func requireLocallyAcquired(t *testing.T, f *testFixture, a *fakeLocalAcquirer, chatID uuid.UUID, returned database.Chat) {
	t.Helper()
	ctx := testutil.Context(t, testutil.WaitShort)
	a.mu.Lock()
	defer a.mu.Unlock()
	require.Len(t, a.inputs, 1)
	require.Len(t, a.starts, 1)
	start := a.starts[0]
	input := a.inputs[0]
	require.Equal(t, chatID, start.chatID)
	require.Equal(t, input, start.input)
	require.Equal(t, uuid.NullUUID{UUID: input.WorkerID, Valid: true}, start.committed.WorkerID)
	require.Equal(t, uuid.NullUUID{UUID: input.RunnerID, Valid: true}, start.committed.RunnerID)
	require.Contains(t, start.published, coderdpubsub.ChatStateUpdateChannel(chatID))
	require.Equal(t, f.readChat(ctx, t, chatID), returned, "the returned row is the owned row")

	_, err := f.DB.GetChatHeartbeat(ctx, database.GetChatHeartbeatParams{ChatID: chatID, RunnerID: input.RunnerID})
	require.NoError(t, err)

	f.Pub.mu.Lock()
	defer f.Pub.mu.Unlock()
	channel := coderdpubsub.ChatStateUpdateChannel(chatID)
	for i := len(f.Pub.channels) - 1; i >= 0; i-- {
		if f.Pub.channels[i] != channel {
			continue
		}
		var msg coderdpubsub.ChatStateUpdateMessage
		require.NoError(t, json.Unmarshal(f.Pub.payloads[i], &msg))
		require.NotNil(t, msg.WorkerID)
		require.NotNil(t, msg.RunnerID)
		require.Equal(t, input.WorkerID, *msg.WorkerID)
		require.Equal(t, input.RunnerID, *msg.RunnerID)
		return
	}
	t.Fatalf("no chat:update on %s", channel)
}

func TestChatMachine_Update_LocalAcquirer(t *testing.T) {
	t.Parallel()

	sendMessage := func(f *testFixture) func(*chatstate.Tx, database.Store) error {
		return func(tx *chatstate.Tx, _ database.Store) error {
			_, err := tx.SendMessage(chatstate.SendMessageInput{
				Message:      userTextMessage("next", f.User.ID, f.Model.ID),
				BusyBehavior: chatstate.BusyBehaviorQueue,
				MaxQueueSize: codersdk.DefaultChatMaxQueuedMessagesPerChat,
			})
			return err
		}
	}
	// waitingChat returns an unowned chat in W, so SendMessage makes it
	// runnable and unowned.
	waitingChat := func(t *testing.T, f *testFixture) uuid.UUID {
		created := createTestChat(t, f)
		landInW(t, f, chatstate.NewChatMachine(f.DB, f.Pub, created.Chat.ID))
		return created.Chat.ID
	}

	t.Run("AcquiresRunnableUnownedChat", func(t *testing.T) {
		t.Parallel()
		f := newTestFixture(t)
		ctx := testutil.Context(t, testutil.WaitShort)
		chatID := waitingChat(t, f)
		hints := f.Pub.ownershipPublishCount()

		a := &fakeLocalAcquirer{db: f.DB, pub: f.Pub}
		counts := &chatReadCounts{}
		m := chatstate.NewChatMachine(&chatReadCountingStore{Store: f.DB, counts: counts}, f.Pub, chatID).WithLocalAcquirer(a)
		updated, err := m.UpdateReturning(ctx, sendMessage(f))
		require.NoError(t, err)
		requireLocallyAcquired(t, f, a, chatID, updated)
		require.Equal(t, database.ChatStatusRunning, updated.Status)
		require.Equal(t, hints, f.Pub.ownershipPublishCount(), "a local acquire publishes no ownership hint")
		require.Equal(t, 1, counts.getChatByID, "the acquire reuses its own write's row")
	})

	t.Run("DeclinedPublishesHint", func(t *testing.T) {
		t.Parallel()
		f := newTestFixture(t)
		ctx := testutil.Context(t, testutil.WaitShort)
		chatID := waitingChat(t, f)
		hints := f.Pub.ownershipPublishCount()

		a := &fakeLocalAcquirer{db: f.DB, pub: f.Pub, decline: true}
		m := chatstate.NewChatMachine(f.DB, f.Pub, chatID).WithLocalAcquirer(a)
		updated, err := m.UpdateReturning(ctx, sendMessage(f))
		require.NoError(t, err)
		prepares, starts := a.counts()
		require.Equal(t, 1, prepares)
		require.Zero(t, starts)
		require.False(t, updated.WorkerID.Valid)
		require.Equal(t, hints+1, f.Pub.ownershipPublishCount())
	})

	t.Run("OwnedChatSkipsPrepare", func(t *testing.T) {
		t.Parallel()
		f := newTestFixture(t)
		ctx := testutil.Context(t, testutil.WaitShort)
		created := createTestChat(t, f)
		owner := chatstate.AcquireInput{WorkerID: uuid.New(), RunnerID: uuid.New()}
		require.NoError(t, chatstate.NewChatMachine(f.DB, f.Pub, created.Chat.ID).Update(ctx, func(tx *chatstate.Tx, _ database.Store) error {
			_, err := tx.Acquire(owner)
			return err
		}))

		a := &fakeLocalAcquirer{db: f.DB, pub: f.Pub}
		m := chatstate.NewChatMachine(f.DB, f.Pub, created.Chat.ID).WithLocalAcquirer(a)
		updated, err := m.UpdateReturning(ctx, sendMessage(f))
		require.NoError(t, err)
		prepares, starts := a.counts()
		require.Zero(t, prepares)
		require.Zero(t, starts)
		require.Equal(t, owner.RunnerID, updated.RunnerID.UUID)
	})

	t.Run("NestedUpdateSkipsPrepare", func(t *testing.T) {
		t.Parallel()
		f := newTestFixture(t)
		ctx := testutil.Context(t, testutil.WaitShort)
		chatID := waitingChat(t, f)

		a := &fakeLocalAcquirer{db: f.DB, pub: f.Pub}
		outer := chatstate.NewChatMachine(f.DB, f.Pub, chatID)
		require.NoError(t, outer.Update(ctx, func(tx *chatstate.Tx, store database.Store) error {
			inner := chatstate.NewChatMachine(store, f.Pub, chatID).WithLocalAcquirer(a)
			return inner.Update(tx.Ctx(), sendMessage(f))
		}))
		prepares, starts := a.counts()
		require.Zero(t, prepares)
		require.Zero(t, starts)
		require.False(t, f.readChat(ctx, t, chatID).WorkerID.Valid)
	})

	t.Run("CallbackErrorStartsNothing", func(t *testing.T) {
		t.Parallel()
		f := newTestFixture(t)
		ctx := testutil.Context(t, testutil.WaitShort)
		chatID := waitingChat(t, f)

		a := &fakeLocalAcquirer{db: f.DB, pub: f.Pub}
		m := chatstate.NewChatMachine(f.DB, f.Pub, chatID).WithLocalAcquirer(a)
		wantErr := xerrors.New("callback failed")
		err := m.Update(ctx, func(tx *chatstate.Tx, store database.Store) error {
			if err := sendMessage(f)(tx, store); err != nil {
				return err
			}
			return wantErr
		})
		require.ErrorIs(t, err, wantErr)
		prepares, starts := a.counts()
		require.Zero(t, prepares)
		require.Zero(t, starts)
	})

	t.Run("PrepareErrorRollsBack", func(t *testing.T) {
		t.Parallel()
		f := newTestFixture(t)
		ctx := testutil.Context(t, testutil.WaitShort)
		chatID := waitingChat(t, f)
		before := f.readChat(ctx, t, chatID)

		wantErr := xerrors.New("prepare failed")
		a := &fakeLocalAcquirer{db: f.DB, pub: f.Pub, prepare: func(context.Context, database.Store) error {
			return wantErr
		}}
		m := chatstate.NewChatMachine(f.DB, f.Pub, chatID).WithLocalAcquirer(a)
		err := m.Update(ctx, sendMessage(f))
		require.ErrorIs(t, err, wantErr)
		_, starts := a.counts()
		require.Zero(t, starts)
		require.Equal(t, before.SnapshotVersion, f.readChat(ctx, t, chatID).SnapshotVersion)
	})

	t.Run("DeadlockRetryStartsOnce", func(t *testing.T) {
		t.Parallel()
		f := newTestFixture(t)
		ctx := testutil.Context(t, testutil.WaitShort)
		chatID := waitingChat(t, f)

		// The first attempt prepares, then its acquire hits a deadlock
		// abort, so Update reruns the whole attempt with a new input.
		a := &fakeLocalAcquirer{db: f.DB, pub: f.Pub, prepare: func(ctx context.Context, store database.Store) error {
			_, err := chatstate.LockAutomations(ctx, store, []uuid.UUID{uuid.New()})
			return err
		}}
		failures := &atomic.Int32{}
		failures.Store(1)
		store := &heartbeatDeadlockStore{Store: f.DB, failures: failures}
		m := chatstate.NewChatMachine(store, f.Pub, chatID).WithLocalAcquirer(a)
		updated, err := m.UpdateReturning(ctx, sendMessage(f))
		require.NoError(t, err)
		prepares, _ := a.counts()
		require.Equal(t, 2, prepares)
		a.mu.Lock()
		require.Len(t, a.inputs, 2)
		a.inputs = a.inputs[1:]
		a.mu.Unlock()
		requireLocallyAcquired(t, f, a, chatID, updated)
	})
}

// heartbeatDeadlockStore injects a PostgreSQL deadlock abort into the
// first failures heartbeat upserts.
type heartbeatDeadlockStore struct {
	database.Store
	failures *atomic.Int32
}

func (s *heartbeatDeadlockStore) InTx(fn func(database.Store) error, opts *database.TxOptions) error {
	return s.Store.InTx(func(tx database.Store) error {
		return fn(&heartbeatDeadlockStore{Store: tx, failures: s.failures})
	}, opts)
}

func (s *heartbeatDeadlockStore) UpsertChatHeartbeat(ctx context.Context, arg database.UpsertChatHeartbeatParams) error {
	if s.failures.Add(-1) >= 0 {
		return &pq.Error{Code: "40P01", Message: "deadlock detected"}
	}
	return s.Store.UpsertChatHeartbeat(ctx, arg)
}

func TestCreateChatWithID_LocalAcquirer(t *testing.T) {
	t.Parallel()

	create := func(t *testing.T, f *testFixture, a chatstate.LocalAcquirer, status database.ChatStatus) (uuid.UUID, chatstate.CreateChatResult, error) {
		t.Helper()
		chatID := uuid.New()
		res, err := chatstate.CreateChatWithID(testutil.Context(t, testutil.WaitShort), f.DB, f.Pub, chatID, chatstate.CreateChatInput{
			OrganizationID:    f.Org.ID,
			OwnerID:           f.User.ID,
			LastModelConfigID: f.Model.ID,
			Title:             "test",
			ClientType:        database.ChatClientTypeApi,
			InitialStatus:     status,
			InitialMessages: []chatstate.Message{
				userTextMessage("hello", f.User.ID, f.Model.ID),
			},
			LocalAcquirer: a,
		})
		return chatID, res, err
	}

	t.Run("AcquiresRunnableChat", func(t *testing.T) {
		t.Parallel()
		f := newTestFixture(t)
		a := &fakeLocalAcquirer{db: f.DB, pub: f.Pub}
		chatID, res, err := create(t, f, a, database.ChatStatusRunning)
		require.NoError(t, err)
		requireLocallyAcquired(t, f, a, chatID, res.Chat)
		require.Equal(t, int64(1), res.Chat.SnapshotVersion)
		require.False(t, f.Pub.hasOwnership())
	})

	t.Run("DeclinedPublishesHint", func(t *testing.T) {
		t.Parallel()
		f := newTestFixture(t)
		a := &fakeLocalAcquirer{db: f.DB, pub: f.Pub, decline: true}
		_, res, err := create(t, f, a, database.ChatStatusRunning)
		require.NoError(t, err)
		prepares, starts := a.counts()
		require.Equal(t, 1, prepares)
		require.Zero(t, starts)
		require.False(t, res.Chat.WorkerID.Valid)
		require.True(t, f.Pub.hasOwnership())
	})

	t.Run("WaitingChatSkipsPrepare", func(t *testing.T) {
		t.Parallel()
		f := newTestFixture(t)
		a := &fakeLocalAcquirer{db: f.DB, pub: f.Pub}
		_, res, err := create(t, f, a, database.ChatStatusWaiting)
		require.NoError(t, err)
		prepares, starts := a.counts()
		require.Zero(t, prepares)
		require.Zero(t, starts)
		require.False(t, res.Chat.WorkerID.Valid)
	})

	t.Run("PrepareErrorCreatesNothing", func(t *testing.T) {
		t.Parallel()
		f := newTestFixture(t)
		wantErr := xerrors.New("prepare failed")
		a := &fakeLocalAcquirer{db: f.DB, pub: f.Pub, prepare: func(context.Context, database.Store) error {
			return wantErr
		}}
		chatID, _, err := create(t, f, a, database.ChatStatusRunning)
		require.ErrorIs(t, err, wantErr)
		_, starts := a.counts()
		require.Zero(t, starts)
		_, err = f.DB.GetChatByID(testutil.Context(t, testutil.WaitShort), chatID)
		require.ErrorIs(t, err, sql.ErrNoRows)
		require.Empty(t, f.Pub.channels)
	})
}
