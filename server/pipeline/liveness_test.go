// Copyright 2026 Woodpecker Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build test

package pipeline

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	forge_mocks "go.woodpecker-ci.org/woodpecker/v3/server/forge/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	manager_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/datastore"
)

// livenessFixture is a real store holding one repo, its owner, and one
// created pipeline. The test clock starts at wall-clock now, because a post's
// context deadline is real time. Unless young, the pipeline's last write is
// backdated an hour, so it is a stale orphan at the test clock.
type livenessFixture struct {
	store    store.Store
	user     *model.User
	repo     *model.Repo
	pipeline *model.Pipeline
	now      time.Time
}

func newLivenessFixture(t *testing.T) *livenessFixture {
	t.Helper()
	return newLivenessFixtureAged(t, true)
}

func newLivenessFixtureAged(t *testing.T, stale bool) *livenessFixture {
	t.Helper()
	s := datastore.NewTestStore(t)

	user := &model.User{Login: "octocat", ForgeRemoteID: "1", Email: "octocat@example.com"}
	require.NoError(t, s.CreateUser(user))
	repo := &model.Repo{
		UserID: user.ID, Owner: "octocat", Name: "hello", FullName: "octocat/hello",
		ForgeRemoteID: "1", ForgeID: 1,
	}
	require.NoError(t, s.CreateRepo(repo))

	p := &model.Pipeline{
		RepoID: repo.ID, Status: model.StatusCreated, Event: model.EventPull,
		Commit: "deadbeef", Branch: "main", Ref: "refs/pull/1/head",
	}
	require.NoError(t, s.CreatePipeline(p))
	p, err := s.GetPipeline(p.ID)
	require.NoError(t, err)

	now := time.Now()
	if stale {
		// The claim is the store's only exact write of updated; use it to
		// backdate the row's last write. The row stays created.
		ok, err := s.PipelineDispatchClaim(p, now.Add(-time.Hour).Unix())
		require.NoError(t, err)
		require.True(t, ok)
	}

	return &livenessFixture{
		store:    s,
		user:     user,
		repo:     repo,
		pipeline: p,
		now:      now,
	}
}

func (f *livenessFixture) reload(t *testing.T) *model.Pipeline {
	t.Helper()
	p, err := f.store.GetPipeline(f.pipeline.ID)
	require.NoError(t, err)
	return p
}

// dueReports returns the report rows due at the given time.
func (f *livenessFixture) dueReports(t *testing.T, at time.Time) []*model.PipelineDispatchReport {
	t.Helper()
	reports, err := f.store.DispatchReportsDue(at.Unix(), 100)
	require.NoError(t, err)
	return reports
}

// creatorBetweenClaimAndFlag makes a creator write land after the sweeper
// won the claim and before it flags.
type creatorBetweenClaimAndFlag struct {
	store.Store
	t *testing.T
}

func (s creatorBetweenClaimAndFlag) PipelineDispatchClaim(p *model.Pipeline, now int64) (bool, error) {
	ok, err := s.Store.PipelineDispatchClaim(p, now)
	stored, getErr := s.GetPipeline(p.ID)
	require.NoError(s.t, getErr)
	_, updErr := UpdateToStatusPending(s.Store, *stored, "")
	require.NoError(s.t, updErr)
	return ok, err
}

func TestFlagOrphanedPipeline(t *testing.T) {
	t.Run("stale orphan flags once and gets a report", func(t *testing.T) {
		f := newLivenessFixture(t)
		now := f.now.Unix()

		require.NoError(t, flagOrphanedPipeline(f.store, f.pipeline, now))
		stored := f.reload(t)
		assert.Equal(t, model.StatusError, stored.Status)
		assert.EqualValues(t, now, stored.Updated)
		require.Len(t, stored.Errors, 1)
		assert.Equal(t, orphanMessage, stored.Errors[0].Message)

		reports := f.dueReports(t, f.now)
		require.Len(t, reports, 1)
		assert.EqualValues(t, f.pipeline.ID, reports[0].PipelineID)
		assert.EqualValues(t, now, reports[0].FlaggedAt)

		// The same stale view flags nothing more.
		require.NoError(t, flagOrphanedPipeline(f.store, f.pipeline, now+1))
		assert.Len(t, f.dueReports(t, f.now.Add(time.Second)), 1)
	})

	t.Run("claim lost to another claimer leaves it created", func(t *testing.T) {
		f := newLivenessFixture(t)
		other := *f.pipeline
		ok, err := f.store.PipelineDispatchClaim(&other, f.now.Unix())
		require.NoError(t, err)
		require.True(t, ok)

		require.NoError(t, flagOrphanedPipeline(f.store, f.pipeline, f.now.Unix()+1))
		assert.Equal(t, model.StatusCreated, f.reload(t).Status)
		assert.Empty(t, f.dueReports(t, f.now.Add(time.Hour)))
	})

	t.Run("creator write between claim and flag wins", func(t *testing.T) {
		f := newLivenessFixture(t)
		racing := creatorBetweenClaimAndFlag{Store: f.store, t: t}

		require.NoError(t, flagOrphanedPipeline(racing, f.pipeline, f.now.Unix()))
		assert.Equal(t, model.StatusPending, f.reload(t).Status)
		assert.Empty(t, f.dueReports(t, f.now.Add(time.Hour)))
	})
}

// postedStatus is one Forge.Status call seen by the liveness harness.
type postedStatus struct {
	ctx      context.Context
	pipeline model.Pipeline
	workflow model.Workflow
}

// livenessHarness drives sweep and delivery ticks against a real store, a
// forge mock and a manager mock, at a test clock.
type livenessHarness struct {
	*livenessFixture
	forge    *forge_mocks.MockForge
	manager  *manager_mocks.MockManager
	delivery *dispatchDelivery
	clock    time.Time

	mu       sync.Mutex
	posts    []postedStatus
	onStatus func(ctx context.Context, p *model.Pipeline, w *model.Workflow) error
}

func newLivenessHarness(t *testing.T) *livenessHarness {
	t.Helper()
	return newLivenessHarnessFor(t, newLivenessFixture(t))
}

func newLivenessHarnessFor(t *testing.T, f *livenessFixture) *livenessHarness {
	t.Helper()
	h := &livenessHarness{livenessFixture: f, clock: f.now}
	h.forge = forge_mocks.NewMockForge(t)
	h.forge.EXPECT().
		Status(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, _ *model.User, _ *model.Repo, p *model.Pipeline, w *model.Workflow) error {
			h.mu.Lock()
			h.posts = append(h.posts, postedStatus{ctx: ctx, pipeline: *p, workflow: *w})
			onStatus := h.onStatus
			h.mu.Unlock()
			if onStatus != nil {
				return onStatus(ctx, p, w)
			}
			return nil
		}).Maybe()
	h.manager = manager_mocks.NewMockManager(t)
	h.manager.EXPECT().ForgeFromRepo(mock.Anything).Return(h.forge, nil).Maybe()
	server.Config.Services.Manager = h.manager

	h.delivery = h.newDelivery(f.store)
	return h
}

// newDelivery returns a delivery on the given store that reads the harness
// clock and uses a short post timeout.
func (h *livenessHarness) newDelivery(s store.Store) *dispatchDelivery {
	d := newDispatchDelivery(s)
	d.clock = func() time.Time { return h.clock }
	d.postTimeout = 5 * time.Second
	return d
}

func (h *livenessHarness) setOnStatus(fn func(ctx context.Context, p *model.Pipeline, w *model.Workflow) error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.onStatus = fn
}

// tick runs one detection tick and one delivery tick at the harness clock.
func (h *livenessHarness) tick(t *testing.T) {
	t.Helper()
	runDispatchLivenessTick(t.Context(), h.store, h.clock)
	h.delivery.runDispatchDeliveryTick(t.Context())
}

// takePosts returns the posted states since the last call and clears them.
func (h *livenessHarness) takePosts() []postedStatus {
	h.mu.Lock()
	defer h.mu.Unlock()
	posts := h.posts
	h.posts = nil
	return posts
}

func (h *livenessHarness) takeStates() []model.StatusValue {
	posts := h.takePosts()
	states := make([]model.StatusValue, 0, len(posts))
	for _, p := range posts {
		states = append(states, p.workflow.State)
	}
	return states
}

// report returns the single report row, or nil when there is none.
func (h *livenessHarness) report(t *testing.T) *model.PipelineDispatchReport {
	t.Helper()
	reports := datastore.DispatchReportsForTest(t, h.store)
	require.LessOrEqual(t, len(reports), 1)
	if len(reports) == 0 {
		return nil
	}
	return reports[0]
}

// deliverRed flags the orphan and delivers its red.
func (h *livenessHarness) deliverRed(t *testing.T) {
	t.Helper()
	h.tick(t)
	require.Equal(t, []model.StatusValue{model.StatusError}, h.takeStates())
	r := h.report(t)
	require.NotNil(t, r)
	require.Equal(t, model.StatusError, r.Delivered)
}

// creator returns the creator's stale in-memory copy of the pipeline.
func (h *livenessHarness) creator() model.Pipeline {
	return *h.pipeline
}

func TestDispatchDeliveryZeroWorkflows(t *testing.T) {
	h := newLivenessHarness(t)
	h.tick(t)

	posts := h.takePosts()
	require.Len(t, posts, 1)
	assert.Equal(t, *dispatchStatusWorkflow(model.StatusError), posts[0].workflow)
	assert.Equal(t, model.StatusError, h.report(t).Delivered)
}

func TestDispatchDeliveryPendingWorkflows(t *testing.T) {
	h := newLivenessHarness(t)
	workflows := []*model.Workflow{
		{PipelineID: h.pipeline.ID, PID: 1, Name: "build", State: model.StatusPending},
		{PipelineID: h.pipeline.ID, PID: 2, Name: "test", State: model.StatusPending},
		{PipelineID: h.pipeline.ID, PID: 3, Name: "lint", State: model.StatusPending},
	}
	require.NoError(t, h.store.WorkflowsCreate(workflows))

	h.tick(t)

	posts := h.takePosts()
	require.Len(t, posts, 1)
	assert.Equal(t, *dispatchStatusWorkflow(model.StatusError), posts[0].workflow)
	stored, err := h.store.WorkflowGetTree(h.pipeline)
	require.NoError(t, err)
	require.Len(t, stored, 3)
	for _, w := range stored {
		assert.Equal(t, model.StatusPending, w.State)
	}
}

// faultyStore fails one named store method with a non-not-found error.
type faultyStore struct {
	store.Store
	fail string
	once bool
}

var errInjected = errors.New("injected store failure")

func (s *faultyStore) trip(name string) bool {
	if s.fail != name {
		return false
	}
	if s.once {
		s.fail = ""
	}
	return true
}

func (s *faultyStore) GetPipeline(id int64) (*model.Pipeline, error) {
	if s.trip("GetPipeline") {
		return nil, errInjected
	}
	return s.Store.GetPipeline(id)
}

func (s *faultyStore) PipelineDispatchSuccessor(p *model.Pipeline) (*model.Pipeline, error) {
	if s.trip("PipelineDispatchSuccessor") {
		return nil, errInjected
	}
	return s.Store.PipelineDispatchSuccessor(p)
}

func (s *faultyStore) GetRepo(id int64) (*model.Repo, error) {
	if s.trip("GetRepo") {
		return nil, errInjected
	}
	return s.Store.GetRepo(id)
}

func (s *faultyStore) GetUser(id int64) (*model.User, error) {
	if s.trip("GetUser") {
		return nil, errInjected
	}
	return s.Store.GetUser(id)
}

func (s *faultyStore) DispatchReportsDue(now int64, limit int) ([]*model.PipelineDispatchReport, error) {
	if s.trip("DispatchReportsDue") {
		return nil, errInjected
	}
	return s.Store.DispatchReportsDue(now, limit)
}

func TestDispatchDeliveryFailedPost(t *testing.T) {
	t.Run("forge error retries with backoff", func(t *testing.T) {
		h := newLivenessHarness(t)
		h.setOnStatus(func(context.Context, *model.Pipeline, *model.Workflow) error {
			return errors.New("forge unavailable")
		})
		h.tick(t)
		require.Equal(t, []model.StatusValue{model.StatusError}, h.takeStates())

		r := h.report(t)
		assert.Empty(t, r.Delivered)
		assert.Equal(t, 1, r.Attempts)
		assert.EqualValues(t, h.clock.Add(reportRetryBase).Unix(), r.NextAttempt)
		assert.Contains(t, r.LastError, "forge unavailable")

		h.setOnStatus(nil)
		due := h.clock.Add(reportRetryBase)
		h.clock = due.Add(-time.Second)
		h.tick(t)
		assert.Empty(t, h.takeStates())

		h.clock = due
		h.tick(t)
		assert.Equal(t, []model.StatusValue{model.StatusError}, h.takeStates())
		r = h.report(t)
		assert.Equal(t, model.StatusError, r.Delivered)
		assert.Zero(t, r.Attempts)
		assert.Empty(t, r.LastError)
	})

	for _, method := range []string{"GetPipeline", "PipelineDispatchSuccessor", "GetRepo", "GetUser", "ForgeFromRepo"} {
		t.Run(method+" error retries without a post", func(t *testing.T) {
			h := newLivenessHarness(t)
			require.NoError(t, flagOrphanedPipeline(h.store, h.pipeline, h.clock.Unix()))

			if method == "ForgeFromRepo" {
				failing := manager_mocks.NewMockManager(t)
				failing.EXPECT().ForgeFromRepo(mock.Anything).Return(nil, errInjected).Once()
				server.Config.Services.Manager = failing
			} else {
				h.delivery = h.newDelivery(&faultyStore{Store: h.store, fail: method})
			}
			h.tick(t)

			assert.Empty(t, h.takeStates())
			r := h.report(t)
			require.NotNil(t, r)
			assert.Empty(t, r.Delivered)
			assert.Equal(t, 1, r.Attempts)
			assert.EqualValues(t, h.clock.Add(reportRetryBase).Unix(), r.NextAttempt)
			assert.Contains(t, r.LastError, errInjected.Error())
		})
	}

	t.Run("DispatchReportsDue error skips the tick", func(t *testing.T) {
		h := newLivenessHarness(t)
		require.NoError(t, flagOrphanedPipeline(h.store, h.pipeline, h.clock.Unix()))
		before := h.report(t)

		h.delivery = h.newDelivery(&faultyStore{Store: h.store, fail: "DispatchReportsDue", once: true})
		h.tick(t)
		assert.Empty(t, h.takeStates())
		assert.Equal(t, before, h.report(t))

		h.tick(t)
		assert.Equal(t, []model.StatusValue{model.StatusError}, h.takeStates())
		assert.Equal(t, model.StatusError, h.report(t).Delivered)
	})
}

func TestDispatchDeliveryRestart(t *testing.T) {
	f := newLivenessFixture(t)
	// The process dies right after the flag commits.
	require.NoError(t, flagOrphanedPipeline(f.store, f.pipeline, f.now.Unix()))

	h := newLivenessHarnessFor(t, f)
	h.tick(t)
	assert.Equal(t, []model.StatusValue{model.StatusError}, h.takeStates())
	assert.Equal(t, model.StatusError, h.report(t).Delivered)
}

func TestDispatchDeliveryLateCreatorAfterRed(t *testing.T) {
	h := newLivenessHarness(t)
	h.deliverRed(t)

	pending, err := UpdateToStatusPending(h.store, h.creator(), "")
	require.NoError(t, err)
	h.tick(t)
	assert.Equal(t, []model.StatusValue{model.StatusPending}, h.takeStates())

	running, err := UpdateToStatusRunning(h.store, *pending, h.clock.Unix())
	require.NoError(t, err)
	h.tick(t)
	assert.Equal(t, []model.StatusValue{model.StatusRunning}, h.takeStates())

	_, err = UpdateStatusToDone(h.store, *running, model.StatusSuccess, h.clock.Unix())
	require.NoError(t, err)
	h.tick(t)
	assert.Equal(t, []model.StatusValue{model.StatusSuccess}, h.takeStates())
	r := h.report(t)
	retireAfter := h.clock.Add(reportSettle)
	assert.EqualValues(t, retireAfter.Unix(), r.RetireAfter)

	h.clock = retireAfter.Add(-time.Second)
	h.tick(t)
	assert.Empty(t, h.takeStates())

	h.clock = retireAfter
	h.tick(t)
	assert.Equal(t, []model.StatusValue{model.StatusSuccess}, h.takeStates())
	assert.Nil(t, h.report(t))
}

func TestDispatchDeliveryLateCreatorBeforeDelivery(t *testing.T) {
	h := newLivenessHarness(t)
	require.NoError(t, flagOrphanedPipeline(h.store, h.pipeline, h.clock.Unix()))
	_, err := UpdateToStatusPending(h.store, h.creator(), "")
	require.NoError(t, err)

	h.tick(t)
	assert.Equal(t, []model.StatusValue{model.StatusPending}, h.takeStates())
	assert.Equal(t, model.StatusPending, h.report(t).Delivered)
}

func TestDispatchDeliveryCreatorBetweenReadAndPost(t *testing.T) {
	h := newLivenessHarness(t)
	h.setOnStatus(func(_ context.Context, _ *model.Pipeline, w *model.Workflow) error {
		if w.State == model.StatusError {
			_, err := UpdateToStatusPending(h.store, h.creator(), "")
			return err
		}
		return nil
	})

	h.tick(t)
	assert.Equal(t, []model.StatusValue{model.StatusError}, h.takeStates())
	assert.Equal(t, model.StatusError, h.report(t).Delivered)

	h.tick(t)
	assert.Equal(t, []model.StatusValue{model.StatusPending}, h.takeStates())
}

func TestDispatchDeliveryDeleteBeforeDelivery(t *testing.T) {
	h := newLivenessHarness(t)
	require.NoError(t, flagOrphanedPipeline(h.store, h.pipeline, h.clock.Unix()))
	require.NoError(t, h.store.DeletePipeline(h.pipeline))

	h.tick(t)
	assert.Empty(t, h.takeStates())
	assert.Nil(t, h.report(t))
}

func TestDispatchDeliveryRepoDeleted(t *testing.T) {
	h := newLivenessHarness(t)
	h.deliverRed(t)
	require.NoError(t, h.store.DeleteRepo(h.repo))

	h.tick(t)
	assert.Empty(t, h.takeStates())
	assert.Nil(t, h.report(t))
}

func TestDispatchDeliveryLateCreatorCanceledOrSkipped(t *testing.T) {
	for _, status := range []model.StatusValue{model.StatusCanceled, model.StatusSkipped} {
		t.Run(string(status), func(t *testing.T) {
			h := newLivenessHarness(t)
			h.deliverRed(t)

			_, err := UpdateStatusToDone(h.store, h.creator(), status, h.clock.Unix())
			require.NoError(t, err)
			h.tick(t)
			posts := h.takePosts()
			require.Len(t, posts, 1)
			assert.Equal(t, model.StatusSuccess, posts[0].workflow.State)
			assert.Equal(t, model.StatusSuccess, posts[0].pipeline.Status)
			r := h.report(t)
			assert.Equal(t, status, r.Delivered)

			h.clock = time.Unix(r.RetireAfter, 0)
			h.tick(t)
			assert.Equal(t, []model.StatusValue{model.StatusSuccess}, h.takeStates())
			assert.Nil(t, h.report(t))
		})
	}
}

func TestDispatchDeliveryTwoServers(t *testing.T) {
	h := newLivenessHarness(t)
	require.NoError(t, flagOrphanedPipeline(h.store, h.pipeline, h.clock.Unix()))
	loaded := h.report(t)
	copyA, copyB := *loaded, *loaded

	serverA := h.newDelivery(h.store)
	serverB := h.newDelivery(h.store)
	require.NoError(t, serverA.deliverDispatchReport(t.Context(), &copyA))
	require.NoError(t, serverB.deliverDispatchReport(t.Context(), &copyB))

	assert.Equal(t, []model.StatusValue{model.StatusError}, h.takeStates())
}

func TestDispatchDeliveryReportLostLease(t *testing.T) {
	for _, creatorStatus := range []model.StatusValue{model.StatusPending, model.StatusSuccess} {
		t.Run(string(creatorStatus), func(t *testing.T) {
			h := newLivenessHarness(t)
			require.NoError(t, flagOrphanedPipeline(h.store, h.pipeline, h.clock.Unix()))
			loaded := h.report(t)

			entered := make(chan struct{})
			release := make(chan struct{})
			h.setOnStatus(func(_ context.Context, _ *model.Pipeline, w *model.Workflow) error {
				if w.State == model.StatusError {
					close(entered)
					<-release
				}
				return nil
			})

			serverA := h.newDelivery(h.store)
			serverA.postTimeout = time.Hour
			resultA := make(chan error, 1)
			go func() { resultA <- serverA.deliverDispatchReport(t.Context(), loaded) }()
			<-entered

			if creatorStatus == model.StatusPending {
				_, err := UpdateToStatusPending(h.store, h.creator(), "")
				require.NoError(t, err)
			} else {
				_, err := UpdateStatusToDone(h.store, h.creator(), creatorStatus, h.clock.Unix())
				require.NoError(t, err)
			}

			// Server B's clock is past A's lease.
			clockB := h.clock.Add(reportLease + time.Second)
			serverB := h.newDelivery(h.store)
			serverB.clock = func() time.Time { return clockB }
			serverB.runDispatchDeliveryTick(t.Context())
			rowB := h.report(t)
			require.Equal(t, creatorStatus, rowB.Delivered)
			if creatorStatus == model.StatusSuccess {
				assert.EqualValues(t, clockB.Add(reportSettle).Unix(), rowB.RetireAfter)
			}

			close(release)
			require.NoError(t, <-resultA)
			assert.Equal(t, rowB, h.report(t))
			assert.Equal(t, []model.StatusValue{model.StatusError, creatorStatus}, h.takeStates())
		})
	}
}

func TestDispatchDeliveryAmbiguousPostThenDelete(t *testing.T) {
	h := newLivenessHarness(t)
	h.delivery.postTimeout = 100 * time.Millisecond
	release := make(chan struct{})
	h.setOnStatus(func(_ context.Context, _ *model.Pipeline, w *model.Workflow) error {
		if w.State == model.StatusError {
			<-release // ignores its context, like an old addon
		}
		return nil
	})

	h.tick(t)
	require.Equal(t, []model.StatusValue{model.StatusError}, h.takeStates())
	r := h.report(t)
	assert.Empty(t, r.Delivered)
	assert.Equal(t, 1, r.Attempts)
	assert.NotEmpty(t, r.LastError)
	assert.EqualValues(t, h.clock.Add(reportRetryBase).Unix(), r.NextAttempt)

	// The post is still in flight, so the due tick only reschedules.
	h.clock = time.Unix(r.NextAttempt, 0)
	h.tick(t)
	assert.Empty(t, h.takeStates())
	r = h.report(t)
	assert.Equal(t, 2, r.Attempts)
	assert.EqualValues(t, h.clock.Add(2*reportRetryBase).Unix(), r.NextAttempt)

	close(release)
	require.Eventually(t, func() bool {
		h.delivery.mu.Lock()
		defer h.delivery.mu.Unlock()
		return len(h.delivery.inFlight) == 0
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, h.store.DeletePipeline(h.pipeline))

	h.clock = time.Unix(r.NextAttempt, 0)
	h.tick(t)
	posts := h.takePosts()
	require.Len(t, posts, 1)
	assert.Equal(t, model.StatusSuccess, posts[0].workflow.State)
	r = h.report(t)
	assert.Equal(t, model.StatusSuccess, r.Delivered)
	assert.EqualValues(t, h.clock.Add(reportSettle).Unix(), r.RetireAfter)
}

func TestDispatchLivenessLoop(t *testing.T) {
	t.Run("young created pipeline is untouched", func(t *testing.T) {
		f := newLivenessFixtureAged(t, false)
		h := newLivenessHarnessFor(t, f)
		h.tick(t)
		assert.Empty(t, h.takeStates())
		assert.Equal(t, model.StatusCreated, f.reload(t).Status)
		assert.Nil(t, h.report(t))
	})

	t.Run("sweeper returns nil on a cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		assert.NoError(t, RunDispatchLivenessSweeper(ctx, datastore.NewTestStore(t)))
	})
}

func TestDispatchDeliveryDeleteAfterPossibleRed(t *testing.T) {
	t.Run("posts success from the snapshot", func(t *testing.T) {
		h := newLivenessHarness(t)
		h.deliverRed(t)
		require.NoError(t, h.store.DeletePipeline(h.pipeline))

		h.tick(t)
		posts := h.takePosts()
		require.Len(t, posts, 1)
		assert.Equal(t, model.StatusSuccess, posts[0].workflow.State)
		assert.Equal(t, model.StatusSuccess, posts[0].pipeline.Status)
		assert.Equal(t, h.pipeline.Commit, posts[0].pipeline.Commit)
		assert.Equal(t, h.pipeline.Event, posts[0].pipeline.Event)
		assert.Equal(t, h.pipeline.Number, posts[0].pipeline.Number)
		r := h.report(t)
		assert.EqualValues(t, h.clock.Add(reportSettle).Unix(), r.RetireAfter)

		h.clock = time.Unix(r.RetireAfter, 0)
		h.tick(t)
		assert.Equal(t, []model.StatusValue{model.StatusSuccess}, h.takeStates())
		assert.Nil(t, h.report(t))
	})

	t.Run("no post after the repo is deleted too", func(t *testing.T) {
		h := newLivenessHarness(t)
		h.deliverRed(t)
		require.NoError(t, h.store.DeletePipeline(h.pipeline))
		require.NoError(t, h.store.DeleteRepo(h.repo))

		h.tick(t)
		assert.Empty(t, h.takeStates())
		assert.Nil(t, h.report(t))
	})
}

// laterRun inserts a later pipeline on the orphan's repo.
func (h *livenessHarness) laterRun(t *testing.T, commit string, event model.WebhookEvent, status model.StatusValue) *model.Pipeline {
	t.Helper()
	p := &model.Pipeline{RepoID: h.repo.ID, Commit: commit, Event: event, Status: status, Branch: "main"}
	require.NoError(t, h.store.CreatePipeline(p))
	require.Greater(t, p.ID, h.pipeline.ID)
	return p
}

func TestDispatchDeliveryLaterRunOnSameCommit(t *testing.T) {
	t.Run("mirrors the later run until it is terminal", func(t *testing.T) {
		h := newLivenessHarness(t)
		h.deliverRed(t)

		later := h.laterRun(t, h.pipeline.Commit, h.pipeline.Event, model.StatusPending)
		h.tick(t)
		posts := h.takePosts()
		require.Len(t, posts, 1)
		assert.Equal(t, model.StatusPending, posts[0].workflow.State)
		assert.Equal(t, h.pipeline.ID, posts[0].pipeline.ID)

		later.Status = model.StatusSuccess
		require.NoError(t, h.store.UpdatePipeline(later))
		h.tick(t)
		assert.Equal(t, []model.StatusValue{model.StatusSuccess}, h.takeStates())

		h.clock = time.Unix(h.report(t).RetireAfter, 0)
		h.tick(t)
		assert.Equal(t, []model.StatusValue{model.StatusSuccess}, h.takeStates())
		assert.Nil(t, h.report(t))
		assert.Equal(t, model.StatusError, h.reload(t).Status)
	})

	t.Run("ignores runs on another commit or event", func(t *testing.T) {
		h := newLivenessHarness(t)
		h.deliverRed(t)
		h.laterRun(t, "other", h.pipeline.Event, model.StatusPending)
		h.laterRun(t, h.pipeline.Commit, model.EventPush, model.StatusPending)

		h.tick(t)
		assert.Empty(t, h.takeStates())
	})

	t.Run("later run ending error settles and retires", func(t *testing.T) {
		h := newLivenessHarness(t)
		h.deliverRed(t)
		h.laterRun(t, h.pipeline.Commit, h.pipeline.Event, model.StatusError)

		h.tick(t)
		assert.Equal(t, []model.StatusValue{model.StatusError}, h.takeStates())
		r := h.report(t)
		require.NotZero(t, r.RetireAfter)

		h.clock = time.Unix(r.RetireAfter, 0)
		h.tick(t)
		assert.Equal(t, []model.StatusValue{model.StatusError}, h.takeStates())
		assert.Nil(t, h.report(t))
	})
}

func TestDispatchDeliverySettleSeesChange(t *testing.T) {
	h := newLivenessHarness(t)
	h.deliverRed(t)
	later := h.laterRun(t, h.pipeline.Commit, h.pipeline.Event, model.StatusPending)
	later.Status = model.StatusSuccess
	require.NoError(t, h.store.UpdatePipeline(later))
	h.tick(t)
	require.Equal(t, []model.StatusValue{model.StatusSuccess}, h.takeStates())
	r := h.report(t)
	require.NotZero(t, r.RetireAfter)

	h.laterRun(t, h.pipeline.Commit, h.pipeline.Event, model.StatusPending)
	h.clock = time.Unix(r.RetireAfter, 0)
	h.tick(t)
	assert.Equal(t, []model.StatusValue{model.StatusPending}, h.takeStates())
	r = h.report(t)
	require.NotNil(t, r)
	assert.Zero(t, r.RetireAfter)
	assert.Equal(t, model.StatusPending, r.Delivered)
}

func TestDispatchDeliveryPostDeadline(t *testing.T) {
	t.Run("bounded by the post timeout", func(t *testing.T) {
		h := newLivenessHarness(t)
		h.delivery.postTimeout = 2 * time.Second
		h.tick(t)
		posts := h.takePosts()
		require.Len(t, posts, 1)
		deadline, ok := posts[0].ctx.Deadline()
		require.True(t, ok)
		assert.False(t, deadline.After(h.clock.Add(2*time.Second)))
		assert.False(t, deadline.After(h.clock.Add(reportLease)))
	})

	t.Run("bounded by the lease", func(t *testing.T) {
		h := newLivenessHarness(t)
		h.delivery.postTimeout = time.Hour
		h.tick(t)
		posts := h.takePosts()
		require.Len(t, posts, 1)
		deadline, ok := posts[0].ctx.Deadline()
		require.True(t, ok)
		assert.False(t, deadline.After(h.clock.Add(reportLease)))
	})

	t.Run("cancelled with the tick", func(t *testing.T) {
		h := newLivenessHarness(t)
		require.NoError(t, flagOrphanedPipeline(h.store, h.pipeline, h.clock.Unix()))
		entered := make(chan struct{})
		postErr := make(chan error, 1)
		h.setOnStatus(func(ctx context.Context, _ *model.Pipeline, _ *model.Workflow) error {
			close(entered)
			<-ctx.Done()
			postErr <- ctx.Err()
			return ctx.Err()
		})

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() {
			h.delivery.runDispatchDeliveryTick(ctx)
			close(done)
		}()
		<-entered
		cancel()
		<-done
		assert.ErrorIs(t, <-postErr, context.Canceled)

		r := h.report(t)
		assert.Empty(t, r.Delivered)
		assert.Zero(t, r.Attempts)
	})
}
