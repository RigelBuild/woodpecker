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

package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pubsub/memory"
	queue_mocks "go.woodpecker-ci.org/woodpecker/v3/server/queue/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/scheduler"
	manager_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/mocks"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

// aggregateResilienceForge is a hand-written fake used to exercise
// updatePipelineStatus. The mockery MockForge cannot serve here: the pipeline
// aggregate is the optional forge.AggregateStatusReporter capability, and
// forge.ReportAggregateStatus type-asserts to it — a type MockForge does not
// satisfy, so the assertion would silently return nil and the aggregate call
// could never be observed. This fake implements both Status and StatusAggregate,
// so ReportAggregateStatus routes through it and we can assert it fired.
//
// It embeds forge.Forge only to satisfy the interface for the fields
// updatePipelineStatus does not touch; any unexpected call on an embedded method
// panics (nil interface), which surfaces accidental new dependencies loudly.
type aggregateResilienceForge struct {
	forge.Forge

	// statusErrForWorkflowID returns an error from Status for exactly one
	// workflow ID, and nil for the rest, so we can simulate a single throttled
	// per-workflow report inside the loop.
	statusErrForWorkflowID int64

	statusCalledFor []int64
	aggregateCalls  int

	// metaCalls counts StatusMeta invocations, and metaWorkflows captures the
	// workflow set the last meta call was handed, so a test can assert both that
	// the meta post fired beside the aggregate and what it rolled up.
	metaCalls     int
	metaWorkflows []*model.Workflow
}

func (f *aggregateResilienceForge) Status(_ context.Context, _ *model.User, _ *model.Repo, _ *model.Pipeline, workflow *model.Workflow) error {
	f.statusCalledFor = append(f.statusCalledFor, workflow.ID)
	if workflow.ID == f.statusErrForWorkflowID {
		return errors.New("simulated per-workflow status failure (e.g. throttled POST)")
	}
	return nil
}

func (f *aggregateResilienceForge) StatusAggregate(_ context.Context, _ *model.User, _ *model.Repo, _ *model.Pipeline) error {
	f.aggregateCalls++
	return nil
}

func (f *aggregateResilienceForge) StatusMeta(_ context.Context, _ *model.User, _ *model.Repo, _ *model.Pipeline, workflows []*model.Workflow) error {
	f.metaCalls++
	f.metaWorkflows = workflows
	return nil
}

// setStatusAggregate toggles the process-global StatusAggregate flag for the
// duration of a test, restoring the previous value on cleanup.
func setStatusAggregate(t *testing.T, v bool) {
	t.Helper()
	orig := server.Config.Server.StatusAggregate
	server.Config.Server.StatusAggregate = v
	t.Cleanup(func() { server.Config.Server.StatusAggregate = orig })
}

// setStatusPerWorkflow toggles the process-global StatusPerWorkflow flag for the
// duration of a test, restoring the previous value on cleanup. The per-workflow
// loop in updatePipelineStatus is gated on it, and it defaults to the Go
// zero-value (false) in the test binary, so any test that exercises the loop must
// set it explicitly.
func setStatusPerWorkflow(t *testing.T, v bool) {
	t.Helper()
	orig := server.Config.Server.StatusPerWorkflow
	server.Config.Server.StatusPerWorkflow = v
	t.Cleanup(func() { server.Config.Server.StatusPerWorkflow = orig })
}

// setStatusMeta toggles the process-global StatusMeta flag for the duration of a
// test, restoring the previous value on cleanup. The flag is what gates the meta
// report in updatePipelineStatus, so a test exercising the meta post must set it
// explicitly.
func setStatusMeta(t *testing.T, v bool) {
	t.Helper()
	orig := server.Config.Server.StatusMeta
	server.Config.Server.StatusMeta = v
	t.Cleanup(func() { server.Config.Server.StatusMeta = orig })
}

func threeWorkflowPipeline() (*model.Pipeline, *model.Repo, *model.User) {
	pipeline := &model.Pipeline{
		Number: 1,
		Event:  model.EventPull,
		Status: model.StatusSuccess,
		Workflows: []*model.Workflow{
			{ID: 1, Name: "lint", State: model.StatusSkipped},
			{ID: 2, Name: "test", State: model.StatusSuccess},
			{ID: 3, Name: "build", State: model.StatusSuccess},
		},
	}
	repo := &model.Repo{Owner: "o", Name: "r", FullName: "o/r"}
	user := &model.User{AccessToken: "x"}
	return pipeline, repo, user
}

// TestUpdatePipelineStatusAggregateSurvivesPerWorkflowError is the "stuck
// pending" regression. A single per-workflow Status failure (e.g. one throttled
// POST) must NOT abort the loop or skip the pipeline-level aggregate: the
// aggregate is the required branch-protection check, so it has to run even when
// an earlier workflow report failed.
//
// It asserts two things a flipped condition or an early return would break:
//  1. Status was called for EVERY workflow (the loop did not abort on the first
//     error), and
//  2. StatusAggregate was still invoked exactly once despite that error.
//
// Pre-fix relevance: the loop `return`ed on the first Status error, so with the
// first workflow failing, Status ran once (not three times) and the aggregate
// never ran — leaving the required check stuck "pending". Restore that early
// return and both assertions below fail.
func TestUpdatePipelineStatusAggregateSurvivesPerWorkflowError(t *testing.T) {
	setStatusAggregate(t, true)
	setStatusPerWorkflow(t, true)

	f := &aggregateResilienceForge{statusErrForWorkflowID: 1} // first workflow fails
	pipeline, repo, user := threeWorkflowPipeline()

	updatePipelineStatus(context.Background(), f, store_mocks.NewMockStore(t), pipeline, repo, user)

	assert.Equal(t, []int64{1, 2, 3}, f.statusCalledFor,
		"a per-workflow Status error must not abort the loop; every workflow must still be reported")
	assert.Equal(t, 1, f.aggregateCalls,
		"the pipeline-level aggregate must still run exactly once despite a per-workflow error")
}

// TestUpdatePipelineStatusAggregateOnAllSuccess guards the happy path: when every
// per-workflow Status succeeds and StatusAggregate is enabled, the aggregate is
// reported exactly once. This pins that the resilience fix did not accidentally
// double-report or drop the aggregate on the common all-green path.
func TestUpdatePipelineStatusAggregateOnAllSuccess(t *testing.T) {
	setStatusAggregate(t, true)
	setStatusPerWorkflow(t, true)

	f := &aggregateResilienceForge{statusErrForWorkflowID: -1} // no workflow fails
	pipeline, repo, user := threeWorkflowPipeline()

	updatePipelineStatus(context.Background(), f, store_mocks.NewMockStore(t), pipeline, repo, user)

	assert.Equal(t, []int64{1, 2, 3}, f.statusCalledFor,
		"every workflow must be reported on the happy path")
	assert.Equal(t, 1, f.aggregateCalls,
		"the aggregate must be reported exactly once when StatusAggregate is enabled")
}

// TestUpdatePipelineStatusNoAggregateWhenDisabled pins that the aggregate is
// gated on the StatusAggregate flag: with it off, per-workflow statuses are still
// reported but the aggregate is never called. This keeps the aggregate assertions
// above honest — they prove the flag drives the call, not that it always fires.
func TestUpdatePipelineStatusNoAggregateWhenDisabled(t *testing.T) {
	setStatusAggregate(t, false)
	setStatusPerWorkflow(t, true)

	f := &aggregateResilienceForge{statusErrForWorkflowID: -1}
	pipeline, repo, user := threeWorkflowPipeline()

	updatePipelineStatus(context.Background(), f, store_mocks.NewMockStore(t), pipeline, repo, user)

	assert.Equal(t, []int64{1, 2, 3}, f.statusCalledFor,
		"every workflow must be reported regardless of the aggregate flag")
	require.Zero(t, f.aggregateCalls,
		"the aggregate must not be reported when StatusAggregate is disabled")
}

// TestUpdatePipelineStatusPerWorkflowDisabledSkipsPerWorkflowStatus is the core
// rate-limit regression this gate exists to prevent. With StatusPerWorkflow off
// (StatusAggregate still on), updatePipelineStatus must post ZERO per-workflow
// forge Status writes — on an affected-aware fan-out those per-workflow POSTs are
// exactly what trips the forge's rate limit — while the pipeline-level aggregate
// (the required branch-protection check) must still fire exactly once.
//
// Red check: remove the `if server.Config.Server.StatusPerWorkflow` guard around
// the loop in helper.go (or invert it) and the loop runs regardless, so
// statusCalledFor becomes [1,2,3] and the Empty assertion fails.
func TestUpdatePipelineStatusPerWorkflowDisabledSkipsPerWorkflowStatus(t *testing.T) {
	setStatusPerWorkflow(t, false)
	setStatusAggregate(t, true)

	f := &aggregateResilienceForge{statusErrForWorkflowID: -1} // no workflow fails
	pipeline, repo, user := threeWorkflowPipeline()

	updatePipelineStatus(context.Background(), f, store_mocks.NewMockStore(t), pipeline, repo, user)

	assert.Empty(t, f.statusCalledFor,
		"no per-workflow Status must be posted when StatusPerWorkflow is disabled")
	assert.Equal(t, 1, f.aggregateCalls,
		"the pipeline-level aggregate must still run exactly once when only StatusPerWorkflow is disabled")
}

// TestUpdatePipelineStatusPerWorkflowEnabledReportsEveryWorkflow pins the ON
// branch of the gate (the upstream-compatible default): with StatusPerWorkflow
// enabled, every workflow gets its own forge Status write, and the aggregate
// still fires when enabled. Paired with the disabled test above, these prove the
// flag — not incidental behavior — drives whether per-workflow statuses are
// posted.
//
// Red check: hardcode the gate to false (loop never runs) and statusCalledFor is
// empty, so the [1,2,3] assertion fails.
func TestUpdatePipelineStatusPerWorkflowEnabledReportsEveryWorkflow(t *testing.T) {
	setStatusPerWorkflow(t, true)
	setStatusAggregate(t, true)

	f := &aggregateResilienceForge{statusErrForWorkflowID: -1} // no workflow fails
	pipeline, repo, user := threeWorkflowPipeline()

	updatePipelineStatus(context.Background(), f, store_mocks.NewMockStore(t), pipeline, repo, user)

	assert.Equal(t, []int64{1, 2, 3}, f.statusCalledFor,
		"every workflow must be reported when StatusPerWorkflow is enabled")
	assert.Equal(t, 1, f.aggregateCalls,
		"the aggregate must still fire exactly once when StatusPerWorkflow is enabled")
}

// TestUpdatePipelineStatusAllReportingDisabledPostsNothing pins the both-off
// corner: with StatusPerWorkflow and StatusAggregate both disabled,
// updatePipelineStatus must perform no forge writes whatsoever — neither a
// per-workflow Status nor the aggregate. This keeps the two gates independent and
// guards against either one leaking a write when both are meant to be silent.
//
// Red check: either gate defaulting open (loop or aggregate running unguarded)
// reddens one of the two assertions below.
func TestUpdatePipelineStatusAllReportingDisabledPostsNothing(t *testing.T) {
	setStatusPerWorkflow(t, false)
	setStatusAggregate(t, false)

	f := &aggregateResilienceForge{statusErrForWorkflowID: -1} // no workflow fails
	pipeline, repo, user := threeWorkflowPipeline()

	updatePipelineStatus(context.Background(), f, store_mocks.NewMockStore(t), pipeline, repo, user)

	assert.Empty(t, f.statusCalledFor,
		"no per-workflow Status must be posted when StatusPerWorkflow is disabled")
	require.Zero(t, f.aggregateCalls,
		"the aggregate must not be posted when StatusAggregate is disabled")
}

// TestUpdatePipelineStatusMetaFlagOffPostsNothing re-states the former
// TestStatusMetaEmptyConfigPostsNothing as the intrinsic flag-OFF contract: with
// StatusMeta disabled, even a pipeline that carries a real meta gate
// (OnMetadataEdit=true) triggers no meta report. The server flag is the master
// switch; gate membership alone never posts.
func TestUpdatePipelineStatusMetaFlagOffPostsNothing(t *testing.T) {
	setStatusPerWorkflow(t, false)
	setStatusAggregate(t, false)
	setStatusMeta(t, false)

	f := &aggregateResilienceForge{statusErrForWorkflowID: -1}
	pipeline := metaGatePipeline(1, model.EventPull, model.StatusSuccess, "abc123")
	repo := &model.Repo{Owner: "o", Name: "r", FullName: "o/r"}
	user := &model.User{AccessToken: "x"}

	updatePipelineStatus(context.Background(), f, store_mocks.NewMockStore(t), pipeline, repo, user)

	assert.Zero(t, f.metaCalls,
		"StatusMeta disabled must post no meta status even when a real meta gate is present")
}

// metaGatePipeline returns a pull_request pipeline carrying one meta gate
// (spec-impact) plus a code workflow, on the given commit + a known PR ref, so
// the meta tests below share one shape. The number arg lets a test place it
// earlier/later in the two-writer ordering, and the commit arg lets a test vary
// the head commit to exercise the commit-scoping discriminator.
func metaGatePipeline(number int64, event model.WebhookEvent, gateState model.StatusValue, commit string) *model.Pipeline {
	return &model.Pipeline{
		Number: number,
		Event:  event,
		Status: model.StatusSuccess,
		Commit: commit,
		Ref:    "refs/pull/7/head",
		Workflows: []*model.Workflow{
			{ID: 1, Name: "build", State: model.StatusSuccess},
			{ID: 2, Name: "spec-impact", State: gateState, OnMetadataEdit: true},
		},
	}
}

// TestUpdatePipelineStatusMetaFiresBesideAggregate is the happy-path coverage the
// design calls for: on the shared updatePipelineStatus path, with the meta
// feature configured, the meta POST fires exactly once BESIDE the aggregate — the
// two are independent sibling reports, not one masking the other. It also proves
// the meta report is handed the pipeline's own (already-loaded) workflows.
func TestUpdatePipelineStatusMetaFiresBesideAggregate(t *testing.T) {
	setStatusPerWorkflow(t, true)
	setStatusAggregate(t, true)
	setStatusMeta(t, true)

	s := store_mocks.NewMockStore(t)
	// Freshness query: this pipeline is the only meta-carrying one for the commit,
	// so nothing is newer and the report proceeds.
	s.On("GetPipelineList", mock.Anything, mock.Anything, mock.Anything).
		Return([]*model.Pipeline{metaGatePipeline(1, model.EventPull, model.StatusSuccess, "abc123")}, nil)

	f := &aggregateResilienceForge{statusErrForWorkflowID: -1}
	pipeline := metaGatePipeline(1, model.EventPull, model.StatusSuccess, "abc123")
	repo := &model.Repo{Owner: "o", Name: "r", FullName: "o/r"}
	user := &model.User{AccessToken: "x"}

	updatePipelineStatus(context.Background(), f, s, pipeline, repo, user)

	assert.Equal(t, 1, f.aggregateCalls, "the code aggregate must still fire exactly once")
	assert.Equal(t, 1, f.metaCalls, "the meta status must fire exactly once beside the aggregate")
	assert.Equal(t, []int64{1, 2}, f.statusCalledFor, "per-workflow statuses are unaffected by the meta report")
}

// TestReportMetaStatusSkipsWhenLaterMetaPipelineExists is the Q1 freshness rule,
// SKIP direction — and specifically the gate-BYPASS case: a PR opened green (the
// slow pull_request pipeline P1 rolls its meta gate green), the title is then
// edited bad and a fresher pull_request_metadata pipeline P2 posts red. When the
// slow P1 finally finishes, it must NOT re-post its stale green over P2's red —
// otherwise a bad-title PR merges. Because the store holds a LATER meta-carrying
// pipeline (P2, higher Number) for the same commit + PR, P1's terminal meta post
// is SKIPPED (no StatusMeta call).
func TestReportMetaStatusSkipsWhenLaterMetaPipelineExists(t *testing.T) {
	setStatusMeta(t, true)

	slowP1 := metaGatePipeline(1, model.EventPull, model.StatusSuccess, "abc123")          // opened green
	freshP2 := metaGatePipeline(2, model.EventPullMetadata, model.StatusFailure, "abc123") // edit-bad, red

	s := store_mocks.NewMockStore(t)
	// Pin the query shape: the freshness scan must be event-filtered to the two
	// meta-carrying PR events and ref-scoped to this pipeline's ref.
	s.On("GetPipelineList", mock.Anything, mock.Anything, mock.MatchedBy(func(filter *model.PipelineFilter) bool {
		return assert.ObjectsAreEqual([]model.WebhookEvent{model.EventPull, model.EventPullMetadata}, filter.Events) &&
			filter.RefContains == slowP1.Ref
	})).Return([]*model.Pipeline{slowP1, freshP2}, nil)

	f := &aggregateResilienceForge{statusErrForWorkflowID: -1}
	repo := &model.Repo{Owner: "o", Name: "r", FullName: "o/r"}
	user := &model.User{AccessToken: "x"}

	err := forge.ReportMetaStatus(context.Background(), f, s, user, repo, slowP1)
	require.NoError(t, err)
	assert.Zero(t, f.metaCalls,
		"a slow pipeline must NOT re-post its stale verdict when a later meta pipeline exists (gate-bypass guard)")
}

// TestReportMetaStatusPostsWhenNewest is the Q1 freshness rule, POST direction:
// the newest meta-carrying pipeline for the commit + PR (no later one in the
// store) DOES post — so the freshness guard suppresses only stale writers, never
// the current verdict.
func TestReportMetaStatusPostsWhenNewest(t *testing.T) {
	setStatusMeta(t, true)

	slowP1 := metaGatePipeline(1, model.EventPull, model.StatusSuccess, "abc123")
	freshP2 := metaGatePipeline(2, model.EventPullMetadata, model.StatusFailure, "abc123")

	s := store_mocks.NewMockStore(t)
	s.On("GetPipelineList", mock.Anything, mock.Anything, mock.Anything).
		Return([]*model.Pipeline{slowP1, freshP2}, nil)

	f := &aggregateResilienceForge{statusErrForWorkflowID: -1}
	repo := &model.Repo{Owner: "o", Name: "r", FullName: "o/r"}
	user := &model.User{AccessToken: "x"}

	err := forge.ReportMetaStatus(context.Background(), f, s, user, repo, freshP2)
	require.NoError(t, err)
	assert.Equal(t, 1, f.metaCalls, "the newest meta-carrying pipeline must post its verdict")
	require.Len(t, f.metaWorkflows, 2, "the newest pipeline's own workflows must be handed to the meta report")
}

// TestReportMetaStatusPostsWhenLaterPipelineIsDifferentCommit pins the
// commit-scoping discriminator (p.Commit == b.Commit) in hasLaterMetaPipeline: a
// HIGHER-Number meta pipeline for a DIFFERENT commit (a different PR head) must
// NOT be treated as "later" for this commit, so this pipeline still posts. This
// kills a mutation that drops the commit check and would let any newer PR
// pipeline anywhere suppress this commit's verdict.
func TestReportMetaStatusPostsWhenLaterPipelineIsDifferentCommit(t *testing.T) {
	setStatusMeta(t, true)

	mine := metaGatePipeline(1, model.EventPull, model.StatusFailure, "abc123")
	otherCommit := metaGatePipeline(2, model.EventPullMetadata, model.StatusSuccess, "def456")

	s := store_mocks.NewMockStore(t)
	s.On("GetPipelineList", mock.Anything, mock.Anything, mock.Anything).
		Return([]*model.Pipeline{mine, otherCommit}, nil)

	f := &aggregateResilienceForge{statusErrForWorkflowID: -1}
	repo := &model.Repo{Owner: "o", Name: "r", FullName: "o/r"}
	user := &model.User{AccessToken: "x"}

	err := forge.ReportMetaStatus(context.Background(), f, s, user, repo, mine)
	require.NoError(t, err)
	assert.Equal(t, 1, f.metaCalls,
		"a higher-Number pipeline for a DIFFERENT commit must not suppress this commit's verdict")
}

// TestReportMetaStatusPostsWhenLaterMetaPipelineErrored is the M1 correctness
// guard: a NEWER same-commit pipeline that died at config-fetch is persisted in
// StatusError with ZERO workflows and never posts a CI (meta) verdict. It must
// NOT suppress this older pipeline's terminal post — otherwise the required
// CI (meta) check is stranded pending forever. So this pipeline STILL posts.
func TestReportMetaStatusPostsWhenLaterMetaPipelineErrored(t *testing.T) {
	setStatusMeta(t, true)

	slowP1 := metaGatePipeline(1, model.EventPull, model.StatusSuccess, "abc123")
	// P2: newer, same commit, but errored before workflows were persisted.
	erroredP2 := &model.Pipeline{
		Number:    2,
		Event:     model.EventPullMetadata,
		Status:    model.StatusError,
		Commit:    "abc123",
		Ref:       "refs/pull/7/head",
		Workflows: nil,
	}

	s := store_mocks.NewMockStore(t)
	s.On("GetPipelineList", mock.Anything, mock.Anything, mock.Anything).
		Return([]*model.Pipeline{slowP1, erroredP2}, nil)

	f := &aggregateResilienceForge{statusErrForWorkflowID: -1}
	repo := &model.Repo{Owner: "o", Name: "r", FullName: "o/r"}
	user := &model.User{AccessToken: "x"}

	err := forge.ReportMetaStatus(context.Background(), f, s, user, repo, slowP1)
	require.NoError(t, err)
	assert.Equal(t, 1, f.metaCalls,
		"a newer pipeline that errored before posting a verdict must not suppress this terminal report")
}

// TestCancelPostsTerminalMetaWithNilWorkflows is the Q2 terminal-never-pending
// guarantee, exercised through the REAL cancel.go path (not the happy helper.go
// path). Cancel calls updatePipelineStatus with killedPipeline.Workflows == nil
// (the tree is loaded AFTER, at cancel.go:86). If ReportMetaStatus trusted that
// nil it would no-op and strand CI (meta) pending forever. Instead it self-loads
// the tree, so the meta post still fires with a TERMINAL rollup: the pending gate
// was skipped by the cancel, and an all-skipped meta set rolls up to success —
// terminal, not pending.
func TestCancelPostsTerminalMetaWithNilWorkflows(t *testing.T) {
	setStatusMeta(t, true)

	s := store_mocks.NewMockStore(t)
	// The gate is pending on the first tree read (so Cancel skips it), then
	// persisted as skipped — the DB read the self-load sees reflects that skip,
	// exactly as production WorkflowGetTree would after the WorkflowUpdate below.
	var treeCalls int
	s.EXPECT().WorkflowGetTree(mock.Anything).RunAndReturn(func(_ *model.Pipeline) ([]*model.Workflow, error) {
		treeCalls++
		state := model.StatusPending
		if treeCalls > 1 {
			state = model.StatusSkipped
		}
		return []*model.Workflow{{ID: 2, Name: "spec-impact", State: state, OnMetadataEdit: true}}, nil
	})
	s.On("WorkflowUpdate", mock.Anything).Return(nil)
	s.On("UpdatePipeline", mock.Anything).Return(nil)
	// Freshness: this pipeline is the only meta-carrying one for the commit.
	s.On("GetPipelineList", mock.Anything, mock.Anything, mock.Anything).
		Return([]*model.Pipeline{{Number: 5, Event: model.EventPull, Commit: "abc123"}}, nil)

	q := queue_mocks.NewMockQueue(t)
	q.On("ErrorAtOnce", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	origScheduler := server.Config.Services.Scheduler
	server.Config.Services.Scheduler = scheduler.NewScheduler(context.Background(), s, q, memory.New())
	t.Cleanup(func() { server.Config.Services.Scheduler = origScheduler })

	f := &aggregateResilienceForge{statusErrForWorkflowID: -1}
	repo := &model.Repo{Owner: "o", Name: "r", FullName: "o/r"}
	user := &model.User{AccessToken: "x"}
	pipeline := &model.Pipeline{
		Number: 5,
		Event:  model.EventPull,
		Status: model.StatusRunning,
		Commit: "abc123",
		Ref:    "refs/pull/7/head",
		// Workflows deliberately nil: cancel.go loads the tree locally, so
		// killedPipeline reaches updatePipelineStatus with nil Workflows.
	}

	err := Cancel(context.Background(), f, s, repo, user, pipeline, &model.CancelInfo{})
	require.NoError(t, err)
	require.Equal(t, 1, f.metaCalls,
		"the canceled pipeline must still post CI (meta) even though its Workflows were nil at the call site")
	require.NotEmpty(t, f.metaWorkflows, "ReportMetaStatus must self-load the tree rather than no-op on nil Workflows")
	assert.Equal(t, model.StatusSuccess, PipelineStatus(f.metaWorkflows),
		"the meta rollup must be TERMINAL (an all-skipped gate set rolls up to success), never left pending")
}

// TestDeclinePostsTerminalMeta is the decline-path counterpart of the cancel
// guard: a DECLINED pipeline whose meta gate is in the tree must post a TERMINAL
// CI (meta) and never leave it pending. The decline path loads the tree
// (decline.go:47) BEFORE updatePipelineStatus (decline.go:65) and sets every
// workflow to declined, so the meta rollup is declined -- a terminal state. This
// guards against a future refactor that moves the tree-load after
// updatePipelineStatus, which would hand ReportMetaStatus an empty tree and
// strand the check.
func TestDeclinePostsTerminalMeta(t *testing.T) {
	setStatusMeta(t, true)

	s := store_mocks.NewMockStore(t)
	s.EXPECT().WorkflowGetTree(mock.Anything).Return([]*model.Workflow{
		{ID: 2, Name: "spec-impact", State: model.StatusBlocked, OnMetadataEdit: true},
	}, nil)
	s.On("WorkflowUpdate", mock.Anything).Return(nil)
	s.On("UpdatePipeline", mock.Anything).Return(nil)
	// Freshness: this pipeline is the only meta-carrying one for the commit.
	s.On("GetPipelineList", mock.Anything, mock.Anything, mock.Anything).
		Return([]*model.Pipeline{{Number: 5, Event: model.EventPull, Commit: "abc123"}}, nil)

	f := &aggregateResilienceForge{statusErrForWorkflowID: -1}
	mgr := manager_mocks.NewMockManager(t)
	mgr.On("ForgeFromRepo", mock.Anything).Return(f, nil)
	origManager := server.Config.Services.Manager
	server.Config.Services.Manager = mgr
	t.Cleanup(func() { server.Config.Services.Manager = origManager })

	q := queue_mocks.NewMockQueue(t)
	origScheduler := server.Config.Services.Scheduler
	server.Config.Services.Scheduler = scheduler.NewScheduler(context.Background(), s, q, memory.New())
	t.Cleanup(func() { server.Config.Services.Scheduler = origScheduler })

	repo := &model.Repo{Owner: "o", Name: "r", FullName: "o/r"}
	user := &model.User{AccessToken: "x", Login: "reviewer"}
	pipeline := &model.Pipeline{
		Number: 5,
		Event:  model.EventPull,
		Status: model.StatusBlocked,
		Commit: "abc123",
		Ref:    "refs/pull/7/head",
	}

	_, err := Decline(context.Background(), s, pipeline, user, repo)
	require.NoError(t, err)
	require.Equal(t, 1, f.metaCalls,
		"a declined pipeline must still post CI (meta) with its tree loaded before the status update")
	require.NotEmpty(t, f.metaWorkflows, "ReportMetaStatus must receive the declined tree, not an empty one")
	assert.Equal(t, model.StatusDeclined, PipelineStatus(f.metaWorkflows),
		"the meta rollup must be TERMINAL (declined), never left pending")
}

// TestReportMetaStatusSkipsNonPullEvents pins the event-scope guard
// (forge.go: only EventPull / EventPullMetadata carry meta gates): a push
// pipeline must trigger NEITHER the freshness query NOR a meta post. The
// MockStore has no GetPipelineList expectation, so any freshness query would
// panic, and metaCalls must stay zero.
func TestReportMetaStatusSkipsNonPullEvents(t *testing.T) {
	setStatusMeta(t, true)

	s := store_mocks.NewMockStore(t)

	f := &aggregateResilienceForge{statusErrForWorkflowID: -1}
	repo := &model.Repo{Owner: "o", Name: "r", FullName: "o/r"}
	user := &model.User{AccessToken: "x"}
	pipeline := metaGatePipeline(1, model.EventPush, model.StatusSuccess, "abc123")

	err := forge.ReportMetaStatus(context.Background(), f, s, user, repo, pipeline)
	require.NoError(t, err)
	assert.Zero(t, f.metaCalls, "a non-PR event must not post CI (meta)")
}
