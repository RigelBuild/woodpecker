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

package rpc

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/rpc"
	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	queue_mocks "go.woodpecker-ci.org/woodpecker/v3/server/queue/mocks"
	log_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/log/mocks"
	manager_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/mocks"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

// doneGuardForge records the aggregate POSTs made while a Done call is in
// flight. Like perWorkflowGateForge it must be hand-written rather than the
// mockery MockForge: the aggregate goes through forge.ReportAggregateStatus,
// which type-asserts to the optional forge.AggregateStatusReporter — an
// assertion MockForge fails, so the call would silently vanish and the test
// could never observe it.
//
// The reports are made from a detached goroutine (reportForgeStatusAsync), so
// the counters are mutex-guarded; newTestRPC's reportWG is what makes the
// observation deterministic.
type doneGuardForge struct {
	forge.Forge

	mu                 sync.Mutex
	aggregateCalls     int
	aggregatePipelines []model.StatusValue
	statusCalls        int
}

func (f *doneGuardForge) Status(_ context.Context, _ *model.User, _ *model.Repo, _ *model.Pipeline, _ *model.Workflow) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statusCalls++
	return nil
}

func (f *doneGuardForge) StatusAggregate(_ context.Context, _ *model.User, _ *model.Repo, p *model.Pipeline, _ []*model.Workflow) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.aggregateCalls++
	f.aggregatePipelines = append(f.aggregatePipelines, p.Status)
	return nil
}

func (f *doneGuardForge) snapshot() (calls int, statuses []model.StatusValue) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.aggregateCalls, append([]model.StatusValue(nil), f.aggregatePipelines...)
}

// installDoneGuardForge points the services Manager at the supplied fake for the
// duration of the test, restoring the process-global on cleanup.
func installDoneGuardForge(t *testing.T, f forge.Forge) {
	t.Helper()
	mgr := manager_mocks.NewMockManager(t)
	mgr.On("ForgeFromRepo", mock.Anything).Return(f, nil).Maybe()
	orig := server.Config.Services.Manager
	server.Config.Services.Manager = mgr
	t.Cleanup(func() { server.Config.Services.Manager = orig })
}

// TestRPCDoneGuardHitPostsTerminalAggregate is the RIG-1129 regression. An agent
// fault kills a step, the cascade-cancel drives every workflow terminal, and the
// agent then re-Dones the already-terminal workflow. checkWorkflowState rejects
// that state change — correctly — but pre-fix it `return err`ed BEFORE the only
// forge report on the Done path (rpc.go reportForgeStatusAsync), so the required
// CI (pr) check stayed pending forever on a pipeline that had already finished.
//
// Post-fix the guard-hit loads the workflow tree, sees no running stage, and
// posts the terminal aggregate through the SAME async poster the clean path
// uses — then still returns the rejection.
//
// Red pre-fix: `return err` fires immediately, so zero aggregate POSTs are
// recorded and the require.Equal(1, calls) fails with "expected: 1 / actual: 0"
// (the rejection assertion passes in both directions — it is the "don't weaken
// the guard" half of the contract, not the repro).
func TestRPCDoneGuardHitPostsTerminalAggregate(t *testing.T) {
	setStatusFlags(t, false, true)

	f := &doneGuardForge{}
	installDoneGuardForge(t, f)

	mockStore := store_mocks.NewMockStore(t)
	agent := defaultAgent()
	// The pipeline already reached a terminal state via the cascade-cancel.
	pipeline := defaultPipeline(model.StatusKilled)
	// …and the workflow the agent re-Dones is already terminal, which is exactly
	// what trips checkWorkflowState.
	workflow := defaultWorkflow(model.StatusKilled)

	mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
	mockStore.On("StepListFromWorkflowFind", mock.Anything).Return([]*model.Step{}, nil)
	mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
	mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)
	mockStore.On("AgentFind", int64(1)).Return(agent, nil)
	// The guard-hit must load the tree itself: at this point in Done it has not
	// been loaded yet. Every workflow is terminal, so no stage is running.
	mockStore.On("WorkflowGetTree", mock.Anything).
		Return([]*model.Workflow{{ID: 30, PipelineID: 20, State: model.StatusKilled}}, nil)
	// Resolved inside the async poster (updateForgeStatus).
	mockStore.On("GetUser", mock.Anything).Return(&model.User{ID: 1, AccessToken: "x"}, nil).Maybe()

	rpcInst := newTestRPC(t, mockStore, nil)
	ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

	err := rpcInst.Done(ctx, "30", rpc.WorkflowState{Finished: 200})

	// The guard must STILL reject the illegal state change: this fix adds a
	// missing report, it never loosens the rejection.
	require.ErrorIs(t, err, ErrAgentIllegalWorkflowReRunStateChange,
		"the guard must keep rejecting the double-finish state change")

	// The report is detached; wait for it before asserting.
	rpcInst.reportWG.Wait()

	calls, statuses := f.snapshot()
	require.Equal(t, 1, calls,
		"a terminal pipeline hitting the Done guard must POST its terminal aggregate exactly once (RIG-1129)")
	require.Len(t, statuses, 1)
	assert.True(t, statuses[0].IsTerminal(),
		"the aggregate posted on the guard-hit must carry a TERMINAL pipeline status, never pending")
}

// TestRPCDoneGuardHitSkipsAggregateWhenPipelineStillRunning is the terminal-only
// constraint. A workflow can hit the Done guard (e.g. a blocked workflow, or one
// re-Doned) while SIBLING workflows are still running — the pipeline is not
// terminal yet, and posting an aggregate here would report a verdict for a
// pipeline that has not finished. The clean path will post when the last workflow
// finishes.
//
// This holds in BOTH directions by design (pre-fix nothing posts at all): it is
// the terminal-only tripwire, not a red repro. Drop the !IsThereRunningStage
// condition and it reddens immediately — aggregateCalls becomes 1. The tree
// expectation is Maybe() so pre-fix (where the guard-hit never loads it) the
// result is a clean pass rather than mock-bookkeeping noise.
func TestRPCDoneGuardHitSkipsAggregateWhenPipelineStillRunning(t *testing.T) {
	setStatusFlags(t, false, true)

	f := &doneGuardForge{}
	installDoneGuardForge(t, f)

	mockStore := store_mocks.NewMockStore(t)
	agent := defaultAgent()
	pipeline := defaultPipeline(model.StatusRunning)
	workflow := defaultWorkflow(model.StatusSuccess) // terminal -> trips the guard

	mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
	mockStore.On("StepListFromWorkflowFind", mock.Anything).Return([]*model.Step{}, nil)
	mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
	mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)
	mockStore.On("AgentFind", int64(1)).Return(agent, nil)
	// A sibling workflow is still running, so the pipeline is NOT terminal.
	mockStore.On("WorkflowGetTree", mock.Anything).Return([]*model.Workflow{
		{ID: 30, PipelineID: 20, State: model.StatusSuccess},
		{ID: 31, PipelineID: 20, State: model.StatusRunning},
	}, nil).Maybe()
	mockStore.On("GetUser", mock.Anything).Return(&model.User{ID: 1, AccessToken: "x"}, nil).Maybe()

	rpcInst := newTestRPC(t, mockStore, nil)
	ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

	err := rpcInst.Done(ctx, "30", rpc.WorkflowState{Finished: 200})
	require.ErrorIs(t, err, ErrAgentIllegalWorkflowReRunStateChange)

	rpcInst.reportWG.Wait()

	calls, _ := f.snapshot()
	assert.Zero(t, calls,
		"a still-running pipeline must not have a terminal aggregate posted early on a guard-hit")
}

// TestRPCDoneGuardHitOnBlockedWorkflowPostsNothing pins the trap that
// IsThereRunningStage alone walks into. checkWorkflowState rejects TWO shapes:
// an already-terminal workflow (the RIG-1129 double-finish) and a BLOCKED one
// (awaiting approval, ErrAgentIllegalWorkflowRun). A blocked workflow is neither
// pending nor running, so IsThereRunningStage reports "no running stage" for a
// pipeline that has not run at all — and an unguarded guard-hit POST would
// publish a verdict for work still waiting on a human.
//
// The pipeline's own stored status is the discriminator, and checking it FIRST
// also means this path does no store read: the MockStore carries no
// WorkflowGetTree expectation, so a tree load here fails the test loudly.
//
// Drop the currentPipeline.Status.IsTerminal() precondition and this reddens on
// the unexpected WorkflowGetTree call.
func TestRPCDoneGuardHitOnBlockedWorkflowPostsNothing(t *testing.T) {
	setStatusFlags(t, false, true)

	f := &doneGuardForge{}
	installDoneGuardForge(t, f)

	mockStore := store_mocks.NewMockStore(t)
	agent := defaultAgent()
	// Pipeline is blocked awaiting approval — NOT terminal.
	pipeline := defaultPipeline(model.StatusBlocked)
	workflow := defaultWorkflow(model.StatusBlocked)

	mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
	mockStore.On("StepListFromWorkflowFind", mock.Anything).Return([]*model.Step{}, nil)
	mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
	mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)
	mockStore.On("AgentFind", int64(1)).Return(agent, nil)

	rpcInst := newTestRPC(t, mockStore, nil)
	ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

	err := rpcInst.Done(ctx, "30", rpc.WorkflowState{Finished: 200})
	require.ErrorIs(t, err, ErrAgentIllegalWorkflowRun,
		"a blocked workflow must still be rejected with the blocked-run error")

	rpcInst.reportWG.Wait()

	calls, _ := f.snapshot()
	assert.Zero(t, calls,
		"a blocked (non-terminal) pipeline must never have a verdict posted on a guard-hit")
	mockStore.AssertNotCalled(t, "WorkflowGetTree", mock.Anything)
}

// TestRPCDoneHappyPathPostsAggregateOnce pins the no-double-post property the
// guard-hit branch buys by construction: the clean Done path is untouched and
// still posts exactly one aggregate. If the new POST were made unconditional
// (rather than scoped to the guard-hit branch), this would see 2.
func TestRPCDoneHappyPathPostsAggregateOnce(t *testing.T) {
	setStatusFlags(t, false, true)

	f := &doneGuardForge{}
	installDoneGuardForge(t, f)

	mockStore := store_mocks.NewMockStore(t)
	agent := defaultAgent()
	pipeline := defaultPipeline(model.StatusRunning)
	workflow := defaultWorkflow(model.StatusRunning) // NOT terminal -> guard passes
	workflow.Children = []*model.Step{}

	mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
	mockStore.On("StepListFromWorkflowFind", mock.Anything).Return([]*model.Step{}, nil)
	mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
	mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)
	mockStore.On("AgentFind", int64(1)).Return(agent, nil)
	mockStore.On("WorkflowUpdate", mock.Anything).Return(nil)
	mockStore.On("WorkflowGetTree", mock.Anything).Return([]*model.Workflow{}, nil)
	mockStore.On("UpdatePipeline", mock.Anything).Return(nil)
	mockStore.On("GetUser", mock.Anything).Return(&model.User{ID: 1, AccessToken: "x"}, nil)
	mockStore.On("AgentUpdate", mock.Anything).Return(nil)

	mockLogStore := log_mocks.NewMockService(t)
	origLogStore := server.Config.Services.LogStore
	server.Config.Services.LogStore = mockLogStore
	t.Cleanup(func() { server.Config.Services.LogStore = origLogStore })

	mockQueue := queue_mocks.NewMockQueue(t)
	mockQueue.On("Done", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	rpcInst := newTestRPC(t, mockStore, mockQueue)
	ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

	require.NoError(t, rpcInst.Done(ctx, "30", rpc.WorkflowState{Started: 100, Finished: 200}))

	rpcInst.reportWG.Wait()

	calls, _ := f.snapshot()
	assert.Equal(t, 1, calls,
		"the clean Done path must still post exactly one aggregate — the guard-hit POST must not double up")
}
