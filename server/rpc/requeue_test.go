// Copyright 2026 Woodpecker Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build cgo

package rpc

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/rpc"
	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/logging"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pubsub/memory"
	"go.woodpecker-ci.org/woodpecker/v3/server/queue"
	"go.woodpecker-ci.org/woodpecker/v3/server/scheduler"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/datastore"
	"go.woodpecker-ci.org/woodpecker/v3/shared/constant"
)

const testWait = 10 * time.Second

// hookStore runs onReset right before the requeue compare-and-set.
type hookStore struct {
	store.Store
	onReset func(workflow *model.Workflow)
}

func (h *hookStore) WorkflowResetForRequeue(workflow *model.Workflow, steps []*model.Step, agentID int64) (bool, error) {
	if h.onReset != nil {
		h.onReset(workflow)
	}
	return h.Store.WorkflowResetForRequeue(workflow, steps, agentID)
}

type requeueEnv struct {
	t        *testing.T
	store    *hookStore
	sched    scheduler.Scheduler
	rpc      *RPC
	repo     *model.Repo
	pipeline *model.Pipeline
}

// newRequeueEnv wires RPC to a real sqlite store and a persistent queue whose
// leases last lease.
func newRequeueEnv(t *testing.T, lease time.Duration) *requeueEnv {
	t.Helper()
	orig := constant.TaskTimeout
	constant.TaskTimeout = lease
	t.Cleanup(func() { constant.TaskTimeout = orig })

	raw, err := datastore.NewEngine(&store.Opts{
		Driver: "sqlite3",
		Config: ":memory:",
		XORM:   store.XORM{MaxOpenConns: 1, MaxIdleConns: 1},
	})
	require.NoError(t, err)
	require.NoError(t, raw.Migrate(t.Context(), true))
	t.Cleanup(func() { require.NoError(t, raw.Close()) })
	s := &hookStore{Store: raw}

	origLogStore := server.Config.Services.LogStore
	server.Config.Services.LogStore = raw
	t.Cleanup(func() { server.Config.Services.LogStore = origLogStore })

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	q, err := queue.New(ctx, queue.Config{Backend: queue.TypeMemory, Store: s})
	require.NoError(t, err)
	sched := scheduler.NewScheduler(ctx, s, q, memory.New())

	reportWG := &sync.WaitGroup{}
	t.Cleanup(reportWG.Wait)
	r := &RPC{
		store:         s,
		scheduler:     sched,
		logger:        logging.New(),
		pipelineTime:  prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "requeue_test_time"}, []string{"repo", "branch", "status", "pipeline"}),
		pipelineCount: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "requeue_test_count"}, []string{"repo", "branch", "status", "pipeline"}),
		reportWG:      reportWG,
	}

	for _, name := range []string{"agent-1", "agent-2"} {
		require.NoError(t, raw.AgentCreate(&model.Agent{Name: name, OrgID: model.IDNotSet, OwnerID: model.IDNotSet}))
	}
	repo := &model.Repo{Owner: "o", Name: "r", FullName: "o/r", ForgeRemoteID: "1", UserID: 99}
	require.NoError(t, raw.CreateRepo(repo))
	p := &model.Pipeline{RepoID: repo.ID, Status: model.StatusPending, Branch: "main"}
	require.NoError(t, raw.CreatePipeline(p))

	return &requeueEnv{t: t, store: s, sched: sched, rpc: r, repo: repo, pipeline: p}
}

func agentCtx(t *testing.T, agentID int64) context.Context {
	return context.WithValue(t.Context(), agentIDKey, agentID)
}

// addWorkflow stores a workflow with the given steps and pushes its task.
func (e *requeueEnv) addWorkflow(pid int, steps []*model.Step, deps ...string) *model.Workflow {
	e.t.Helper()
	for i, step := range steps {
		step.PipelineID = e.pipeline.ID
		step.PPID = pid
		step.PID = pid*10 + i
		step.UUID = strconv.Itoa(step.PID)
		step.Name = "step-" + step.UUID
		step.State = model.StatusPending
		step.Failure = model.FailureFail
	}
	wf := &model.Workflow{PipelineID: e.pipeline.ID, PID: pid, Name: "wf-" + strconv.Itoa(pid), State: model.StatusPending, Children: steps}
	require.NoError(e.t, e.store.WorkflowsCreate([]*model.Workflow{wf}))
	task := &model.Task{ID: strconv.FormatInt(wf.ID, 10), Labels: map[string]string{}, Dependencies: deps, DepStatus: map[string]model.StatusValue{}, PipelineID: e.pipeline.ID, RepoID: e.repo.ID, Created: 1}
	require.NoError(e.t, task.ApplyLabelsFromRepo(e.repo))
	data, err := json.Marshal(rpc.Workflow{ID: task.ID})
	require.NoError(e.t, err)
	task.Data = data
	require.NoError(e.t, e.sched.StartPipeline(e.t.Context(), e.repo, e.pipeline, []*model.Task{task}))
	return wf
}

// poll lets agentID take the next workflow, like an agent's Next call.
func (e *requeueEnv) poll(agentID int64) string {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(agentCtx(e.t, agentID), testWait)
	defer cancel()
	wf, err := e.rpc.Next(ctx, rpc.Filter{Labels: map[string]string{"repo": "*"}})
	require.NoError(e.t, err)
	require.NotNil(e.t, wf)
	return wf.ID
}

// start dispatches wf to agentID, initializes it and sets the step states.
func (e *requeueEnv) start(wf *model.Workflow, agentID int64, attempts int, states ...model.StatusValue) {
	e.t.Helper()
	id := strconv.FormatInt(wf.ID, 10)
	require.Equal(e.t, id, e.poll(agentID))
	require.NoError(e.t, e.rpc.Init(agentCtx(e.t, agentID), id, rpc.WorkflowState{Started: 100}))
	if attempts > 0 {
		row := e.workflow(wf.ID)
		row.Attempts = attempts
		require.NoError(e.t, e.store.WorkflowUpdate(row))
	}
	for i, state := range states {
		step := wf.Children[i]
		step.State, step.Started = state, 100
		if state != model.StatusRunning {
			step.Finished = 110
		}
		require.NoError(e.t, e.store.StepUpdate(step))
	}
}

func (e *requeueEnv) workflow(id int64) *model.Workflow {
	e.t.Helper()
	wf, err := e.store.WorkflowLoad(id)
	require.NoError(e.t, err)
	wf.Children, err = e.store.StepListFromWorkflowFind(wf)
	require.NoError(e.t, err)
	return wf
}

func (e *requeueEnv) shutdownDone(wf *model.Workflow, agentID int64) error {
	return e.rpc.Done(agentCtx(e.t, agentID), strconv.FormatInt(wf.ID, 10),
		rpc.WorkflowState{Started: 100, Finished: 200, Canceled: true, AgentShutdown: true})
}

func (e *requeueEnv) running(id int64) bool {
	return slices.ContainsFunc(e.sched.Info(e.t.Context()).Running, func(task *model.Task) bool {
		return task.ID == strconv.FormatInt(id, 10)
	})
}

func (e *requeueEnv) queued(id int64) bool {
	info := e.sched.Info(e.t.Context())
	return slices.ContainsFunc(slices.Concat(info.Pending, info.WaitingOnDeps, info.Running), func(task *model.Task) bool {
		return task.ID == strconv.FormatInt(id, 10)
	})
}

func (e *requeueEnv) nextExpired() queue.ExpiredTask {
	e.t.Helper()
	select {
	case ev := <-e.sched.Expired():
		return ev
	case <-time.After(testWait):
		e.t.Fatal("no lease-expiry event")
		return queue.ExpiredTask{}
	}
}

func steps(n int) []*model.Step {
	out := make([]*model.Step, n)
	for i := range out {
		out[i] = &model.Step{Type: model.StepTypeCommands}
	}
	return out
}

func stepStates(wf *model.Workflow) []model.StatusValue {
	out := make([]model.StatusValue, len(wf.Children))
	for i, step := range wf.Children {
		out[i] = step.State
	}
	return out
}

func TestDoneAgentShutdownRequeue(t *testing.T) {
	t.Run("running step requeues and resets", func(t *testing.T) {
		e := newRequeueEnv(t, time.Hour)
		wf := e.addWorkflow(1, steps(2))
		e.start(wf, 1, 0, model.StatusSuccess, model.StatusRunning)

		require.NoError(t, e.shutdownDone(wf, 1))

		row := e.workflow(wf.ID)
		assert.Equal(t, model.StatusPending, row.State)
		assert.Zero(t, row.AgentID)
		assert.Equal(t, 1, row.Attempts)
		assert.Equal(t, []model.StatusValue{model.StatusPending, model.StatusPending}, stepStates(row))
		for _, step := range row.Children {
			assert.Zero(t, step.Started)
			assert.Zero(t, step.Finished)
		}

		id := strconv.FormatInt(wf.ID, 10)
		require.Equal(t, id, e.poll(2))
		require.NoError(t, e.rpc.Init(agentCtx(t, 2), id, rpc.WorkflowState{Started: 300}))
		require.NoError(t, e.rpc.Update(agentCtx(t, 2), id, rpc.StepState{StepUUID: row.Children[0].UUID, Started: 300}))
		assert.Equal(t, model.StatusRunning, e.workflow(wf.ID).Children[0].State)

		err := e.rpc.Update(agentCtx(t, 1), id, rpc.StepState{StepUUID: row.Children[1].UUID, Started: 100, Finished: 200, Exited: true})
		require.ErrorIs(t, err, ErrAgentIllegalWorkflowAgentID)
		assert.Equal(t, model.StatusPending, e.workflow(wf.ID).Children[1].State)
	})

	t.Run("at the cap it ends killed", func(t *testing.T) {
		e := newRequeueEnv(t, time.Hour)
		wf := e.addWorkflow(1, steps(2))
		e.start(wf, 1, maxAgentLossRequeues, model.StatusSuccess, model.StatusRunning)

		require.NoError(t, e.shutdownDone(wf, 1))

		row := e.workflow(wf.ID)
		assert.Equal(t, model.StatusKilled, row.State)
		assert.Equal(t, maxAgentLossRequeues, row.Attempts)
		assert.False(t, e.queued(wf.ID))
	})

	noRequeue := []struct {
		name     string
		state    rpc.WorkflowState
		pipeline model.StatusValue
		detached bool
		steps    []model.StatusValue
		want     model.StatusValue
	}{
		{
			name:  "plain cancel",
			state: rpc.WorkflowState{Started: 100, Finished: 200, Canceled: true},
			steps: []model.StatusValue{model.StatusSuccess, model.StatusRunning},
			want:  model.StatusKilled,
		},
		{
			name:     "canceled pipeline",
			state:    rpc.WorkflowState{Started: 100, Finished: 200, Canceled: true, AgentShutdown: true},
			pipeline: model.StatusKilled,
			steps:    []model.StatusValue{model.StatusSuccess, model.StatusRunning},
			want:     model.StatusKilled,
		},
		{
			name:  "failed step with a running on_failure step",
			state: rpc.WorkflowState{Started: 100, Finished: 200, Canceled: true, AgentShutdown: true},
			steps: []model.StatusValue{model.StatusFailure, model.StatusRunning},
			want:  model.StatusKilled,
		},
		{
			name:  "all steps finished green",
			state: rpc.WorkflowState{Started: 100, Finished: 200, Canceled: true, AgentShutdown: true},
			steps: []model.StatusValue{model.StatusSuccess, model.StatusSuccess},
			want:  model.StatusSuccess,
		},
		{
			name:     "detached step still running",
			state:    rpc.WorkflowState{Started: 100, Finished: 200, Canceled: true, AgentShutdown: true},
			detached: true,
			steps:    []model.StatusValue{model.StatusSuccess, model.StatusRunning},
			want:     model.StatusSuccess,
		},
	}
	for _, tc := range noRequeue {
		t.Run("no requeue: "+tc.name, func(t *testing.T) {
			e := newRequeueEnv(t, time.Hour)
			s := steps(2)
			s[1].Detached = tc.detached
			wf := e.addWorkflow(1, s)
			e.start(wf, 1, 0, tc.steps...)
			if tc.pipeline != "" {
				p, err := e.store.GetPipeline(e.pipeline.ID)
				require.NoError(t, err)
				p.Status = tc.pipeline
				require.NoError(t, e.store.UpdatePipeline(p))
			}

			require.NoError(t, e.rpc.Done(agentCtx(t, 1), strconv.FormatInt(wf.ID, 10), tc.state))

			row := e.workflow(wf.ID)
			assert.Equal(t, tc.want, row.State)
			assert.Zero(t, row.Attempts)
			assert.Equal(t, tc.steps[0], row.Children[0].State)
			assert.False(t, e.queued(wf.ID))
		})
	}

	t.Run("reserve miss ends killed with step rows untouched", func(t *testing.T) {
		e := newRequeueEnv(t, time.Hour)
		wf := e.addWorkflow(1, steps(2))
		e.start(wf, 1, 0, model.StatusSuccess, model.StatusRunning)
		require.NoError(t, e.sched.CancelWorkflows(t.Context(), []string{strconv.FormatInt(wf.ID, 10)}))

		require.NoError(t, e.shutdownDone(wf, 1))

		row := e.workflow(wf.ID)
		assert.Equal(t, model.StatusKilled, row.State)
		assert.Zero(t, row.Attempts)
		assert.Equal(t, model.StatusSuccess, row.Children[0].State)
		assert.Equal(t, int64(110), row.Children[0].Finished)
		assert.NotEqual(t, model.StatusPending, row.Children[1].State)
	})

	t.Run("cancel between reserve and requeue writes back the first attempt", func(t *testing.T) {
		e := newRequeueEnv(t, time.Hour)
		wf := e.addWorkflow(1, steps(2))
		e.start(wf, 1, 0, model.StatusSuccess, model.StatusRunning)
		e.store.onReset = func(w *model.Workflow) {
			require.NoError(t, e.sched.CancelWorkflows(t.Context(), []string{strconv.FormatInt(w.ID, 10)}))
		}

		require.NoError(t, e.shutdownDone(wf, 1))

		row := e.workflow(wf.ID)
		assert.Equal(t, model.StatusKilled, row.State)
		assert.Equal(t, model.StatusSuccess, row.Children[0].State)
		assert.Equal(t, int64(100), row.Children[0].Started)
		assert.Equal(t, int64(110), row.Children[0].Finished)
		assert.NotEqual(t, model.StatusPending, row.Children[1].State)
		assert.False(t, e.queued(wf.ID))
	})
}

func TestHandleExpired(t *testing.T) {
	const lease = 300 * time.Millisecond

	t.Run("running step is reset and requeued", func(t *testing.T) {
		e := newRequeueEnv(t, lease)
		wf := e.addWorkflow(1, steps(2))
		e.start(wf, 1, 0, model.StatusSuccess, model.StatusRunning)
		ev := e.nextExpired()
		require.Equal(t, int64(1), ev.AgentID)

		require.NoError(t, e.rpc.HandleExpired(t.Context(), ev))

		row := e.workflow(wf.ID)
		assert.Equal(t, model.StatusPending, row.State)
		assert.Equal(t, 1, row.Attempts)
		assert.Equal(t, []model.StatusValue{model.StatusPending, model.StatusPending}, stepStates(row))
		tasks, err := e.store.TaskList()
		require.NoError(t, err)
		assert.True(t, slices.ContainsFunc(tasks, func(task *model.Task) bool { return task.ID == ev.ID }), "backup row")

		err = e.rpc.Done(agentCtx(t, 1), ev.ID, rpc.WorkflowState{Started: 100, Finished: 200})
		require.ErrorIs(t, err, ErrAgentIllegalWorkflowAgentID)
		assert.Equal(t, ev.ID, e.poll(2))
	})

	for _, agentID := range []int64{0, 1} {
		t.Run("pending workflow is pollable again, agent "+strconv.FormatInt(agentID, 10), func(t *testing.T) {
			e := newRequeueEnv(t, lease)
			wf := e.addWorkflow(1, steps(1))
			dep := e.addWorkflow(2, steps(1), strconv.FormatInt(wf.ID, 10))
			require.Equal(t, strconv.FormatInt(wf.ID, 10), e.poll(1))
			if agentID == 0 {
				row := e.workflow(wf.ID)
				row.AgentID = 0
				require.NoError(t, e.store.WorkflowUpdate(row))
			}
			ev := e.nextExpired()

			require.NoError(t, e.rpc.HandleExpired(t.Context(), ev))

			row := e.workflow(wf.ID)
			assert.Equal(t, model.StatusPending, row.State)
			assert.Zero(t, row.AgentID)
			assert.Zero(t, row.Attempts)
			info := e.sched.Info(t.Context())
			for _, task := range slices.Concat(info.Pending, info.WaitingOnDeps) {
				if task.ID == strconv.FormatInt(dep.ID, 10) {
					assert.Empty(t, task.DepStatus, "dependent saw the discarded attempt")
				}
			}
			require.ErrorIs(t, e.rpc.Init(agentCtx(t, 1), ev.ID, rpc.WorkflowState{Started: 100}), ErrAgentIllegalWorkflowAgentID)
			assert.Equal(t, ev.ID, e.poll(2))
		})
	}

	t.Run("at the cap it ends killed and dependents do not run", func(t *testing.T) {
		e := newRequeueEnv(t, lease)
		wf := e.addWorkflow(1, steps(2))
		dep := e.addWorkflow(2, steps(1), strconv.FormatInt(wf.ID, 10))
		e.start(wf, 1, maxAgentLossRequeues, model.StatusSuccess, model.StatusRunning)

		require.NoError(t, e.rpc.HandleExpired(t.Context(), e.nextExpired()))

		assert.Equal(t, model.StatusKilled, e.workflow(wf.ID).State)
		assert.False(t, e.queued(wf.ID))
		info := e.sched.Info(t.Context())
		found := false
		for _, task := range slices.Concat(info.Pending, info.WaitingOnDeps) {
			if task.ID == strconv.FormatInt(dep.ID, 10) {
				found = true
				assert.False(t, task.ShouldRun())
			}
		}
		assert.True(t, found)
	})

	t.Run("at the cap the pipeline leaves running", func(t *testing.T) {
		e := newRequeueEnv(t, lease)
		wf := e.addWorkflow(1, steps(2))
		e.start(wf, 1, maxAgentLossRequeues, model.StatusSuccess, model.StatusRunning)

		require.NoError(t, e.rpc.HandleExpired(t.Context(), e.nextExpired()))

		p, err := e.store.GetPipeline(e.pipeline.ID)
		require.NoError(t, err)
		assert.Equal(t, model.StatusKilled, p.Status)
	})

	t.Run("failed step ends killed with the failure kept", func(t *testing.T) {
		e := newRequeueEnv(t, lease)
		wf := e.addWorkflow(1, steps(2))
		e.start(wf, 1, 0, model.StatusFailure, model.StatusRunning)

		require.NoError(t, e.rpc.HandleExpired(t.Context(), e.nextExpired()))

		row := e.workflow(wf.ID)
		assert.Equal(t, model.StatusKilled, row.State)
		assert.Equal(t, model.StatusFailure, row.Children[0].State)
		assert.Zero(t, row.Attempts)
		assert.False(t, e.queued(wf.ID))
	})

	for _, next := range []int64{1, 2} {
		t.Run("event after re-dispatch to agent "+strconv.FormatInt(next, 10)+" does nothing", func(t *testing.T) {
			e := newRequeueEnv(t, lease)
			wf := e.addWorkflow(1, steps(2))
			e.start(wf, 1, 0, model.StatusSuccess, model.StatusRunning)
			stale := e.nextExpired()
			require.NoError(t, e.rpc.HandleExpired(t.Context(), stale))
			e.start(wf, next, 0)
			before := e.workflow(wf.ID)

			require.NoError(t, e.rpc.HandleExpired(t.Context(), stale))

			after := e.workflow(wf.ID)
			assert.Equal(t, model.StatusRunning, after.State)
			assert.Equal(t, next, after.AgentID)
			assert.Equal(t, before.Attempts, after.Attempts)
			assert.True(t, e.running(wf.ID))
		})
	}

	t.Run("compare-and-set miss after the old agent's Done leaves no entry", func(t *testing.T) {
		e := newRequeueEnv(t, lease)
		wf := e.addWorkflow(1, steps(2))
		e.start(wf, 1, 0, model.StatusSuccess, model.StatusRunning)
		e.store.onReset = func(w *model.Workflow) {
			row := e.workflow(w.ID)
			row.State = model.StatusSuccess
			require.NoError(t, e.store.WorkflowUpdate(row))
		}

		require.NoError(t, e.rpc.HandleExpired(t.Context(), e.nextExpired()))

		assert.Equal(t, model.StatusSuccess, e.workflow(wf.ID).State)
		assert.False(t, e.queued(wf.ID))
	})
}
