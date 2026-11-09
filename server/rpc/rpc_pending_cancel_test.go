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

package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/mattn/go-sqlite3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/rpc"
	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/api"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
	"go.woodpecker-ci.org/woodpecker/v3/server/logging"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
	"go.woodpecker-ci.org/woodpecker/v3/server/pubsub/memory"
	"go.woodpecker-ci.org/woodpecker/v3/server/queue"
	"go.woodpecker-ci.org/woodpecker/v3/server/scheduler"
	manager_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/datastore"
)

type pendingCancelForge struct {
	forge.Forge
	mu       sync.Mutex
	verdicts []model.StatusValue
}

func (f *pendingCancelForge) StatusAggregate(_ context.Context, _ *model.User, _ *model.Repo, p *model.Pipeline, _ []*model.Workflow) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.verdicts = append(f.verdicts, p.Status)
	return nil
}

type blockingAggregateForge struct {
	forge.Forge
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *blockingAggregateForge) StatusAggregate(ctx context.Context, _ *model.User, _ *model.Repo, _ *model.Pipeline, _ []*model.Workflow) error {
	f.once.Do(func() { close(f.entered) })
	select {
	case <-f.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestPendingCancelRacesLastWorkflowCompletion(t *testing.T) {
	for _, cancelFirst := range []bool{true, false} {
		name := "completion-first"
		if cancelFirst {
			name = "cancel-first"
		}
		t.Run(name, func(t *testing.T) {
			s := datastore.NewTestStore(t)
			user := &model.User{Login: "owner", ForgeRemoteID: "owner"}
			require.NoError(t, s.CreateUser(user))
			repo := &model.Repo{UserID: user.ID, ForgeRemoteID: "repo", Owner: "owner", Name: "repo", FullName: "owner/repo"}
			require.NoError(t, s.CreateRepo(repo))
			p := &model.Pipeline{RepoID: repo.ID, Number: 1, Status: model.StatusRunning, Event: model.EventPush}
			successor := &model.Pipeline{RepoID: repo.ID, Number: 2, Status: model.StatusPending}
			require.NoError(t, s.CreatePipeline(p))
			require.NoError(t, s.CreatePipeline(successor))
			workflows := []*model.Workflow{
				{PipelineID: p.ID, PID: 1, Name: "a", State: model.StatusPending},
				{PipelineID: p.ID, PID: 2, Name: "b", State: model.StatusPending, DependsOn: []string{"a"}},
				{PipelineID: p.ID, PID: 3, Name: "running", State: model.StatusRunning, Started: 1, AgentID: 1},
			}
			require.NoError(t, s.WorkflowsCreate(workflows))
			q := queue.WithTaskStore(t.Context(), queue.NewMemoryQueue(t.Context()), s)
			ids := []string{strconv.FormatInt(workflows[0].ID, 10), strconv.FormatInt(workflows[1].ID, 10)}
			require.NoError(t, q.PushAtOnce(t.Context(), []*model.Task{{ID: ids[0]}, {ID: ids[1], Dependencies: []string{ids[0]}}}))
			sched := scheduler.NewScheduler(t.Context(), s, q, memory.New())
			f := &pendingCancelForge{}
			manager := manager_mocks.NewMockManager(t)
			manager.On("ForgeFromRepo", mock.Anything).Return(f, nil).Maybe()
			oldManager, oldConfig := server.Config.Services.Manager, server.Config.Server
			server.Config.Services.Manager = manager
			oldScheduler := server.Config.Services.Scheduler
			server.Config.Services.Scheduler = sched
			t.Cleanup(func() { server.Config.Services.Scheduler = oldScheduler })
			server.Config.Server.StatusPerWorkflow = false
			server.Config.Server.StatusAggregate = true
			server.Config.Server.StatusMeta = false
			t.Cleanup(func() { server.Config.Services.Manager, server.Config.Server = oldManager, oldConfig })
			var reports sync.WaitGroup
			instance := RPC{
				store: s, scheduler: sched, logger: logging.New(), reportWG: &reports,
				pipelineCount: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_pending_cancel_count"}, []string{"repo", "branch", "status", "pipeline"}),
				pipelineTime:  prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "test_pending_cancel_time"}, []string{"repo", "branch", "status", "pipeline"}),
			}
			held := pipeline.LockPipeline(p.ID)
			ready := make(chan struct{}, 2)
			done := make(chan error, 2)
			startCancel, startFinish := make(chan struct{}), make(chan struct{})
			go func() {
				ready <- struct{}{}
				<-startCancel
				recorder := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(recorder)
				c.Set("store", s)
				c.Set("repo", repo)
				c.Set("perm", &model.Perm{})
				c.Set("pipeline", p)
				body, err := json.Marshal(api.CancelPendingWorkflowsRequest{WorkflowIDs: []int64{workflows[0].ID, workflows[1].ID}, SupersededBy: 2})
				if err != nil {
					done <- err
					return
				}
				c.Request = httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
				api.CancelPendingWorkflows(c)
				if recorder.Code != http.StatusOK {
					done <- fmt.Errorf("cancel HTTP status %d: %s", recorder.Code, recorder.Body.String())
					return
				}
				done <- nil
			}()
			go func() {
				ready <- struct{}{}
				<-startFinish
				done <- instance.finishWorkflow(t.Context(), workflows[2], p, repo, rpc.WorkflowState{Started: 1, Finished: 2})
			}()
			<-ready
			<-ready
			if cancelFirst {
				close(startCancel)
			} else {
				close(startFinish)
			}
			held()
			require.NoError(t, <-done)
			if !cancelFirst {
				reports.Wait()
			}
			if cancelFirst {
				close(startFinish)
			} else {
				close(startCancel)
			}
			require.NoError(t, <-done)
			reports.Wait()
			fresh, err := s.GetPipeline(p.ID)
			require.NoError(t, err)
			require.Equal(t, model.StatusSuccess, fresh.Status)
			tree, err := s.WorkflowGetTree(fresh)
			require.NoError(t, err)
			require.Equal(t, model.StatusSuccess, pipeline.PipelineStatus(tree))
			require.Equal(t, model.StatusSkipped, tree[0].State)
			require.Equal(t, model.StatusSkipped, tree[1].State)
			require.Equal(t, model.StatusSuccess, tree[2].State)
			f.mu.Lock()
			require.NotEmpty(t, f.verdicts)
			require.Equal(t, model.StatusSuccess, f.verdicts[len(f.verdicts)-1])
			f.mu.Unlock()
		})
	}
}

func TestDelayedCompletionReportUsesCommittedCancellation(t *testing.T) {
	s := datastore.NewTestStore(t)
	user := &model.User{Login: "owner", ForgeRemoteID: "owner"}
	require.NoError(t, s.CreateUser(user))
	repo := &model.Repo{UserID: user.ID, ForgeRemoteID: "repo", Owner: "owner", Name: "repo", FullName: "owner/repo"}
	require.NoError(t, s.CreateRepo(repo))
	p := &model.Pipeline{RepoID: repo.ID, Number: 1, Status: model.StatusRunning, Event: model.EventPush}
	require.NoError(t, s.CreatePipeline(p))
	workflows := []*model.Workflow{{PipelineID: p.ID, PID: 1, Name: "pending", State: model.StatusPending}}
	require.NoError(t, s.WorkflowsCreate(workflows))
	f := &pendingCancelForge{}
	manager := manager_mocks.NewMockManager(t)
	manager.On("ForgeFromRepo", mock.Anything).Return(f, nil).Maybe()
	oldManager, oldConfig := server.Config.Services.Manager, server.Config.Server
	server.Config.Services.Manager = manager
	server.Config.Server.StatusPerWorkflow = false
	server.Config.Server.StatusAggregate = true
	server.Config.Server.StatusMeta = false
	t.Cleanup(func() { server.Config.Services.Manager, server.Config.Server = oldManager, oldConfig })
	var reports sync.WaitGroup
	instance := RPC{store: s, reportWG: &reports}
	unlock := pipeline.LockReport(p.ID)
	instance.reportForgeStatusAsync(t.Context(), repo, p, nil)
	require.NoError(t, s.WorkflowSupersedePending(p.ID, []int64{workflows[0].ID}, 2))
	p.Status = model.StatusSuccess
	require.NoError(t, s.UpdatePipeline(p))
	instance.updateForgeStatus(t.Context(), repo, p, nil)
	unlock()
	reports.Wait()
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Equal(t, []model.StatusValue{model.StatusSuccess, model.StatusSuccess}, f.verdicts)
}

func TestBlockedForgeReportDoesNotBlockWorkflowCompletion(t *testing.T) {
	s := datastore.NewTestStore(t)
	user := &model.User{Login: "owner", ForgeRemoteID: "owner"}
	require.NoError(t, s.CreateUser(user))
	repo := &model.Repo{UserID: user.ID, ForgeRemoteID: "repo", Owner: "owner", Name: "repo", FullName: "owner/repo"}
	require.NoError(t, s.CreateRepo(repo))
	agent := &model.Agent{Name: "agent", OrgID: model.IDNotSet, OwnerID: model.IDNotSet}
	require.NoError(t, s.AgentCreate(agent))
	p := &model.Pipeline{RepoID: repo.ID, Number: 1, Status: model.StatusRunning, Event: model.EventPush}
	require.NoError(t, s.CreatePipeline(p))
	workflows := []*model.Workflow{{PipelineID: p.ID, PID: 1, Name: "running", State: model.StatusRunning, Started: 1, AgentID: agent.ID}}
	require.NoError(t, s.WorkflowsCreate(workflows))
	q := queue.NewMemoryQueue(t.Context())
	sched := scheduler.NewScheduler(t.Context(), s, q, memory.New())
	f := &blockingAggregateForge{entered: make(chan struct{}), release: make(chan struct{})}
	manager := manager_mocks.NewMockManager(t)
	manager.On("ForgeFromRepo", mock.Anything).Return(f, nil).Maybe()
	oldManager, oldConfig := server.Config.Services.Manager, server.Config.Server
	server.Config.Services.Manager = manager
	server.Config.Server.StatusPerWorkflow = false
	server.Config.Server.StatusAggregate = true
	server.Config.Server.StatusMeta = false
	t.Cleanup(func() { server.Config.Services.Manager, server.Config.Server = oldManager, oldConfig })
	var reports sync.WaitGroup
	instance := RPC{
		store: s, scheduler: sched, logger: logging.New(), reportWG: &reports,
		pipelineCount: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_blocked_report_count"}, []string{"repo", "branch", "status", "pipeline"}),
		pipelineTime:  prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "test_blocked_report_time"}, []string{"repo", "branch", "status", "pipeline"}),
	}
	instance.reportForgeStatusAsync(t.Context(), repo, p, nil)
	<-f.entered
	done := make(chan error, 1)
	go func() {
		done <- instance.Done(context.WithValue(t.Context(), agentIDKey, agent.ID), strconv.FormatInt(workflows[0].ID, 10), rpc.WorkflowState{Started: 1, Finished: 2})
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		close(f.release)
		<-done
		reports.Wait()
		t.Fatal("workflow completion blocked on forge report")
	}
	close(f.release)
	reports.Wait()
}

func TestNextDoesNotReturnCanceledWorkflows(t *testing.T) {
	s := datastore.NewTestStore(t)
	agent := &model.Agent{Name: "agent", OrgID: model.IDNotSet, OwnerID: model.IDNotSet}
	require.NoError(t, s.AgentCreate(agent))
	workflows := []*model.Workflow{
		{PipelineID: 1, PID: 1, Name: "a", State: model.StatusPending},
		{PipelineID: 1, PID: 2, Name: "b", State: model.StatusPending, DependsOn: []string{"a"}},
	}
	require.NoError(t, s.WorkflowsCreate(workflows))
	q := queue.WithTaskStore(t.Context(), queue.NewMemoryQueue(t.Context()), s)
	ids := []string{strconv.FormatInt(workflows[0].ID, 10), strconv.FormatInt(workflows[1].ID, 10)}
	require.NoError(t, q.PushAtOnce(t.Context(), []*model.Task{{ID: ids[0]}, {ID: ids[1], Dependencies: []string{ids[0]}}}))
	require.NoError(t, q.CancelPending(t.Context(), ids, func() error {
		return s.WorkflowSupersedePending(1, []int64{workflows[0].ID, workflows[1].ID}, 2)
	}))
	instance := RPC{store: s, scheduler: scheduler.NewScheduler(t.Context(), s, q, memory.New())}
	ctx, cancel := context.WithTimeout(context.WithValue(t.Context(), agentIDKey, agent.ID), 250*time.Millisecond)
	defer cancel()
	workflow, err := instance.Next(ctx, rpc.Filter{Labels: map[string]string{}})
	require.Nil(t, workflow)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
