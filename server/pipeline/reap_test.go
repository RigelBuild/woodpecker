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
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/queue"
	scheduler_mocks "go.woodpecker-ci.org/woodpecker/v3/server/scheduler/mocks"
	manager_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/mocks"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
	store_types "go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

func TestReapOrphanedWorkflows(t *testing.T) {
	const nowUnix = int64(1_700_000_000)
	now := time.Unix(nowUnix, 0)
	grace := 10 * time.Minute

	tests := []struct {
		name              string
		pipelineStatus    model.StatusValue
		pipelineCreated   int64
		workflows         []*model.Workflow
		repoTimeout       int64
		queueTasks        []string
		queueState        string
		agents            map[int64]*model.Agent
		missingAgents     map[int64]bool
		workflowStates    map[int64]model.StatusValue
		stepStates        map[int64]model.StatusValue
		pipelineFinal     model.StatusValue
		pipelineFinalized bool
	}{
		{
			name:            "deleted agent reaps old running workflow and its steps",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 3600,
			workflows: []*model.Workflow{{
				ID: 41, State: model.StatusRunning, Started: nowUnix - 3600, AgentID: 7,
				Children: []*model.Step{
					{ID: 411, State: model.StatusRunning, Started: nowUnix - 3500},
					{ID: 412, State: model.StatusPending},
				},
			}},
			missingAgents:     map[int64]bool{7: true},
			workflowStates:    map[int64]model.StatusValue{41: model.StatusKilled},
			stepStates:        map[int64]model.StatusValue{411: model.StatusKilled, 412: model.StatusCanceled},
			pipelineFinal:     model.StatusKilled,
			pipelineFinalized: true,
		},
		{
			name:            "pending queue task remains untouched",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 3600,
			workflows: []*model.Workflow{{
				ID: 42, State: model.StatusRunning, Started: nowUnix - 3600, AgentID: 7,
			}},
			queueTasks: []string{fmt.Sprint(42)},
		},
		{
			name:            "waiting-on-dependencies queue task remains untouched",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 3600,
			workflows: []*model.Workflow{{
				ID: 49, State: model.StatusRunning, Started: nowUnix - 3600, AgentID: 7,
			}},
			queueTasks: []string{fmt.Sprint(49)},
			queueState: "waiting",
		},
		{
			name:            "running queue task remains untouched",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 3600,
			workflows: []*model.Workflow{{
				ID: 50, State: model.StatusRunning, Started: nowUnix - 3600, AgentID: 7,
			}},
			queueTasks: []string{fmt.Sprint(50)},
			queueState: "running",
		},
		{
			name:            "recent agent within timeout remains untouched",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 1800,
			workflows: []*model.Workflow{{
				ID: 43, State: model.StatusRunning, Started: nowUnix - 1800, AgentID: 8,
			}},
			repoTimeout: 60,
			agents: map[int64]*model.Agent{
				8: {ID: 8, LastContact: nowUnix - 30},
			},
		},
		{
			name:            "old agent contact reaps a workflow",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 3600,
			workflows: []*model.Workflow{{
				ID: 54, State: model.StatusRunning, Started: nowUnix - 1800, AgentID: 15,
			}},
			repoTimeout: 60,
			agents: map[int64]*model.Agent{
				15: {ID: 15, LastContact: nowUnix - 900},
			},
			workflowStates:    map[int64]model.StatusValue{54: model.StatusKilled},
			pipelineFinal:     model.StatusKilled,
			pipelineFinalized: true,
		},
		{
			name:            "recently created pending workflow stays within grace",
			pipelineStatus:  model.StatusPending,
			pipelineCreated: nowUnix - 300,
			workflows: []*model.Workflow{{
				ID: 55, State: model.StatusPending,
			}},
		},
		{
			name:            "pipeline pending age protects a recently started workflow",
			pipelineStatus:  model.StatusPending,
			pipelineCreated: nowUnix - 3600,
			workflows: []*model.Workflow{{
				ID: 53, State: model.StatusRunning, Started: nowUnix - 300, AgentID: 14,
			}},
			missingAgents: map[int64]bool{14: true},
		},
		{
			name:            "agent contact exactly at grace cutoff is not old",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 3600,
			workflows: []*model.Workflow{{
				ID: 56, State: model.StatusRunning, Started: nowUnix - 1800, AgentID: 16,
			}},
			repoTimeout: 60,
			agents: map[int64]*model.Agent{
				16: {ID: 16, LastContact: nowUnix - int64(grace.Seconds())},
			},
		},
		{
			name:            "workflow exactly at age grace cutoff is not old",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 3600,
			workflows: []*model.Workflow{{
				ID: 57, State: model.StatusRunning, Started: nowUnix - int64(grace.Seconds()), AgentID: 17,
			}},
			missingAgents: map[int64]bool{17: true},
		},
		{
			name:            "default timeout remains active until timeout and grace pass",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 3600,
			workflows: []*model.Workflow{{
				ID: 52, State: model.StatusRunning, Started: nowUnix - int64((60*time.Minute + 9*time.Minute).Seconds()), AgentID: 13,
			}},
			agents: map[int64]*model.Agent{
				13: {ID: 13, LastContact: nowUnix - 30},
			},
		},
		{
			name:            "timeout reaps workflow with live agent",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 1800,
			workflows: []*model.Workflow{{
				ID: 44, State: model.StatusRunning, Started: nowUnix - 1500, AgentID: 9,
			}},
			repoTimeout: 10,
			agents: map[int64]*model.Agent{
				9: {ID: 9, LastContact: nowUnix - 30},
			},
			workflowStates:    map[int64]model.StatusValue{44: model.StatusKilled},
			pipelineFinal:     model.StatusKilled,
			pipelineFinalized: true,
		},
		{
			name:            "orphan younger than grace remains untouched",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 300,
			workflows: []*model.Workflow{{
				ID: 45, State: model.StatusRunning, Started: nowUnix - 300, AgentID: 10,
			}},
			missingAgents: map[int64]bool{10: true},
		},
		{
			name:            "old pending workflow without agent is canceled",
			pipelineStatus:  model.StatusPending,
			pipelineCreated: nowUnix - 3600,
			workflows: []*model.Workflow{{
				ID: 46, State: model.StatusPending,
				Children: []*model.Step{{ID: 461, State: model.StatusPending}},
			}},
			workflowStates:    map[int64]model.StatusValue{46: model.StatusCanceled},
			stepStates:        map[int64]model.StatusValue{461: model.StatusCanceled},
			pipelineFinal:     model.StatusKilled,
			pipelineFinalized: true,
		},
		{
			name:            "live workflow keeps pipeline running after another is reaped",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 3600,
			workflows: []*model.Workflow{
				{ID: 47, State: model.StatusRunning, Started: nowUnix - 3600, AgentID: 11},
				{ID: 48, State: model.StatusRunning, Started: nowUnix - 300, AgentID: 12},
			},
			agents: map[int64]*model.Agent{
				12: {ID: 12, LastContact: nowUnix - 30},
			},
			missingAgents:  map[int64]bool{11: true},
			workflowStates: map[int64]model.StatusValue{47: model.StatusKilled},
		},
		{
			name:              "active pipeline with only terminal workflows is finalized",
			pipelineStatus:    model.StatusRunning,
			pipelineCreated:   nowUnix - 3600,
			workflows:         []*model.Workflow{{ID: 51, State: model.StatusFailure}},
			pipelineFinal:     model.StatusFailure,
			pipelineFinalized: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const pipelineID = int64(100)
			repo := &model.Repo{ID: 200, UserID: 300, FullName: "owner/repo", Timeout: tt.repoTimeout}
			pipeline := &model.Pipeline{
				ID: pipelineID, RepoID: repo.ID, Number: 13,
				Status: tt.pipelineStatus, Created: tt.pipelineCreated,
			}

			oldScheduler := server.Config.Services.Scheduler
			oldManager := server.Config.Services.Manager
			oldPerWorkflow := server.Config.Server.StatusPerWorkflow
			oldAggregate := server.Config.Server.StatusAggregate
			oldMeta := server.Config.Server.StatusMeta
			t.Cleanup(func() {
				server.Config.Services.Scheduler = oldScheduler
				server.Config.Services.Manager = oldManager
				server.Config.Server.StatusPerWorkflow = oldPerWorkflow
				server.Config.Server.StatusAggregate = oldAggregate
				server.Config.Server.StatusMeta = oldMeta
			})
			server.Config.Server.StatusPerWorkflow = false
			server.Config.Server.StatusAggregate = false
			server.Config.Server.StatusMeta = false

			schedulerMock := scheduler_mocks.NewMockScheduler(t)
			server.Config.Services.Scheduler = schedulerMock
			tasks := make([]*model.Task, 0, len(tt.queueTasks))
			for _, id := range tt.queueTasks {
				tasks = append(tasks, &model.Task{ID: id})
			}
			queueInfo := queue.InfoT{}
			switch tt.queueState {
			case "waiting":
				queueInfo.WaitingOnDeps = tasks
			case "running":
				queueInfo.Running = tasks
			default:
				queueInfo.Pending = tasks
			}
			schedulerMock.On("Info", mock.Anything).Return(queueInfo).Once()

			managerMock := manager_mocks.NewMockManager(t)
			server.Config.Services.Manager = managerMock
			storeMock := store_mocks.NewMockStore(t)
			storeMock.On("GetPipelineQueue").Return([]*model.Feed{{ID: pipeline.ID}}, nil).Once()
			storeMock.On("GetPipeline", pipeline.ID).Return(pipeline, nil).Once()
			storeMock.On("GetRepo", repo.ID).Return(repo, nil).Once()
			storeMock.On("WorkflowGetTree", pipeline).Return(tt.workflows, nil).Once()

			for _, workflow := range tt.workflows {
				if slices.Contains(tt.queueTasks, fmt.Sprint(workflow.ID)) || !workflow.Running() || workflow.AgentID == 0 {
					continue
				}
				if tt.missingAgents[workflow.AgentID] {
					storeMock.On("AgentFind", workflow.AgentID).Return(nil, store_types.ErrRecordNotExist).Once()
				} else {
					storeMock.On("AgentFind", workflow.AgentID).Return(tt.agents[workflow.AgentID], nil).Once()
				}
			}

			for _, workflow := range tt.workflows {
				if want, reap := tt.workflowStates[workflow.ID]; reap {
					finished := int64(0)
					if workflow.Started != 0 {
						finished = nowUnix
					}
					storeMock.On("WorkflowUpdate", mock.MatchedBy(func(updated *model.Workflow) bool {
						return updated.ID == workflow.ID && updated.State == want && updated.Finished == finished
					})).Return(nil).Once()
				}
				for _, step := range workflow.Children {
					want, reap := tt.stepStates[step.ID]
					if !reap {
						continue
					}
					finished := int64(0)
					if step.Started != 0 {
						finished = nowUnix
					}
					storeMock.On("StepUpdate", mock.MatchedBy(func(updated *model.Step) bool {
						return updated.ID == step.ID && updated.State == want && updated.Finished == finished
					})).Return(nil).Once()
				}
			}

			pipelineUpdated := false
			if tt.pipelineFinalized {
				storeMock.On("UpdatePipeline", mock.MatchedBy(func(updated *model.Pipeline) bool {
					return updated.ID == pipeline.ID && updated.Status == tt.pipelineFinal && updated.Finished == nowUnix
				})).Run(func(mock.Arguments) {
					pipelineUpdated = true
				}).Return(nil).Once()
			}

			changed := len(tt.workflowStates) > 0 || tt.pipelineFinalized
			if changed {
				user := &model.User{ID: repo.UserID}
				storeMock.On("GetUser", repo.UserID).Return(user, nil).Once()
				managerMock.On("ForgeFromRepo", repo).Return(nil, nil).Once()
				schedulerMock.On("PublishPipelineEvent", mock.Anything, repo, mock.Anything).Return(nil).Once()
			}

			workflowStates := make(map[int64]model.StatusValue, len(tt.workflows))
			workflowFinished := make(map[int64]int64, len(tt.workflows))
			stepStates := make(map[int64]model.StatusValue)
			stepFinished := make(map[int64]int64)
			for _, workflow := range tt.workflows {
				workflowStates[workflow.ID] = workflow.State
				workflowFinished[workflow.ID] = workflow.Finished
				for _, step := range workflow.Children {
					stepStates[step.ID] = step.State
					stepFinished[step.ID] = step.Finished
				}
			}

			require.NoError(t, ReapOrphanedWorkflows(t.Context(), storeMock, now, grace))
			assert.Equal(t, tt.pipelineFinalized, pipelineUpdated)
			for _, workflow := range tt.workflows {
				if want, reap := tt.workflowStates[workflow.ID]; reap {
					assert.Equal(t, want, workflow.State, "workflow %d", workflow.ID)
					finished := int64(0)
					if workflow.Started != 0 {
						finished = nowUnix
					}
					assert.Equal(t, finished, workflow.Finished, "workflow %d finished", workflow.ID)
				} else {
					assert.Equal(t, workflowStates[workflow.ID], workflow.State, "workflow %d", workflow.ID)
					assert.Equal(t, workflowFinished[workflow.ID], workflow.Finished, "workflow %d finished", workflow.ID)
				}
				for _, step := range workflow.Children {
					if want, reap := tt.stepStates[step.ID]; reap {
						assert.Equal(t, want, step.State, "step %d", step.ID)
						finished := int64(0)
						if step.Started != 0 {
							finished = nowUnix
						}
						assert.Equal(t, finished, step.Finished, "step %d finished", step.ID)
					} else {
						assert.Equal(t, stepStates[step.ID], step.State, "step %d", step.ID)
						assert.Equal(t, stepFinished[step.ID], step.Finished, "step %d finished", step.ID)
					}
				}
			}
			if changed {
				schedulerMock.AssertNumberOfCalls(t, "PublishPipelineEvent", 1)
			} else {
				schedulerMock.AssertNotCalled(t, "PublishPipelineEvent", mock.Anything, mock.Anything, mock.Anything)
			}
		})
	}
}
