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
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	forge_mocks "go.woodpecker-ci.org/woodpecker/v3/server/forge/mocks"
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
		pipelineUpdated   int64
		workflows         []*model.Workflow
		repoTimeout       int64
		maxTimeout        int64
		queueTasks        []string
		persistedTasks    []string
		queueState        string
		taskListErr       error
		missingAgents     map[int64]bool
		agents            map[int64]*model.Agent
		workflowStates    map[int64]model.StatusValue
		stepStates        map[int64]model.StatusValue
		pipelineFinal     model.StatusValue
		pipelineFinalized bool
		forgeStatus       bool
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
			name:            "pending scheduler task remains untouched",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 3600,
			workflows:       []*model.Workflow{{ID: 42, State: model.StatusRunning, Started: nowUnix - 3600, AgentID: 7}},
			queueTasks:      []string{fmt.Sprint(42)},
		},
		{
			name:            "persisted task absent from scheduler remains untouched",
			pipelineStatus:  model.StatusPending,
			pipelineCreated: nowUnix - 3600,
			workflows:       []*model.Workflow{{ID: 420, State: model.StatusPending}},
			persistedTasks:  []string{fmt.Sprint(420)},
		},
		{
			name:           "task list error fails closed",
			pipelineStatus: model.StatusRunning,
			workflows:      []*model.Workflow{{ID: 421, State: model.StatusRunning, Started: nowUnix - 3600, AgentID: 71}},
			taskListErr:    errors.New("task store unavailable"),
			missingAgents:  map[int64]bool{71: true},
		},
		{
			name:            "waiting-on-dependencies queue task remains untouched",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 3600,
			workflows:       []*model.Workflow{{ID: 49, State: model.StatusRunning, Started: nowUnix - 3600, AgentID: 7}},
			queueTasks:      []string{fmt.Sprint(49)},
			queueState:      "waiting",
		},
		{
			name:            "running queue task remains untouched",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 3600,
			workflows:       []*model.Workflow{{ID: 50, State: model.StatusRunning, Started: nowUnix - 3600, AgentID: 7}},
			queueTasks:      []string{fmt.Sprint(50)},
			queueState:      "running",
		},
		{
			name:            "recent agent within timeout remains untouched",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 1800,
			workflows:       []*model.Workflow{{ID: 43, State: model.StatusRunning, Started: nowUnix - 1800, AgentID: 8}},
			repoTimeout:     60,
			agents:          map[int64]*model.Agent{8: {ID: 8, LastContact: nowUnix - 30}},
		},
		{
			name:              "old agent contact reaps a workflow",
			pipelineStatus:    model.StatusRunning,
			pipelineCreated:   nowUnix - 3600,
			workflows:         []*model.Workflow{{ID: 54, State: model.StatusRunning, Started: nowUnix - 1800, AgentID: 15}},
			repoTimeout:       60,
			agents:            map[int64]*model.Agent{15: {ID: 15, LastContact: nowUnix - 900}},
			workflowStates:    map[int64]model.StatusValue{54: model.StatusKilled},
			pipelineFinal:     model.StatusKilled,
			pipelineFinalized: true,
		},
		{
			name:            "recently created pending workflow stays within grace",
			pipelineStatus:  model.StatusPending,
			pipelineCreated: nowUnix - 300,
			workflows:       []*model.Workflow{{ID: 55, State: model.StatusPending}},
		},
		{
			name:            "recent approval protects pending workflow",
			pipelineStatus:  model.StatusPending,
			pipelineCreated: nowUnix - 3600,
			pipelineUpdated: nowUnix - 300,
			workflows:       []*model.Workflow{{ID: 551, State: model.StatusPending}},
		},
		{
			name:            "pipeline pending age protects a recently started workflow",
			pipelineStatus:  model.StatusPending,
			pipelineCreated: nowUnix - 3600,
			workflows:       []*model.Workflow{{ID: 53, State: model.StatusRunning, Started: nowUnix - 300, AgentID: 14}},
			missingAgents:   map[int64]bool{14: true},
		},
		{
			name:            "agent contact exactly at grace cutoff is not old",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 3600,
			workflows:       []*model.Workflow{{ID: 56, State: model.StatusRunning, Started: nowUnix - 1800, AgentID: 16}},
			repoTimeout:     60,
			agents:          map[int64]*model.Agent{16: {ID: 16, LastContact: nowUnix - int64(grace.Seconds())}},
		},
		{
			name:            "workflow exactly at age grace cutoff is not old",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 3600,
			workflows:       []*model.Workflow{{ID: 57, State: model.StatusRunning, Started: nowUnix - int64(grace.Seconds()), AgentID: 17}},
			missingAgents:   map[int64]bool{17: true},
		},
		{
			name:            "default timeout remains active until timeout and grace pass",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 3600,
			workflows:       []*model.Workflow{{ID: 52, State: model.StatusRunning, Started: nowUnix - int64((60*time.Minute + 9*time.Minute).Seconds()), AgentID: 13}},
			agents:          map[int64]*model.Agent{13: {ID: 13, LastContact: nowUnix - 30}},
		},
		{
			name:            "max timeout protects live agent workflow",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 1800,
			workflows:       []*model.Workflow{{ID: 443, State: model.StatusRunning, Started: nowUnix - int64((30 * time.Minute).Seconds()), AgentID: 90}},
			repoTimeout:     10,
			maxTimeout:      60,
			agents:          map[int64]*model.Agent{90: {ID: 90, LastContact: nowUnix - 30}},
		},
		{
			name:              "max timeout plus grace reaps live agent workflow",
			pipelineStatus:    model.StatusRunning,
			pipelineCreated:   nowUnix - 4800,
			workflows:         []*model.Workflow{{ID: 444, State: model.StatusRunning, Started: nowUnix - int64((80 * time.Minute).Seconds()), AgentID: 91}},
			repoTimeout:       10,
			maxTimeout:        60,
			agents:            map[int64]*model.Agent{91: {ID: 91, LastContact: nowUnix - 30}},
			workflowStates:    map[int64]model.StatusValue{444: model.StatusKilled},
			pipelineFinal:     model.StatusKilled,
			pipelineFinalized: true,
		},
		{
			name:            "orphan younger than grace remains untouched",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 300,
			workflows:       []*model.Workflow{{ID: 45, State: model.StatusRunning, Started: nowUnix - 300, AgentID: 10}},
			missingAgents:   map[int64]bool{10: true},
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
			pipelineFinal:     model.StatusCanceled,
			pipelineFinalized: true,
		},
		{
			name:              "reaped started workflow finalizes pipeline as killed",
			pipelineStatus:    model.StatusRunning,
			pipelineCreated:   nowUnix - 3600,
			workflows:         []*model.Workflow{{ID: 461, State: model.StatusRunning, Started: nowUnix - 3600, AgentID: 92}},
			missingAgents:     map[int64]bool{92: true},
			workflowStates:    map[int64]model.StatusValue{461: model.StatusKilled},
			pipelineFinal:     model.StatusKilled,
			pipelineFinalized: true,
		},
		{
			name:            "two orphan workflows sharing agent use one lookup",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 3600,
			workflows: []*model.Workflow{
				{ID: 462, State: model.StatusRunning, Started: nowUnix - 3600, AgentID: 93},
				{ID: 463, State: model.StatusRunning, Started: nowUnix - 3600, AgentID: 93},
			},
			missingAgents:     map[int64]bool{93: true},
			workflowStates:    map[int64]model.StatusValue{462: model.StatusKilled, 463: model.StatusKilled},
			pipelineFinal:     model.StatusKilled,
			pipelineFinalized: true,
		},
		{
			name:              "forge status receives terminal pipeline",
			pipelineStatus:    model.StatusPending,
			pipelineCreated:   nowUnix - 3600,
			workflows:         []*model.Workflow{{ID: 464, State: model.StatusPending}},
			workflowStates:    map[int64]model.StatusValue{464: model.StatusCanceled},
			pipelineFinal:     model.StatusCanceled,
			pipelineFinalized: true,
			forgeStatus:       true,
		},
		{
			name:            "live workflow keeps pipeline running after another is reaped",
			pipelineStatus:  model.StatusRunning,
			pipelineCreated: nowUnix - 3600,
			workflows: []*model.Workflow{
				{ID: 47, State: model.StatusRunning, Started: nowUnix - 3600, AgentID: 11},
				{ID: 48, State: model.StatusRunning, Started: nowUnix - 300, AgentID: 12},
			},
			agents:         map[int64]*model.Agent{12: {ID: 12, LastContact: nowUnix - 30}},
			missingAgents:  map[int64]bool{11: true},
			workflowStates: map[int64]model.StatusValue{47: model.StatusKilled},
		},
		{
			name:              "active pipeline with only terminal workflows keeps their status",
			pipelineStatus:    model.StatusRunning,
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
				Status: tt.pipelineStatus, Created: tt.pipelineCreated, Updated: tt.pipelineUpdated,
			}

			oldScheduler := server.Config.Services.Scheduler
			oldManager := server.Config.Services.Manager
			oldPerWorkflow := server.Config.Server.StatusPerWorkflow
			oldAggregate := server.Config.Server.StatusAggregate
			oldMeta := server.Config.Server.StatusMeta
			oldMaxTimeout := server.Config.Pipeline.MaxTimeout
			t.Cleanup(func() {
				server.Config.Services.Scheduler = oldScheduler
				server.Config.Services.Manager = oldManager
				server.Config.Server.StatusPerWorkflow = oldPerWorkflow
				server.Config.Server.StatusAggregate = oldAggregate
				server.Config.Server.StatusMeta = oldMeta
				server.Config.Pipeline.MaxTimeout = oldMaxTimeout
			})
			server.Config.Server.StatusPerWorkflow = tt.forgeStatus
			server.Config.Server.StatusAggregate = false
			server.Config.Server.StatusMeta = false
			server.Config.Pipeline.MaxTimeout = tt.maxTimeout

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

			managerMock := manager_mocks.NewMockManager(t)
			server.Config.Services.Manager = managerMock
			storeMock := store_mocks.NewMockStore(t)
			storeMock.On("GetPipelineQueue").Return([]*model.Feed{{ID: pipeline.ID}}, nil).Once()
			persistedTasks := make([]*model.Task, 0, len(tt.persistedTasks))
			for _, id := range tt.persistedTasks {
				persistedTasks = append(persistedTasks, &model.Task{ID: id})
			}
			taskListCall := storeMock.On("TaskList").Return(persistedTasks, tt.taskListErr).Once()
			if tt.taskListErr == nil {
				schedulerMock.On("Info", mock.Anything).Return(queueInfo).Once().NotBefore(taskListCall)
			}
			if tt.taskListErr == nil {
				storeMock.On("GetPipeline", pipeline.ID).Return(pipeline, nil).Once()
				storeMock.On("GetRepo", repo.ID).Return(repo, nil).Once()
				storeMock.On("WorkflowGetTree", pipeline).Return(tt.workflows, nil).Once()
			}

			if tt.taskListErr == nil {
				seenAgents := make(map[int64]struct{})
				for _, workflow := range tt.workflows {
					if slices.Contains(tt.queueTasks, fmt.Sprint(workflow.ID)) || !workflow.Running() || workflow.AgentID == 0 {
						continue
					}
					if _, seen := seenAgents[workflow.AgentID]; seen {
						continue
					}
					seenAgents[workflow.AgentID] = struct{}{}
					if tt.missingAgents[workflow.AgentID] {
						storeMock.On("AgentFind", workflow.AgentID).Return(nil, store_types.ErrRecordNotExist).Once()
					} else {
						storeMock.On("AgentFind", workflow.AgentID).Return(tt.agents[workflow.AgentID], nil).Once()
					}
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
				if !tt.pipelineFinalized {
					// A non-terminal post re-reads the pipeline first (stale-pending guard).
					storeMock.On("GetPipeline", pipeline.ID).Return(pipeline, nil).Once()
				}
				if tt.forgeStatus {
					forgeMock := forge_mocks.NewMockForge(t)
					forgeMock.On("Status", mock.Anything, user, repo, mock.MatchedBy(func(got *model.Pipeline) bool {
						return got.Status == tt.pipelineFinal && got.Finished == nowUnix
					}), mock.MatchedBy(func(got *model.Workflow) bool {
						return got.ID == 464 && got.State == model.StatusCanceled
					})).Return(nil).Once()
					managerMock.On("ForgeFromRepo", repo).Return(forgeMock, nil).Once()
				} else {
					managerMock.On("ForgeFromRepo", repo).Return(nil, nil).Once()
				}
				schedulerMock.On("PublishPipelineEvent", mock.Anything, repo, mock.Anything).Return(nil).Once()
			}

			workflowInitialStates := make(map[int64]model.StatusValue, len(tt.workflows))
			stepInitialStates := make(map[int64]model.StatusValue)
			for _, workflow := range tt.workflows {
				workflowInitialStates[workflow.ID] = workflow.State
				for _, step := range workflow.Children {
					stepInitialStates[step.ID] = step.State
				}
			}
			wantErr := tt.taskListErr
			err := ReapOrphanedWorkflows(t.Context(), storeMock, now, grace)
			if wantErr != nil {
				require.ErrorIs(t, err, wantErr)
			} else {
				require.NoError(t, err)
			}
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
					assert.Equal(t, workflowInitialStates[workflow.ID], workflow.State, "workflow %d", workflow.ID)
				}
				for _, step := range workflow.Children {
					if want, reap := tt.stepStates[step.ID]; reap {
						assert.Equal(t, want, step.State, "step %d", step.ID)
					} else {
						assert.Equal(t, stepInitialStates[step.ID], step.State, "step %d", step.ID)
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
