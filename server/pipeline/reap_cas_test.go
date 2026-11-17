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
	"fmt"
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
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

// updateAfterTreeSnapshotStore applies a concurrent workflow update after the
// reaper has loaded its workflow tree, while returning the original snapshot.
type updateAfterTreeSnapshotStore struct {
	store.Store
	pipelineID int64
	update     func([]*model.Workflow) error
	updated    bool
}

func (s *updateAfterTreeSnapshotStore) WorkflowGetTree(pipeline *model.Pipeline) ([]*model.Workflow, error) {
	workflows, err := s.Store.WorkflowGetTree(pipeline)
	if err != nil || s.updated || pipeline.ID != s.pipelineID {
		return workflows, err
	}
	s.updated = true
	if err := s.update(workflows); err != nil {
		return nil, err
	}
	return workflows, nil
}

func TestReapPreservesDoneAfterWorkflowRead(t *testing.T) {
	f := newLivenessFixture(t)
	now := time.Now()
	pipeline, err := f.store.GetPipeline(f.pipeline.ID)
	require.NoError(t, err)
	pipeline.Status = model.StatusRunning
	pipeline.Started = now.Add(-time.Hour).Unix()
	require.NoError(t, f.store.UpdatePipeline(pipeline))
	require.NoError(t, f.store.WorkflowsCreate([]*model.Workflow{{
		PipelineID: pipeline.ID,
		PID:        1,
		Name:       "build",
		State:      model.StatusRunning,
		Started:    now.Add(-time.Hour).Unix(),
		Children: []*model.Step{{
			PipelineID: pipeline.ID,
			PID:        1,
			PPID:       1,
			Name:       "build",
			State:      model.StatusRunning,
			Started:    now.Add(-time.Hour).Unix(),
		}},
	}}))

	schedulerMock := scheduler_mocks.NewMockScheduler(t)
	schedulerMock.On("Info", mock.Anything).Return(queue.InfoT{}).Maybe()
	schedulerMock.On("PublishPipelineEvent", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	managerMock := manager_mocks.NewMockManager(t)
	managerMock.On("ForgeFromRepo", mock.Anything).Return(nil, nil).Maybe()
	oldScheduler, oldManager := server.Config.Services.Scheduler, server.Config.Services.Manager
	server.Config.Services.Scheduler = schedulerMock
	server.Config.Services.Manager = managerMock
	t.Cleanup(func() {
		server.Config.Services.Scheduler = oldScheduler
		server.Config.Services.Manager = oldManager
	})

	reaperStore := &updateAfterTreeSnapshotStore{
		Store:      f.store,
		pipelineID: pipeline.ID,
		update: func(workflows []*model.Workflow) error {
			require.Len(t, workflows, 1)
			workflow := *workflows[0]
			require.Len(t, workflow.Children, 1)
			step := *workflow.Children[0]
			step.State = model.StatusSuccess
			step.Finished = now.Unix()
			if err := f.store.StepUpdate(&step); err != nil {
				return err
			}
			workflow.State = model.StatusSuccess
			workflow.Finished = now.Unix()
			return f.store.WorkflowUpdate(&workflow)
		},
	}
	require.NoError(t, ReapOrphanedWorkflows(t.Context(), reaperStore, now, time.Minute, 0, 0))
	require.True(t, reaperStore.updated, "Done must land after the reaper reads the workflow tree")

	stored, err := f.store.WorkflowGetTree(pipeline)
	require.NoError(t, err)
	require.Len(t, stored, 1)
	assert.Equal(t, model.StatusSuccess, stored[0].State)
	require.Len(t, stored[0].Children, 1)
	assert.Equal(t, model.StatusSuccess, stored[0].Children[0].State)
}

func TestReapPreservesInitAfterPendingTimeoutRead(t *testing.T) {
	f := newLivenessFixture(t)
	now := time.Now()
	pipeline, err := f.store.GetPipeline(f.pipeline.ID)
	require.NoError(t, err)
	pipeline.Status = model.StatusRunning
	pipeline.Created = now.Add(-8 * time.Hour).Unix()
	require.NoError(t, f.store.UpdatePipeline(pipeline))
	require.NoError(t, f.store.WorkflowsCreate([]*model.Workflow{{
		PipelineID: pipeline.ID,
		PID:        1,
		Name:       "build",
		State:      model.StatusPending,
	}}))
	workflows, err := f.store.WorkflowGetTree(pipeline)
	require.NoError(t, err)
	require.Len(t, workflows, 1)
	workflowID := workflows[0].ID

	task := &model.Task{
		ID:           fmt.Sprint(workflowID),
		Labels:       map[string]string{"type": "linux"},
		Dependencies: []string{},
		DepStatus:    map[string]model.StatusValue{},
	}
	schedulerMock := scheduler_mocks.NewMockScheduler(t)
	schedulerMock.On("Info", mock.Anything).Return(queue.InfoT{Pending: []*model.Task{task}}).Maybe()
	schedulerMock.On("CancelWorkflows", mock.Anything, mock.Anything).Return(nil).Maybe()
	schedulerMock.On("PublishPipelineEvent", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	managerMock := manager_mocks.NewMockManager(t)
	managerMock.On("ForgeFromRepo", mock.Anything).Return(nil, nil).Maybe()
	oldScheduler, oldManager := server.Config.Services.Scheduler, server.Config.Services.Manager
	server.Config.Services.Scheduler = schedulerMock
	server.Config.Services.Manager = managerMock
	t.Cleanup(func() {
		server.Config.Services.Scheduler = oldScheduler
		server.Config.Services.Manager = oldManager
	})

	reaperStore := &updateAfterTreeSnapshotStore{
		Store:      f.store,
		pipelineID: pipeline.ID,
		update: func(workflows []*model.Workflow) error {
			require.Len(t, workflows, 1)
			updated := *workflows[0]
			updated.State = model.StatusRunning
			updated.Started = now.Unix()
			updated.AgentID = 1
			return f.store.WorkflowUpdate(&updated)
		},
	}
	require.NoError(t, ReapOrphanedWorkflows(t.Context(), reaperStore, now, time.Minute, 6*time.Hour, 0))
	require.True(t, reaperStore.updated, "Init must land after the reaper reads the workflow tree")

	stored, err := f.store.WorkflowLoad(workflowID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusRunning, stored.State)
	assert.Equal(t, now.Unix(), stored.Started)
	assert.Equal(t, int64(1), stored.AgentID)
}
