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

// killAfterSnapshotStore kills the pipeline through the real store on the
// reaper's first read of it. The reaper keeps the pre-kill snapshot, as it
// does when a kill lands between its read and its finalize write.
type killAfterSnapshotStore struct {
	store.Store
	pipelineID int64
	killed     bool
}

func (s *killAfterSnapshotStore) GetPipeline(id int64) (*model.Pipeline, error) {
	snapshot, err := s.Store.GetPipeline(id)
	if err != nil || s.killed || id != s.pipelineID {
		return snapshot, err
	}
	s.killed = true
	if _, err := UpdateToStatusKilled(s.Store, *snapshot, &model.CancelInfo{}, model.StatusKilled); err != nil {
		return nil, err
	}
	return snapshot, nil
}

func TestReapDoesNotOverwriteKilledPipeline(t *testing.T) {
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
		State:      model.StatusSuccess,
		Started:    now.Add(-time.Hour).Unix(),
		Finished:   now.Add(-time.Minute).Unix(),
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

	reaperStore := &killAfterSnapshotStore{Store: f.store, pipelineID: pipeline.ID}
	require.NoError(t, ReapOrphanedWorkflows(t.Context(), reaperStore, now, time.Minute, 0, 0))
	require.True(t, reaperStore.killed, "the kill must land between the reaper's read and its finalize")

	stored, err := f.store.GetPipeline(pipeline.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusKilled, stored.Status, "reaper must not replace a concurrent kill with its aggregate status")
}

// TestReapConflictAfterWorkflowWritesPublishesStoredPipeline reaps a running
// workflow, then lets a kill land after the reaper's snapshot. The reaper's
// workflow write already happened, so the stored pipeline must still publish.
func TestReapConflictAfterWorkflowWritesPublishesStoredPipeline(t *testing.T) {
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
		AgentID:    0,
	}}))

	schedulerMock := scheduler_mocks.NewMockScheduler(t)
	schedulerMock.On("Info", mock.Anything).Return(queue.InfoT{}).Maybe()
	schedulerMock.On("PublishPipelineEvent", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	managerMock := manager_mocks.NewMockManager(t)
	managerMock.On("ForgeFromRepo", mock.Anything).Return(nil, nil).Maybe()
	oldScheduler, oldManager := server.Config.Services.Scheduler, server.Config.Services.Manager
	server.Config.Services.Scheduler = schedulerMock
	server.Config.Services.Manager = managerMock
	t.Cleanup(func() {
		server.Config.Services.Scheduler = oldScheduler
		server.Config.Services.Manager = oldManager
	})

	reaperStore := &killAfterSnapshotStore{Store: f.store, pipelineID: pipeline.ID}
	require.NoError(t, ReapOrphanedWorkflows(t.Context(), reaperStore, now, time.Minute, 0, 0))
	require.True(t, reaperStore.killed, "the kill must land between the reaper's read and its finalize")

	stored, err := f.store.GetPipeline(pipeline.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusKilled, stored.Status, "reaper must not replace a concurrent kill")

	schedulerMock.AssertNumberOfCalls(t, "PublishPipelineEvent", 1)
	var published *model.Pipeline
	for _, call := range schedulerMock.Calls {
		if call.Method == "PublishPipelineEvent" {
			published = call.Arguments.Get(2).(*model.Pipeline)
		}
	}
	require.NotNil(t, published)
	assert.Equal(t, model.StatusKilled, published.Status, "publish must carry the stored killed status")
}

type workflowCompletionRaceStore struct {
	store.Store
	completedAt int64
	updated     bool
}

func (s *workflowCompletionRaceStore) WorkflowUpdateIfState(workflow *model.Workflow, from model.StatusValue) error {
	current, err := s.Store.WorkflowLoad(workflow.ID)
	if err != nil {
		return err
	}
	current.State = model.StatusSuccess
	current.Finished = s.completedAt
	if err := s.Store.WorkflowUpdate(current); err != nil {
		return err
	}
	s.updated = true
	return s.Store.WorkflowUpdateIfState(workflow, from)
}

type stepCompletionRaceStore struct {
	store.Store
	completedAt int64
	updated     bool
}

func (s *stepCompletionRaceStore) StepUpdateIfState(step *model.Step, from model.StatusValue) error {
	current, err := s.Store.StepLoad(step.PipelineID, step.ID)
	if err != nil {
		return err
	}
	current.State = model.StatusSuccess
	current.Finished = s.completedAt
	if err := s.Store.StepUpdate(current); err != nil {
		return err
	}
	s.updated = true
	return s.Store.StepUpdateIfState(step, from)
}

func TestReapDoesNotOverwriteCompletedWorkflow(t *testing.T) {
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
	}}))

	schedulerMock := scheduler_mocks.NewMockScheduler(t)
	schedulerMock.On("Info", mock.Anything).Return(queue.InfoT{}).Maybe()
	oldScheduler := server.Config.Services.Scheduler
	server.Config.Services.Scheduler = schedulerMock
	t.Cleanup(func() { server.Config.Services.Scheduler = oldScheduler })

	reaperStore := &workflowCompletionRaceStore{Store: f.store, completedAt: now.Unix()}
	require.NoError(t, ReapOrphanedWorkflows(t.Context(), reaperStore, now, time.Minute, 0, 0))
	require.True(t, reaperStore.updated, "concurrent completion must land after the reaper snapshot")

	stored, err := f.store.WorkflowLoad(1)
	require.NoError(t, err)
	assert.Equal(t, model.StatusSuccess, stored.State)
	assert.Equal(t, now.Unix(), stored.Finished)
}

func TestReapDoesNotOverwriteCompletedStep(t *testing.T) {
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
			PID:     1,
			PPID:    1,
			Name:    "step",
			State:   model.StatusRunning,
			Started: now.Add(-time.Hour).Unix(),
		}},
	}}))

	workflow, err := f.store.WorkflowLoad(1)
	require.NoError(t, err)

	schedulerMock := scheduler_mocks.NewMockScheduler(t)
	schedulerMock.On("Info", mock.Anything).Return(queue.InfoT{}).Maybe()
	oldScheduler := server.Config.Services.Scheduler
	server.Config.Services.Scheduler = schedulerMock
	t.Cleanup(func() { server.Config.Services.Scheduler = oldScheduler })

	reaperStore := &stepCompletionRaceStore{Store: f.store, completedAt: now.Unix()}
	require.NoError(t, ReapOrphanedWorkflows(t.Context(), reaperStore, now, time.Minute, 0, 0))
	require.True(t, reaperStore.updated, "concurrent completion must land after the reaper snapshot")

	steps, err := f.store.StepListFromWorkflowFind(workflow)
	require.NoError(t, err)
	require.Len(t, steps, 1)
	assert.Equal(t, model.StatusSuccess, steps[0].State)
	assert.Equal(t, now.Unix(), steps[0].Finished)
}
