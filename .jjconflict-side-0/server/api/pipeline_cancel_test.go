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

//go:build test

package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	forge_mocks "go.woodpecker-ci.org/woodpecker/v3/server/forge/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pubsub/memory"
	"go.woodpecker-ci.org/woodpecker/v3/server/queue"
	"go.woodpecker-ci.org/woodpecker/v3/server/scheduler"
	manager_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/mocks"
)

func TestCancelPendingWorkflowsRejectsMalformedRequests(t *testing.T) {
	s := newTestStore(t)
	repo, _ := cronFixture(t, s)
	current := &model.Pipeline{RepoID: repo.ID, Number: 1, Status: model.StatusPending}
	require.NoError(t, s.CreatePipeline(current))
	workflow := &model.Workflow{PipelineID: current.ID, PID: 1, Name: "pending", State: model.StatusPending}
	require.NoError(t, s.WorkflowsCreate([]*model.Workflow{workflow}))
	q := queue.WithTaskStore(t.Context(), queue.NewMemoryQueue(t.Context()), s)
	require.NoError(t, q.PushAtOnce(t.Context(), []*model.Task{{ID: strconv.FormatInt(workflow.ID, 10)}}))
	valid := `{"workflow_ids":[1],"superseded_by":2}`
	tooLarge := valid + strings.Repeat(" ", (64<<10)+1-len(valid))
	ids := make([]string, 513)
	for i := range ids {
		ids[i] = strconv.Itoa(i + 1)
	}
	tooManyIDs := `{"workflow_ids":[` + strings.Join(ids, ",") + `],"superseded_by":2}`
	for name, body := range map[string]string{
		"empty selection": `{"workflow_ids":[],"superseded_by":2}`,
		"duplicate IDs":   `{"workflow_ids":[1,1],"superseded_by":2}`,
		"negative ID":     `{"workflow_ids":[-1],"superseded_by":2}`,
		"unknown field":   `{"workflow_ids":[1],"superseded_by":2,"extra":true}`,
		"trailing JSON":   valid + ` {}`,
		"too many IDs":    tooManyIDs,
		"oversized body":  tooLarge,
	} {
		t.Run(name, func(t *testing.T) {
			tc := newTestContext(t, s)
			withRepo(repo, &model.Perm{})(tc)
			withRawBody(http.MethodPost, "application/json", []byte(body))(tc)
			tc.Ctx.Set("pipeline", current)
			CancelPendingWorkflows(tc.Ctx)
			want := http.StatusBadRequest
			if name == "oversized body" {
				want = http.StatusRequestEntityTooLarge
			}
			assert.Equal(t, want, tc.Recorder.Code)
			got, err := s.WorkflowLoad(workflow.ID)
			require.NoError(t, err)
			assert.Equal(t, model.StatusPending, got.State)
			assert.Nil(t, got.SupersededBy)
			assert.Len(t, q.Info(t.Context()).Pending, 1)
			tasks, err := s.TaskList()
			require.NoError(t, err)
			assert.Len(t, tasks, 1)
		})
	}
}

func TestCancelPendingWorkflowsSupersedesSelectedClosure(t *testing.T) {
	s := newTestStore(t)
	repo, _ := cronFixture(t, s)
	current := &model.Pipeline{RepoID: repo.ID, Number: 1, Status: model.StatusRunning}
	successor := &model.Pipeline{RepoID: repo.ID, Number: 2, Status: model.StatusPending}
	require.NoError(t, s.CreatePipeline(current))
	require.NoError(t, s.CreatePipeline(successor))
	workflows := []*model.Workflow{
		{PipelineID: current.ID, PID: 1, Name: "producer", State: model.StatusPending, Children: []*model.Step{{UUID: "producer", PipelineID: current.ID, PID: 1, PPID: 1, State: model.StatusPending}}},
		{PipelineID: current.ID, PID: 2, Name: "consumer", DependsOn: []string{"producer"}, State: model.StatusPending, Children: []*model.Step{{UUID: "consumer", PipelineID: current.ID, PID: 2, PPID: 2, State: model.StatusPending}}},
		{PipelineID: current.ID, PID: 3, Name: "running", State: model.StatusRunning},
	}
	require.NoError(t, s.WorkflowsCreate(workflows))
	for _, workflow := range workflows[:2] {
		require.NoError(t, s.TaskInsert(&model.Task{ID: strconv.FormatInt(workflow.ID, 10), PipelineID: current.ID}))
	}

	runHandler := func(ids []int64) *testContext {
		tc := newTestContext(t, s)
		withRepo(repo, &model.Perm{})(tc)
		withParam("pipeline_number", "1")(tc)
		tc.Ctx.Set("pipeline", current)
		withRawBody(http.MethodPost, "application/json", mustJSON(t, CancelPendingWorkflowsRequest{WorkflowIDs: ids, SupersededBy: 2}))(tc)
		CancelPendingWorkflows(tc.Ctx)
		return tc
	}

	originalScheduler := server.Config.Services.Scheduler
	originalManager := server.Config.Services.Manager
	originalServerConfig := server.Config.Server
	t.Cleanup(func() {
		server.Config.Services.Scheduler = originalScheduler
		server.Config.Services.Manager = originalManager
		server.Config.Server = originalServerConfig
	})
	server.Config.Server.StatusPerWorkflow = false
	server.Config.Server.StatusAggregate = false
	server.Config.Server.StatusMeta = false

	mockForge := forge_mocks.NewMockForge(t)
	manager := manager_mocks.NewMockManager(t)
	manager.On("ForgeFromRepo", repo).Return(mockForge, nil)
	server.Config.Services.Manager = manager
	q := queue.NewMemoryQueue(t.Context())
	for _, workflow := range workflows[:2] {
		require.NoError(t, q.PushAtOnce(t.Context(), []*model.Task{{ID: strconv.FormatInt(workflow.ID, 10), PipelineID: current.ID}}))
	}
	server.Config.Services.Scheduler = scheduler.NewScheduler(t.Context(), s, q, memory.New())
	tc := newTestContext(t, s)
	withRepo(repo, &model.Perm{})(tc)
	tc.Ctx.Set("pipeline", current)
	withRawBody(http.MethodPost, "application/json", mustJSON(t, CancelPendingWorkflowsRequest{WorkflowIDs: []int64{workflows[0].ID}, SupersededBy: current.Number}))(tc)
	CancelPendingWorkflows(tc.Ctx)
	assert.Equal(t, http.StatusBadRequest, tc.Recorder.Code)
	unchanged, err := s.WorkflowLoad(workflows[0].ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusPending, unchanged.State)
	assert.Len(t, q.Info(t.Context()).Pending, 2)

	response := runHandler([]int64{workflows[0].ID})
	assert.Equal(t, http.StatusConflict, response.Recorder.Code)
	pending, err := s.WorkflowLoad(workflows[0].ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusPending, pending.State)
	foreignPipeline := &model.Pipeline{RepoID: repo.ID, Number: 3, Status: model.StatusPending}
	require.NoError(t, s.CreatePipeline(foreignPipeline))
	foreign := &model.Workflow{PipelineID: foreignPipeline.ID, PID: 1, Name: "foreign", State: model.StatusPending}
	require.NoError(t, s.WorkflowsCreate([]*model.Workflow{foreign}))
	for _, id := range []int64{foreign.ID, foreign.ID + 10000} {
		response = runHandler([]int64{workflows[0].ID, id})
		assert.Equal(t, http.StatusNotFound, response.Recorder.Code)
		pending, err = s.WorkflowLoad(workflows[0].ID)
		require.NoError(t, err)
		assert.Equal(t, model.StatusPending, pending.State)
		assert.Len(t, q.Info(t.Context()).Pending, 2)
		tasks, listErr := s.TaskList()
		require.NoError(t, listErr)
		assert.Len(t, tasks, 2)
	}

	response = runHandler([]int64{workflows[0].ID, workflows[1].ID})
	assert.Equal(t, http.StatusOK, response.Recorder.Code)
	for _, workflow := range workflows[:2] {
		got, err := s.WorkflowLoad(workflow.ID)
		require.NoError(t, err)
		assert.Equal(t, model.StatusSkipped, got.State)
		require.NotNil(t, got.SupersededBy)
		assert.Equal(t, int64(2), *got.SupersededBy)
	}
	gotRunning, err := s.WorkflowLoad(workflows[2].ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusRunning, gotRunning.State)
	assert.Empty(t, q.Info(t.Context()).Pending)
	assert.Empty(t, q.Info(t.Context()).WaitingOnDeps)
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	return encoded
}
