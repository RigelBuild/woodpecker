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

package github

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/go-github/v92/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/datastore"
)

// TestStatusAggregateStoredSuccessReapedCodeWorkflowIsNotGreen is the store-backed
// RIG-7551 regression for Woodpecker pipeline #10874. The reaper leaves a stored
// success pipeline whose `test` workflow is skipped with a canceled step, beside a
// successful `build`. The tree is read back from the store, so the test sees the
// rows the live report loads. The rollup alone reads success; the reaped-workflow
// guard must post failure.
func TestStatusAggregateStoredSuccessReapedCodeWorkflowIsNotGreen(t *testing.T) {
	s := datastore.NewTestStore(t)
	owner := &model.User{Login: "octocat", ForgeRemoteID: "1"}
	require.NoError(t, s.CreateUser(owner))

	var posted github.RepoStatus
	c, ctx, repo, user, pipeline := statusAggregateFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&posted); err != nil {
			t.Errorf("decode posted status: %v", err)
			return
		}
		w.WriteHeader(http.StatusCreated)
		if _, err := w.Write([]byte(`{}`)); err != nil {
			t.Errorf("write response: %v", err)
		}
	})
	repo.UserID = owner.ID
	repo.FullName = "o/r"
	require.NoError(t, s.CreateRepo(repo))
	pipeline.RepoID = repo.ID
	require.NoError(t, s.CreatePipeline(pipeline))

	workflows := []*model.Workflow{
		{PipelineID: pipeline.ID, PID: 1, Name: "build", State: model.StatusSuccess, Children: []*model.Step{
			{UUID: "build", PipelineID: pipeline.ID, PID: 1, PPID: 1, Name: "build", State: model.StatusSuccess},
		}},
		{PipelineID: pipeline.ID, PID: 2, Name: "test", State: model.StatusSkipped, Children: []*model.Step{
			{UUID: "test", PipelineID: pipeline.ID, PID: 2, PPID: 2, Name: "test", State: model.StatusCanceled},
		}},
	}
	require.NoError(t, s.WorkflowsCreate(workflows))
	tree, err := s.WorkflowGetTree(pipeline)
	require.NoError(t, err)
	require.Len(t, tree, 2)

	require.NoError(t, c.StatusAggregate(ctx, user, repo, pipeline, tree))
	assert.Equal(t, statusFailure, posted.GetState(),
		"a stored success pipeline with a reaped code workflow must not post CI (pr) green")
}
