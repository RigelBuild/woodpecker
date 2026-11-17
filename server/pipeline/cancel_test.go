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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pubsub/memory"
	queue_mocks "go.woodpecker-ci.org/woodpecker/v3/server/queue/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/scheduler"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

func TestSupersedes(t *testing.T) {
	pr := func(id, number int64, commit string) *model.Pipeline {
		return &model.Pipeline{ID: id, Number: number, Commit: commit, Event: model.EventPull, Refspec: "feat:main"}
	}
	push := func(id, number int64, commit, branch string) *model.Pipeline {
		return &model.Pipeline{ID: id, Number: number, Commit: commit, Event: model.EventPush, Branch: branch}
	}
	other := pr(2, 2, "bbb")
	restart := pr(3, 3, "aaa")
	restart.Parent = 1
	staleRestart := pr(1, 1, "aaa")
	staleRestart.Parent = 1
	other.Refspec = "other:main"
	meta := func(id, number int64, commit string) *model.Pipeline {
		p := pr(id, number, commit)
		p.Event = model.EventPullMetadata
		return p
	}

	tests := []struct {
		name           string
		pipeline, prev *model.Pipeline
		want           bool
	}{
		{"newer push to the PR cancels the older one", pr(2, 2, "bbb"), pr(1, 1, "aaa"), true},
		{"older pipeline never cancels a newer one", pr(1, 1, "aaa"), pr(2, 2, "bbb"), false},
		{"two pipelines for one commit both run", pr(2, 2, "aaa"), pr(1, 1, "aaa"), false},
		{"the reverse pair also leaves both running", pr(1, 1, "aaa"), pr(2, 2, "aaa"), false},
		{"a restart cancels the older run of its commit", restart, pr(1, 1, "aaa"), true},
		{"an older restart never cancels a newer run", staleRestart, pr(2, 2, "aaa"), false},
		{"itself", pr(1, 1, "aaa"), pr(1, 1, "aaa"), false},
		{"a different PR", other, pr(1, 1, "aaa"), false},
		{"a different event", pr(2, 2, "bbb"), push(1, 1, "aaa", "main"), false},
		{"newer push on the branch", push(2, 2, "bbb", "main"), push(1, 1, "aaa", "main"), true},
		{"push on another branch", push(2, 2, "bbb", "main"), push(1, 1, "aaa", "dev"), false},
		{"a newer metadata edit cancels the older one on the same commit", meta(2, 2, "aaa"), meta(1, 1, "aaa"), true},
		{"an older metadata edit never cancels a newer one", meta(1, 1, "aaa"), meta(2, 2, "aaa"), false},
		{"metadata never cancels a code pipeline on its commit", meta(2, 2, "aaa"), pr(1, 1, "aaa"), false},
		{"metadata on another PR", func() *model.Pipeline { p := meta(2, 2, "aaa"); p.Refspec = "other:main"; return p }(), meta(1, 1, "aaa"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, supersedes(tt.pipeline, tt.prev))
		})
	}
}

func TestCancelStatusWhenNoStepStarted(t *testing.T) {
	setStatusAggregate(t, false)
	setStatusMeta(t, false)
	setStatusPerWorkflow(t, false)

	tests := []struct {
		name      string
		workflows []*model.Workflow
		want      model.StatusValue
	}{
		{
			name: "workflow pending, its only step pending: canceled",
			workflows: []*model.Workflow{
				{ID: 1, State: model.StatusPending, Children: []*model.Step{{ID: 11, State: model.StatusPending}}},
			},
			want: model.StatusCanceled,
		},
		{
			name: "workflow running but no step started: canceled",
			workflows: []*model.Workflow{
				{ID: 1, State: model.StatusRunning, Children: []*model.Step{{ID: 11, State: model.StatusPending}, {ID: 12, State: model.StatusPending}}},
			},
			want: model.StatusCanceled,
		},
		{
			name: "one step running: killed",
			workflows: []*model.Workflow{
				{ID: 1, State: model.StatusRunning, Children: []*model.Step{{ID: 11, State: model.StatusRunning, Started: 1}, {ID: 12, State: model.StatusPending}}},
			},
			want: model.StatusKilled,
		},
		{
			name: "one step finished: killed",
			workflows: []*model.Workflow{
				{ID: 1, State: model.StatusRunning, Children: []*model.Step{{ID: 11, State: model.StatusSuccess, Started: 1, Finished: 2}, {ID: 12, State: model.StatusPending}}},
			},
			want: model.StatusKilled,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline := &model.Pipeline{ID: 7, Number: 7, Event: model.EventPush, Status: model.StatusRunning}
			repo := &model.Repo{Owner: "o", Name: "r", FullName: "o/r"}
			user := &model.User{AccessToken: "x"}

			var stored model.StatusValue
			s := store_mocks.NewMockStore(t)
			s.On("GetPipeline", pipeline.ID).Return(pipeline, nil)
			s.On("WorkflowGetTree", mock.Anything).Return(tt.workflows, nil)
			s.On("WorkflowUpdate", mock.Anything).Return(nil).Maybe()
			s.On("StepUpdate", mock.Anything).Return(nil).Maybe()
			s.On("UpdatePipeline", mock.Anything).Run(func(args mock.Arguments) {
				stored = args.Get(0).(*model.Pipeline).Status
			}).Return(nil)

			q := queue_mocks.NewMockQueue(t)
			q.On("ErrorAtOnce", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
			origScheduler := server.Config.Services.Scheduler
			server.Config.Services.Scheduler = scheduler.NewScheduler(t.Context(), s, q, memory.New())
			t.Cleanup(func() { server.Config.Services.Scheduler = origScheduler })

			err := Cancel(t.Context(), &aggregateResilienceForge{statusErrForWorkflowID: -1}, s, repo, user, pipeline, &model.CancelInfo{})
			require.NoError(t, err)
			assert.Equal(t, tt.want, stored)
		})
	}
}
