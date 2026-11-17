<<<<<<< conflict 1 of 1
%%%%%%% diff from: ouulyvmm f4d3d580 "Update pre-commit hook rbubley/mirrors-prettier to v3.9.10 (#7277)"
\\\\\\\        to: vsrqopkp 79d82d67 "Merge pull request #149 from RigelBuild/ci-pipelines/rig-8574-release-3-17-0-rigel-23"
+// Copyright 2026 Woodpecker Authors
+//
+// Licensed under the Apache License, Version 2.0 (the "License");
+// you may not use this file except in compliance with the License.
+// You may obtain a copy of the License at
+//
+//      http://www.apache.org/licenses/LICENSE-2.0
+//
+// Unless required by applicable law or agreed to in writing, software
+// distributed under the License is distributed on an "AS IS" BASIS,
+// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
+// See the License for the specific language governing permissions and
+// limitations under the License.
+
+package pipeline
+
+import (
+	"testing"
+
+	"github.com/stretchr/testify/assert"
+
+	"go.woodpecker-ci.org/woodpecker/v3/server/model"
+)
+
+func TestSupersedes(t *testing.T) {
+	pr := func(id, number int64, commit string) *model.Pipeline {
+		return &model.Pipeline{ID: id, Number: number, Commit: commit, Event: model.EventPull, Refspec: "feat:main"}
+	}
+	push := func(id, number int64, commit, branch string) *model.Pipeline {
+		return &model.Pipeline{ID: id, Number: number, Commit: commit, Event: model.EventPush, Branch: branch}
+	}
+	other := pr(2, 2, "bbb")
+	restart := pr(3, 3, "aaa")
+	restart.Parent = 1
+	staleRestart := pr(1, 1, "aaa")
+	staleRestart.Parent = 1
+	other.Refspec = "other:main"
+	meta := func(id, number int64, commit string) *model.Pipeline {
+		p := pr(id, number, commit)
+		p.Event = model.EventPullMetadata
+		return p
+	}
+
+	tests := []struct {
+		name           string
+		pipeline, prev *model.Pipeline
+		want           bool
+	}{
+		{"newer push to the PR cancels the older one", pr(2, 2, "bbb"), pr(1, 1, "aaa"), true},
+		{"older pipeline never cancels a newer one", pr(1, 1, "aaa"), pr(2, 2, "bbb"), false},
+		{"two pipelines for one commit both run", pr(2, 2, "aaa"), pr(1, 1, "aaa"), false},
+		{"the reverse pair also leaves both running", pr(1, 1, "aaa"), pr(2, 2, "aaa"), false},
+		{"a restart cancels the older run of its commit", restart, pr(1, 1, "aaa"), true},
+		{"an older restart never cancels a newer run", staleRestart, pr(2, 2, "aaa"), false},
+		{"itself", pr(1, 1, "aaa"), pr(1, 1, "aaa"), false},
+		{"a different PR", other, pr(1, 1, "aaa"), false},
+		{"a different event", pr(2, 2, "bbb"), push(1, 1, "aaa", "main"), false},
+		{"newer push on the branch", push(2, 2, "bbb", "main"), push(1, 1, "aaa", "main"), true},
+		{"push on another branch", push(2, 2, "bbb", "main"), push(1, 1, "aaa", "dev"), false},
+		{"a newer metadata edit cancels the older one on the same commit", meta(2, 2, "aaa"), meta(1, 1, "aaa"), true},
+		{"an older metadata edit never cancels a newer one", meta(1, 1, "aaa"), meta(2, 2, "aaa"), false},
+		{"metadata never cancels a code pipeline on its commit", meta(2, 2, "aaa"), pr(1, 1, "aaa"), false},
+		{"metadata on another PR", func() *model.Pipeline { p := meta(2, 2, "aaa"); p.Refspec = "other:main"; return p }(), meta(1, 1, "aaa"), false},
+	}
+	for _, tt := range tests {
+		t.Run(tt.name, func(t *testing.T) {
+			assert.Equal(t, tt.want, supersedes(tt.pipeline, tt.prev))
+		})
+	}
+}
+++++++ yyprtmsp 1a845577 "Update module gitlab.com/gitlab-org/api/client-go/v3 to v3.47.0 (#7541)"
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

package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	forge_mocks "go.woodpecker-ci.org/woodpecker/v3/server/forge/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	scheduler_mocks "go.woodpecker-ci.org/woodpecker/v3/server/scheduler/mocks"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

// Evicting a running workflow signals its agent, which then reports the
// workflow done and the server kills every step still pending. Cancel must
// mark pending steps canceled before that signal, otherwise its write from
// the stale workflow tree races with the agent and can leave them killed.
func TestCancelMarksPendingStepsBeforeEvictingWorkflows(t *testing.T) {
	repo := &model.Repo{ID: 1}
	user := &model.User{ID: 1}
	running := &model.Pipeline{ID: 1, Status: model.StatusRunning}
	workflow := &model.Workflow{ID: 2, State: model.StatusRunning, Children: []*model.Step{
		{ID: 3, State: model.StatusRunning, Started: 1},
		{ID: 4, State: model.StatusPending},
	}}

	s := store_mocks.NewMockStore(t)
	s.EXPECT().WorkflowGetTree(mock.Anything).Return([]*model.Workflow{workflow}, nil).Twice()
	pendingCanceled := s.EXPECT().StepUpdate(mock.MatchedBy(func(step *model.Step) bool {
		return step.ID == 4 && step.State == model.StatusCanceled
	})).Return(nil).Once()
	s.EXPECT().UpdatePipeline(mock.MatchedBy(func(p *model.Pipeline) bool {
		return p.Status == model.StatusKilled
	})).Return(nil).Once()

	scheduler := scheduler_mocks.NewMockScheduler(t)
	scheduler.EXPECT().CancelWorkflows(mock.Anything, []string{"2"}).Return(nil).Once().NotBefore(pendingCanceled)
	scheduler.EXPECT().PublishPipelineEvent(mock.Anything, repo, mock.Anything).Return(nil).Once()

	forge := forge_mocks.NewMockForge(t)

	previous := server.Config.Services.Scheduler
	server.Config.Services.Scheduler = scheduler
	t.Cleanup(func() { server.Config.Services.Scheduler = previous })

	assert.NoError(t, Cancel(t.Context(), forge, s, repo, user, running, nil))
}

func TestCancelPreviousPipelinesOrdering(t *testing.T) {
	for _, event := range []model.WebhookEvent{model.EventPull, model.EventPush} {
		for _, newestFirst := range []bool{false, true} {
			name := string(event) + "/older handler first"
			if newestFirst {
				name = string(event) + "/newer handler first"
			}
			t.Run(name, func(t *testing.T) {
				repo := &model.Repo{ID: 1, CancelPreviousPipelineEvents: []model.WebhookEvent{event}}
				older := &model.Pipeline{ID: 20, Number: 10, Status: model.StatusPending, Event: event, Branch: "main", Refspec: "feature:main"}
				newer := &model.Pipeline{ID: 21, Number: 11, Status: model.StatusPending, Event: event, Branch: "main", Refspec: "feature:main"}
				otherContext := *older
				otherContext.ID = 18
				otherContext.Number = 8
				otherContext.Branch = "other"
				otherContext.Refspec = "other:main"
				otherEvent := *older
				otherEvent.ID = 19
				otherEvent.Number = 9
				otherEvent.Event = model.EventManual
				s := store_mocks.NewMockStore(t)
				scheduler := scheduler_mocks.NewMockScheduler(t)
				previous := server.Config.Services.Scheduler
				server.Config.Services.Scheduler = scheduler
				t.Cleanup(func() { server.Config.Services.Scheduler = previous })

				// Both handlers can read the same active list before either cancellation is stored.
				s.EXPECT().GetActivePipelineList(repo).Return([]*model.Pipeline{older, newer, &otherContext, &otherEvent}, nil).Twice()
				s.EXPECT().WorkflowGetTree(mock.Anything).Return([]*model.Workflow{}, nil)
				scheduler.EXPECT().CancelWorkflows(mock.Anything, mock.Anything).Return(nil)
				scheduler.EXPECT().PublishPipelineEvent(mock.Anything, repo, mock.Anything).Return(nil)
				var canceled []*model.Pipeline
				s.EXPECT().UpdatePipeline(mock.Anything).Run(func(pipeline *model.Pipeline) {
					canceled = append(canceled, pipeline)
				}).Return(nil)

				order := []*model.Pipeline{older, newer}
				if newestFirst {
					order = []*model.Pipeline{newer, older}
				}
				for _, p := range order {
					require.NoError(t, cancelPreviousPipelines(t.Context(), nil, s, p, repo, nil))
				}
				require.Len(t, canceled, 1)
				assert.Equal(t, older.ID, canceled[0].ID)
				assert.Equal(t, model.StatusCanceled, canceled[0].Status)
				assert.Equal(t, newer.Number, canceled[0].CancelInfo.SupersededBy)
			})
		}
	}
}
>>>>>>> conflict 1 of 1 ends
