// Copyright 2022 Woodpecker Authors
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

package rpc

import (
	"context"
<<<<<<< conflict 1 of 3
+++++++ vsrqopkp 79d82d67 "Merge pull request #149 from RigelBuild/ci-pipelines/rig-8574-release-3-17-0-rigel-23"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
%%%%%%% diff from: ouulyvmm f4d3d580 "Update pre-commit hook rbubley/mirrors-prettier to v3.9.10 (#7277)"
\\\\\\\        to: yyprtmsp 1a845577 "Update module gitlab.com/gitlab-org/api/client-go/v3 to v3.47.0 (#7541)"
+	"errors"
+	"fmt"
>>>>>>> conflict 1 of 3 ends
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	backend_types "go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types" //nolint:depguard // rpc.Workflow.Config is a backend config; tests must build one
	"go.woodpecker-ci.org/woodpecker/v3/rpc"
<<<<<<< conflict 2 of 3
+++++++ vsrqopkp 79d82d67 "Merge pull request #149 from RigelBuild/ci-pipelines/rig-8574-release-3-17-0-rigel-23"
	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
	forge_mocks "go.woodpecker-ci.org/woodpecker/v3/server/forge/mocks"
%%%%%%% diff from: ouulyvmm f4d3d580 "Update pre-commit hook rbubley/mirrors-prettier to v3.9.10 (#7277)"
\\\\\\\        to: yyprtmsp 1a845577 "Update module gitlab.com/gitlab-org/api/client-go/v3 to v3.47.0 (#7541)"
+	"go.woodpecker-ci.org/woodpecker/v3/server"
+	"go.woodpecker-ci.org/woodpecker/v3/server/logging"
+	"go.woodpecker-ci.org/woodpecker/v3/server/metric"
>>>>>>> conflict 2 of 3 ends
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
<<<<<<< conflict 3 of 3
%%%%%%% diff from: ouulyvmm f4d3d580 "Update pre-commit hook rbubley/mirrors-prettier to v3.9.10 (#7277)"
\\\\\\\        to: vsrqopkp 79d82d67 "Merge pull request #149 from RigelBuild/ci-pipelines/rig-8574-release-3-17-0-rigel-23"
+	queue_mocks "go.woodpecker-ci.org/woodpecker/v3/server/queue/mocks"
+	"go.woodpecker-ci.org/woodpecker/v3/server/scheduler"
+	manager_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/mocks"
+++++++ yyprtmsp 1a845577 "Update module gitlab.com/gitlab-org/api/client-go/v3 to v3.47.0 (#7541)"
	"go.woodpecker-ci.org/woodpecker/v3/server/pubsub/memory"
	"go.woodpecker-ci.org/woodpecker/v3/server/queue"
	queue_mocks "go.woodpecker-ci.org/woodpecker/v3/server/queue/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/scheduler"
	scheduler_mocks "go.woodpecker-ci.org/woodpecker/v3/server/scheduler/mocks"
	log_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/log/mocks"
>>>>>>> conflict 3 of 3 ends
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

type readOnlyNetrcForge struct {
	forge.Forge
	readOnlyNetrc func(context.Context, *model.Repo) (*model.Netrc, error)
}

func (f *readOnlyNetrcForge) ReadOnlyNetrc(ctx context.Context, repo *model.Repo) (*model.Netrc, error) {
	return f.readOnlyNetrc(ctx, repo)
}

func setForgeForRepo(t *testing.T, repo *model.Repo, forgeClient forge.Forge) *manager_mocks.MockManager {
	t.Helper()
	manager := manager_mocks.NewMockManager(t)
	manager.On("ForgeFromRepo", repo).Return(forgeClient, nil).Maybe()
	previousManager := server.Config.Services.Manager
	server.Config.Services.Manager = manager
	t.Cleanup(func() { server.Config.Services.Manager = previousManager })
	return manager
}

func setForgeManagerForRepo(t *testing.T) *manager_mocks.MockManager {
	t.Helper()
	manager := manager_mocks.NewMockManager(t)
	previousManager := server.Config.Services.Manager
	server.Config.Services.Manager = manager
	t.Cleanup(func() { server.Config.Services.Manager = previousManager })
	return manager
}

func setReadOnlyNetrcForge(t *testing.T, repo *model.Repo, readOnlyNetrc func(context.Context, *model.Repo) (*model.Netrc, error)) {
	t.Helper()
	setForgeForRepo(t, repo, &readOnlyNetrcForge{readOnlyNetrc: readOnlyNetrc})
}

func TestUsableFor(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		remaining time.Duration
		want      bool
	}{
		{name: "fresh token", remaining: 59*time.Minute + 59*time.Second, want: true},
		{name: "exact minimum", remaining: 10 * time.Minute, want: true},
		{name: "below minimum", remaining: 10*time.Minute - time.Second},
		{name: "expired", remaining: -time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, usableFor(now.Add(tt.remaining), now))
		})
	}
}

func TestAddNetrcMaskPreservesSameNameUserSecret(t *testing.T) {
	userSecret := &backend_types.Secret{Name: "__WOODPECKER_READ_ONLY_NETRC__", Value: "user-secret"}
	workflow := &rpc.Workflow{Config: &backend_types.Config{Secrets: []*backend_types.Secret{userSecret}}}
	pipeline.AddNetrcMask(workflow, "dispatch-token")
	stripNetrc(workflow)
	require.Len(t, workflow.Config.Secrets, 2)
	assert.Same(t, userSecret, workflow.Config.Secrets[0])
}

func TestInjectNetrc(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	repo := &model.Repo{ID: 10, ForgeID: 5, Timeout: 55, IsSCMPrivate: true}
	makeWorkflow := func() *rpc.Workflow {
		return &rpc.Workflow{Timeout: 55, Config: &backend_types.Config{
			Secrets: []*backend_types.Secret{
				{Name: "__WOODPECKER_READ_ONLY_NETRC__", Value: "user-secret"},
				{Name: "unrelated-secret", Value: "keep-masking"},
			},
			Stages: []*backend_types.Stage{{Steps: []*backend_types.Step{
				{Name: "clone", Environment: map[string]string{"CI_NETRC_MACHINE": "github.com", "CI_NETRC_USERNAME": "x-access-token", "CI_NETRC_PASSWORD": "approver-token", "CI_NETRC_EXTRA": "stale"}},
				{Name: "step", Environment: map[string]string{"CI_NETRC_MACHINE": "github.com", "CI_NETRC_USERNAME": "x-access-token", "CI_NETRC_PASSWORD": "approver-token", "CI_NETRC_EXTRA": "stale"}},
			}}},
		}}
	}

	t.Run("injects a fresh token in clone and every step", func(t *testing.T) {
		setReadOnlyNetrcForge(t, repo, func(_ context.Context, _ *model.Repo) (*model.Netrc, error) {
			return &model.Netrc{Machine: "github.com", Login: "x-access-token", Password: "dispatch-token", ExpiresAt: now.Add(59 * time.Minute)}, nil
		})
		workflow := makeWorkflow()
		s := RPC{now: func() time.Time { return now }}
		require.NoError(t, s.injectNetrc(t.Context(), workflow, repo))
		assert.Equal(t, "dispatch-token", workflow.Config.Stages[0].Steps[0].Environment["CI_NETRC_PASSWORD"])
		assert.Equal(t, "x-access-token", workflow.Config.Stages[0].Steps[0].Environment["CI_NETRC_USERNAME"])
		assert.NotContains(t, workflow.Config.Stages[0].Steps[0].Environment, "approver-token")
		require.Len(t, workflow.Config.Secrets, 3)
		assert.Equal(t, "__WOODPECKER_READ_ONLY_NETRC__", workflow.Config.Secrets[0].Name)
		assert.Equal(t, "user-secret", workflow.Config.Secrets[0].Value)
		assert.Equal(t, "unrelated-secret", workflow.Config.Secrets[1].Name)
		assert.Equal(t, "keep-masking", workflow.Config.Secrets[1].Value)
		assert.Equal(t, "__WOODPECKER_READ_ONLY_NETRC__", workflow.Config.Secrets[2].Name)
		assert.Equal(t, "dispatch-token", workflow.Config.Secrets[2].Value)
		stripNetrc(workflow)
		require.Len(t, workflow.Config.Secrets, 3)
	})

	t.Run("strips the token when it expires too soon", func(t *testing.T) {
		setReadOnlyNetrcForge(t, repo, func(_ context.Context, _ *model.Repo) (*model.Netrc, error) {
			return &model.Netrc{Machine: "github.com", Login: "x-access-token", Password: "new-token", ExpiresAt: now.Add(9 * time.Minute)}, nil
		})
		workflow := makeWorkflow()
		s := RPC{now: func() time.Time { return now }}
		require.NoError(t, s.injectNetrc(t.Context(), workflow, repo))
		assertNoNetrc(t, workflow)
	})

	// A 60-minute repo timeout with a fresh one-hour App token must still clone.
	t.Run("injects a fresh token for a 60 minute workflow", func(t *testing.T) {
		setReadOnlyNetrcForge(t, repo, func(_ context.Context, _ *model.Repo) (*model.Netrc, error) {
			return &model.Netrc{Machine: "github.com", Login: "x-access-token", Password: "hour-token", ExpiresAt: now.Add(59*time.Minute + 59*time.Second)}, nil
		})
		workflow := makeWorkflow()
		workflow.Timeout = 60
		s := RPC{now: func() time.Time { return now }}
		require.NoError(t, s.injectNetrc(t.Context(), workflow, repo))
		assert.Equal(t, "hour-token", workflow.Config.Stages[0].Steps[0].Environment["CI_NETRC_PASSWORD"])
	})

	t.Run("mint failure strips existing netrc values", func(t *testing.T) {
		setReadOnlyNetrcForge(t, repo, func(_ context.Context, _ *model.Repo) (*model.Netrc, error) { return nil, assert.AnError })
		workflow := makeWorkflow()
		s := RPC{now: func() time.Time { return now }}
		require.Error(t, s.injectNetrc(t.Context(), workflow, repo))
		assertNoNetrc(t, workflow)
	})

	t.Run("incomplete netrc strips existing netrc values", func(t *testing.T) {
		setReadOnlyNetrcForge(t, repo, func(_ context.Context, _ *model.Repo) (*model.Netrc, error) {
			return &model.Netrc{Machine: "github.com", Login: "x-access-token", ExpiresAt: now.Add(time.Hour)}, nil
		})
		workflow := makeWorkflow()
		s := RPC{now: func() time.Time { return now }}
		require.Error(t, s.injectNetrc(t.Context(), workflow, repo))
		assertNoNetrc(t, workflow)
	})

	t.Run("nil netrc strips existing netrc values", func(t *testing.T) {
		setReadOnlyNetrcForge(t, repo, func(_ context.Context, _ *model.Repo) (*model.Netrc, error) { return nil, nil })
		workflow := makeWorkflow()
		s := RPC{now: func() time.Time { return now }}
		require.Error(t, s.injectNetrc(t.Context(), workflow, repo))
		assertNoNetrc(t, workflow)
	})

	t.Run("expired token strips existing netrc values", func(t *testing.T) {
		setReadOnlyNetrcForge(t, repo, func(_ context.Context, _ *model.Repo) (*model.Netrc, error) {
			return &model.Netrc{Machine: "github.com", Login: "x-access-token", Password: "expired", ExpiresAt: now.Add(-time.Second)}, nil
		})
		workflow := makeWorkflow()
		s := RPC{now: func() time.Time { return now }}
		require.NoError(t, s.injectNetrc(t.Context(), workflow, repo))
		assertNoNetrc(t, workflow)
	})
	t.Run("bounded mint failure strips stale netrc", func(t *testing.T) {
		setReadOnlyNetrcForge(t, repo, func(ctx context.Context, _ *model.Repo) (*model.Netrc, error) {
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			assert.LessOrEqual(t, time.Until(deadline), forge.ReadOnlyNetrcMintTimeout)
			return nil, context.DeadlineExceeded
		})
		workflow := makeWorkflow()
		s := RPC{now: func() time.Time { return now }}
		require.ErrorIs(t, s.injectNetrc(t.Context(), workflow, repo), context.DeadlineExceeded)
		assertNoNetrc(t, workflow)
	})
	t.Run("public repositories skip minting by default", func(t *testing.T) {
		previous := server.Config.Pipeline.AuthenticatePublicRepos
		server.Config.Pipeline.AuthenticatePublicRepos = false
		t.Cleanup(func() { server.Config.Pipeline.AuthenticatePublicRepos = previous })
		publicRepo := *repo
		publicRepo.IsSCMPrivate = false
		workflow := makeWorkflow()
		s := RPC{now: func() time.Time { return now }}
		require.NoError(t, s.injectNetrc(t.Context(), workflow, &publicRepo))
		assertNoNetrc(t, workflow)
	})
	t.Run("public repository authentication honors setting", func(t *testing.T) {
		previous := server.Config.Pipeline.AuthenticatePublicRepos
		server.Config.Pipeline.AuthenticatePublicRepos = true
		t.Cleanup(func() { server.Config.Pipeline.AuthenticatePublicRepos = previous })
		publicRepo := *repo
		publicRepo.IsSCMPrivate = false
		setReadOnlyNetrcForge(t, &publicRepo, func(_ context.Context, _ *model.Repo) (*model.Netrc, error) {
			return &model.Netrc{Machine: "github.com", Login: "x-access-token", Password: "public-token", ExpiresAt: now.Add(time.Hour)}, nil
		})
		workflow := makeWorkflow()
		s := RPC{now: func() time.Time { return now }}
		require.NoError(t, s.injectNetrc(t.Context(), workflow, &publicRepo))
		assert.Equal(t, "public-token", workflow.Config.Stages[0].Steps[0].Environment["CI_NETRC_PASSWORD"])
	})
	t.Run("each dispatch mints its own token", func(t *testing.T) {
		mints := 0
		setReadOnlyNetrcForge(t, repo, func(_ context.Context, _ *model.Repo) (*model.Netrc, error) {
			mints++
			return &model.Netrc{Machine: "github.com", Login: "x-access-token", Password: fmt.Sprintf("token-%d", mints), ExpiresAt: now.Add(time.Hour)}, nil
		})
		s := RPC{now: func() time.Time { return now }}
		first, second := makeWorkflow(), makeWorkflow()
		require.NoError(t, s.injectNetrc(t.Context(), first, repo))
		require.NoError(t, s.injectNetrc(t.Context(), second, repo))
		assert.Equal(t, "token-1", first.Config.Stages[0].Steps[0].Environment["CI_NETRC_PASSWORD"])
		assert.Equal(t, "token-2", second.Config.Stages[0].Steps[0].Environment["CI_NETRC_PASSWORD"])
	})
}

func assertNoNetrc(t *testing.T, workflow *rpc.Workflow) {
	t.Helper()
	for _, stage := range workflow.Config.Stages {
		for _, step := range stage.Steps {
			for name := range step.Environment {
				assert.False(t, strings.HasPrefix(name, "CI_NETRC_"), "%s still has %s", step.Name, name)
			}
		}
	}
}

func TestRPCNextInjectsReadOnlyNetrc(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	repo := &model.Repo{ID: 10, ForgeID: 5, Timeout: 55, IsSCMPrivate: true, FullName: "test-org/test-repo"}
	currentPipeline := &model.Pipeline{ID: 20, RepoID: repo.ID}
	config := &backend_types.Config{Stages: []*backend_types.Stage{{Steps: []*backend_types.Step{{
		Name:        "custom clone",
		Environment: map[string]string{"CI_NETRC_MACHINE": "github.com", "CI_NETRC_USERNAME": "x-access-token", "CI_NETRC_PASSWORD": "approver-token"},
	}}}}}
	data, err := json.Marshal(rpc.Workflow{ID: "30", Config: config, Timeout: 55})
	require.NoError(t, err)
	queue := queue_mocks.NewMockQueue(t)
	queue.On("Poll", mock.Anything, int64(1), mock.Anything).Return(&model.Task{ID: "30", Data: data}, nil).Once()
	mockStore := store_mocks.NewMockStore(t)
	mockStore.On("AgentFind", int64(1)).Return(defaultAgent(), nil)
	mockStore.On("WorkflowUpdate", mock.Anything).Return(nil)
	mockStore.On("WorkflowLoad", int64(30)).Return(&model.Workflow{ID: 30, PipelineID: currentPipeline.ID}, nil)
	mockStore.On("GetPipeline", currentPipeline.ID).Return(currentPipeline, nil)
	mockStore.On("GetRepo", repo.ID).Return(repo, nil)
	setReadOnlyNetrcForge(t, repo, func(_ context.Context, _ *model.Repo) (*model.Netrc, error) {
		return &model.Netrc{Machine: "github.com", Login: "x-access-token", Password: "dispatch-token", ExpiresAt: now.Add(59 * time.Minute)}, nil
	})
	s := newTestRPC(t, mockStore, queue)
	s.now = func() time.Time { return now }
	ctx := context.WithValue(t.Context(), agentIDKey, int64(1))
	workflow, err := s.Next(ctx, rpc.Filter{Labels: map[string]string{}})
	require.NoError(t, err)
	require.NotNil(t, workflow)
	assert.Equal(t, int64(55), workflow.Timeout)
	assert.Equal(t, "dispatch-token", workflow.Config.Stages[0].Steps[0].Environment["CI_NETRC_PASSWORD"])
	require.Len(t, workflow.Config.Secrets, 1)
	assert.Equal(t, "__WOODPECKER_READ_ONLY_NETRC__", workflow.Config.Secrets[0].Name)
	assert.Equal(t, "dispatch-token", workflow.Config.Secrets[0].Value)
}

func TestRPCNextFilterMatchesAgentPollLabels(t *testing.T) {
	agent := &model.Agent{
		ID: 1, Name: "test-agent", OrgID: model.IDNotSet, Platform: "linux/amd64", Backend: "docker",
		CustomLabels: map[string]string{"type": "gpu", "pool": "a"},
		Filters:      map[string]string{"pool": "b"},
	}
	// The labels the agent's run loop sends: defaults, then its custom labels.
	sent := map[string]string{"hostname": "builder-1", "platform": "linux/amd64", "backend": "docker", "repo": "*"}
	maps.Copy(sent, agent.CustomLabels)

	var nextFilter func(*model.Task) (bool, int)
	queue := queue_mocks.NewMockQueue(t)
	queue.On("Poll", mock.Anything, int64(1), mock.Anything).Run(func(args mock.Arguments) {
		filter, ok := args.Get(2).(func(*model.Task) (bool, int))
		require.True(t, ok, "queue poll filter type")
		nextFilter = filter
	}).Return(nil, nil).Once()
	mockStore := store_mocks.NewMockStore(t)
	mockStore.On("AgentFind", int64(1)).Return(agent, nil)
	s := newTestRPC(t, mockStore, queue)
	ctx := context.WithValue(t.Context(), agentIDKey, int64(1))
	workflow, err := s.Next(ctx, rpc.Filter{Labels: sent})
	require.NoError(t, err)
	require.Nil(t, workflow)
	require.NotNil(t, nextFilter)

	labels, err := agent.PollLabels()
	require.NoError(t, err)
	pollFilter := scheduler.NewLabelFilter(labels)
	tasks := []*model.Task{
		{ID: "1", Labels: map[string]string{"type": "gpu", "platform": "linux/amd64"}},
		{ID: "2", Labels: map[string]string{"type": "gpu", "pool": "a"}},
		{ID: "3", Labels: map[string]string{"type": "gpu", "pool": "b", "repo": "owner/repo", "org-id": "5"}},
		{ID: "4", Labels: map[string]string{"type": "linux"}},
		{ID: "5", Labels: map[string]string{"backend": "kubernetes"}},
		{ID: "6", Labels: map[string]string{"backend": "docker", "woodpecker-ci.org/repo-id": "9"}},
	}
	matchedAny := false
	for _, task := range tasks {
		wantMatched, wantScore := nextFilter(task)
		gotMatched, gotScore := pollFilter(task)
		assert.Equal(t, wantMatched, gotMatched, "task %s matched", task.ID)
		assert.Equal(t, wantScore, gotScore, "task %s score", task.ID)
		matchedAny = matchedAny || gotMatched
	}
	assert.True(t, matchedAny, "the task set includes a match")
}

func TestRPCNextStripsNetrcOnFailures(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "workflow lookup fails"},
		{name: "pipeline lookup fails"},
		{name: "repo lookup fails"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &model.Repo{ID: 10, ForgeID: 5, Timeout: 55, IsSCMPrivate: true, FullName: "test-org/test-repo"}
			workflowData, err := json.Marshal(rpc.Workflow{
				ID:      "30",
				Timeout: 55,
				Config: &backend_types.Config{Stages: []*backend_types.Stage{{Steps: []*backend_types.Step{{
					Name: "clone",
					Environment: map[string]string{
						"CI_NETRC_MACHINE":  "github.com",
						"CI_NETRC_USERNAME": "x-access-token",
						"CI_NETRC_PASSWORD": "stale-password",
					},
				}}}}},
			})
			require.NoError(t, err)

			queue := queue_mocks.NewMockQueue(t)
			queue.On("Poll", mock.Anything, int64(1), mock.Anything).Return(&model.Task{ID: "30", Data: workflowData}, nil).Once()
			mockStore := store_mocks.NewMockStore(t)
			mockStore.On("AgentFind", int64(1)).Return(defaultAgent(), nil)
			mockStore.On("WorkflowUpdate", mock.Anything).Return(nil)
			switch tt.name {
			case "workflow lookup fails":
				mockStore.On("WorkflowLoad", int64(30)).Return(&model.Workflow{ID: 30, PipelineID: 20}, nil).Once()
				mockStore.On("WorkflowLoad", int64(30)).Return((*model.Workflow)(nil), assert.AnError).Once()
			case "pipeline lookup fails":
				mockStore.On("WorkflowLoad", int64(30)).Return(&model.Workflow{ID: 30, PipelineID: 20}, nil).Twice()
				mockStore.On("GetPipeline", int64(20)).Return((*model.Pipeline)(nil), assert.AnError).Once()
			case "repo lookup fails":
				mockStore.On("WorkflowLoad", int64(30)).Return(&model.Workflow{ID: 30, PipelineID: 20}, nil).Twice()
				mockStore.On("GetPipeline", int64(20)).Return(&model.Pipeline{ID: 20, RepoID: repo.ID}, nil).Once()
				mockStore.On("GetRepo", repo.ID).Return((*model.Repo)(nil), assert.AnError).Once()
			}

			s := newTestRPC(t, mockStore, queue)
			ctx := context.WithValue(t.Context(), agentIDKey, int64(1))
			workflow, err := s.Next(ctx, rpc.Filter{Labels: map[string]string{}})
			require.NoError(t, err)
			require.NotNil(t, workflow)
			assertNoNetrc(t, workflow)
		})
	}
}

func setupNetrcNext(t *testing.T, repo *model.Repo, mint func(context.Context, *model.Repo) (*model.Netrc, error)) (*RPC, context.Context, *store_mocks.MockStore, *queue_mocks.MockQueue) {
	return setupNetrcNextWithSteps(t, repo, mint, nil)
}

func setupNetrcNextWithSteps(t *testing.T, repo *model.Repo, mint func(context.Context, *model.Repo) (*model.Netrc, error), steps []*model.Step) (*RPC, context.Context, *store_mocks.MockStore, *queue_mocks.MockQueue) {
	t.Helper()
	workflowData, err := json.Marshal(rpc.Workflow{
		ID:      "30",
		Timeout: 55,
		Config: &backend_types.Config{Stages: []*backend_types.Stage{{Steps: []*backend_types.Step{{
			Name: "clone",
			Environment: map[string]string{
				"CI_NETRC_MACHINE":  "github.com",
				"CI_NETRC_USERNAME": "x-access-token",
				"CI_NETRC_PASSWORD": "stale-password",
			},
		}}}}},
	})
	require.NoError(t, err)

	queue := queue_mocks.NewMockQueue(t)
	queue.On("Poll", mock.Anything, int64(1), mock.Anything).Return(&model.Task{ID: "30", Data: workflowData}, nil).Once()
	mockStore := store_mocks.NewMockStore(t)
	mockStore.On("AgentFind", int64(1)).Return(defaultAgent(), nil)
	workflow := &model.Workflow{ID: 30, PipelineID: 20, State: model.StatusPending, Children: steps}
	mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
	mockStore.On("WorkflowUpdate", mock.Anything).Maybe().Return(nil)
	mockStore.On("StepListFromWorkflowFind", mock.Anything).Maybe().Return(steps, nil)
	currentPipeline := &model.Pipeline{ID: 20, RepoID: repo.ID, Status: model.StatusRunning, Started: 100}
	mockStore.On("GetPipeline", int64(20)).Return(currentPipeline, nil)
	mockStore.On("GetRepo", repo.ID).Return(repo, nil)
	mockStore.On("WorkflowGetTree", currentPipeline).Maybe().Return([]*model.Workflow{{ID: 30, State: model.StatusFailure, Error: "mint failed"}}, nil)
	mockStore.On("UpdatePipeline", mock.Anything).Maybe().Return(nil)
	mockStore.On("GetUser", mock.Anything).Maybe().Return(nil, errors.New("user not found"))
	mockStore.On("AgentUpdate", mock.Anything).Maybe().Return(nil)
	queue.On("Error", mock.Anything, "30", mock.Anything).Maybe().Return(nil)

	if mint != nil {
		setReadOnlyNetrcForge(t, repo, mint)
	}
	s := newTestRPC(t, mockStore, queue)
	s.now = func() time.Time { return time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC) }
	return &s, context.WithValue(t.Context(), agentIDKey, int64(1)), mockStore, queue
}

func TestRPCNextFailsAfterReadOnlyNetrcMintRetries(t *testing.T) {
	repo := &model.Repo{ID: 10, ForgeID: 5, Timeout: 55, IsSCMPrivate: true, FullName: "test-org/test-repo"}
	mintError := errors.New("mint failed")
	mints := 0
	s, ctx, mockStore, queue := setupNetrcNext(t, repo, func(_ context.Context, _ *model.Repo) (*model.Netrc, error) {
		mints++
		return nil, mintError
	})
	waits := make([]time.Duration, 0, 2)
	s.wait = func(_ context.Context, delay time.Duration) error { waits = append(waits, delay); return nil }

	workflow, err := s.Next(ctx, rpc.Filter{Labels: map[string]string{}})
	require.NoError(t, err)
	assert.Nil(t, workflow)
	assert.Equal(t, 3, mints)
	assert.Equal(t, []time.Duration{time.Second, 2 * time.Second}, waits)
	mockStore.AssertCalled(t, "WorkflowUpdate", mock.MatchedBy(func(workflow *model.Workflow) bool {
		return workflow.ID == 30 && workflow.Error == "could not mint read-only netrc for test-org/test-repo: mint failed"
	}))
	queue.AssertCalled(t, "Error", mock.Anything, "30", mock.MatchedBy(func(err error) bool {
		return strings.Contains(err.Error(), repo.FullName) && strings.Contains(err.Error(), mintError.Error())
	}))
}

func TestRPCNextDoesNotFailWorkflowWhenMintContextCanceled(t *testing.T) {
	repo := &model.Repo{ID: 10, ForgeID: 5, Timeout: 55, IsSCMPrivate: true, FullName: "test-org/test-repo"}
	s, baseCtx, mockStore, queue := setupNetrcNext(t, repo, nil)
	ctx, cancel := context.WithCancelCause(baseCtx)
	defer cancel(nil)
	mints := 0
	waits := 0
	setReadOnlyNetrcForge(t, repo, func(context.Context, *model.Repo) (*model.Netrc, error) {
		mints++
		cancel(context.Canceled)
		return nil, errors.New("mint failed")
	})
	s.wait = func(context.Context, time.Duration) error { waits++; return nil }
	workflow, err := s.Next(ctx, rpc.Filter{Labels: map[string]string{}})
	assert.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, workflow)
	assert.Equal(t, 1, mints)
	assert.Zero(t, waits)
	mockStore.AssertNotCalled(t, "StepListFromWorkflowFind", mock.Anything)
	mockStore.AssertNotCalled(t, "StepUpdate", mock.Anything)
	mockStore.AssertNotCalled(t, "WorkflowUpdate", mock.MatchedBy(func(saved *model.Workflow) bool {
		return saved.State == model.StatusFailure || saved.Error != ""
	}))
	queue.AssertNotCalled(t, "Error", mock.Anything, "30", mock.Anything)
}

func TestRPCNextDoesNotFailWorkflowWhenRetryWaitCanceled(t *testing.T) {
	repo := &model.Repo{ID: 10, ForgeID: 5, Timeout: 55, IsSCMPrivate: true, FullName: "test-org/test-repo"}
	mints := 0
	s, baseCtx, mockStore, queue := setupNetrcNext(t, repo, func(context.Context, *model.Repo) (*model.Netrc, error) {
		mints++
		return nil, errors.New("mint failed")
	})
	ctx, cancel := context.WithCancelCause(baseCtx)
	defer cancel(nil)
	waits := 0
	s.wait = func(context.Context, time.Duration) error {
		waits++
		cancel(context.Canceled)
		return nil
	}
	workflow, err := s.Next(ctx, rpc.Filter{Labels: map[string]string{}})
	assert.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, workflow)
	assert.Equal(t, 1, mints)
	assert.Equal(t, 1, waits)
	mockStore.AssertNotCalled(t, "StepListFromWorkflowFind", mock.Anything)
	mockStore.AssertNotCalled(t, "WorkflowUpdate", mock.MatchedBy(func(saved *model.Workflow) bool {
		return saved.State == model.StatusFailure || saved.Error != ""
	}))
	queue.AssertNotCalled(t, "Error", mock.Anything, "30", mock.Anything)
}

func TestRPCNextFailsWorkflowAndPersistsPendingChild(t *testing.T) {
	repo := &model.Repo{ID: 10, ForgeID: 5, Timeout: 55, IsSCMPrivate: true, FullName: "test-org/test-repo"}
	mintError := errors.New("mint failed")
	child := &model.Step{ID: 41, PipelineID: 20, State: model.StatusPending, Name: "clone"}
	s, ctx, mockStore, _ := setupNetrcNextWithSteps(t, repo, func(context.Context, *model.Repo) (*model.Netrc, error) {
		return nil, mintError
	}, []*model.Step{child})
	s.wait = func(context.Context, time.Duration) error { return nil }
	mockStore.On("StepUpdate", mock.MatchedBy(func(step *model.Step) bool {
		return step.ID == child.ID && step.State == model.StatusKilled
	})).Once().Return(nil)
	workflow, err := s.Next(ctx, rpc.Filter{Labels: map[string]string{}})
	require.NoError(t, err)
	assert.Nil(t, workflow)
	mockStore.AssertCalled(t, "StepUpdate", mock.MatchedBy(func(step *model.Step) bool {
		return step.ID == child.ID && step.State == model.StatusKilled
	}))
	mockStore.AssertCalled(t, "WorkflowUpdate", mock.MatchedBy(func(workflow *model.Workflow) bool {
		return workflow.ID == 30 && workflow.State == model.StatusFailure && strings.Contains(workflow.Error, mintError.Error())
	}))
}

func TestRPCNextRetriesReadOnlyNetrcMint(t *testing.T) {
	repo := &model.Repo{ID: 10, ForgeID: 5, Timeout: 55, IsSCMPrivate: true, FullName: "test-org/test-repo"}
	mints := 0
	s, ctx, _, queue := setupNetrcNext(t, repo, func(_ context.Context, _ *model.Repo) (*model.Netrc, error) {
		mints++
		if mints == 1 {
			return nil, errors.New("temporary mint failure")
		}
		return &model.Netrc{Machine: "github.com", Login: "x-access-token", Password: "dispatch-token", ExpiresAt: time.Now().Add(time.Hour)}, nil
	})
	s.wait = func(context.Context, time.Duration) error { return nil }
	queue.On("Error", mock.Anything, "30", mock.Anything).Maybe().Return(nil)
	workflow, err := s.Next(ctx, rpc.Filter{Labels: map[string]string{}})
	require.NoError(t, err)
	require.NotNil(t, workflow)
	assert.Equal(t, 2, mints)
	assert.Equal(t, "dispatch-token", workflow.Config.Stages[0].Steps[0].Environment["CI_NETRC_PASSWORD"])
}

func TestRPCNextFailsForUnusableReadOnlyNetrc(t *testing.T) {
	tests := []struct {
		name           string
		mint           func(context.Context, *model.Repo) (*model.Netrc, error)
		forgeErr       error
		unsupported    bool
		wantMints      int
		wantForgeCalls int
	}{
		{name: "forge lacks read-only netrc", unsupported: true, wantForgeCalls: 1},
		{name: "forge lookup fails", forgeErr: errors.New("forge lookup failed"), wantForgeCalls: 1},
		{name: "nil netrc", mint: func(context.Context, *model.Repo) (*model.Netrc, error) { return nil, nil }, wantMints: 1, wantForgeCalls: 1},
		{name: "incomplete netrc", mint: func(context.Context, *model.Repo) (*model.Netrc, error) {
			return &model.Netrc{Machine: "github.com", Login: "x-access-token", ExpiresAt: time.Now().Add(time.Hour)}, nil
		}, wantMints: 1, wantForgeCalls: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &model.Repo{ID: 10, ForgeID: 5, Timeout: 55, IsSCMPrivate: true, FullName: "test-org/test-repo"}
			mints := 0
			waits := 0
			forgeCalls := 0
			switch {
			case tt.unsupported:
				unsupported := forge_mocks.NewMockForge(t)
				unsupported.On("Name").Return("unsupported")
				manager := setForgeManagerForRepo(t)
				manager.On("ForgeFromRepo", repo).Run(func(mock.Arguments) { forgeCalls++ }).Return(unsupported, nil).Once()
			case tt.forgeErr != nil:
				manager := setForgeManagerForRepo(t)
				manager.On("ForgeFromRepo", repo).Run(func(mock.Arguments) { forgeCalls++ }).Return(nil, tt.forgeErr).Once()
			default:
				forgeClient := &readOnlyNetrcForge{readOnlyNetrc: func(ctx context.Context, repo *model.Repo) (*model.Netrc, error) {
					mints++
					return tt.mint(ctx, repo)
				}}
				manager := setForgeManagerForRepo(t)
				manager.On("ForgeFromRepo", repo).Run(func(mock.Arguments) { forgeCalls++ }).Return(forgeClient, nil).Once()
			}
			s, ctx, mockStore, queue := setupNetrcNext(t, repo, nil)
			s.wait = func(context.Context, time.Duration) error { waits++; return nil }
			workflow, err := s.Next(ctx, rpc.Filter{Labels: map[string]string{}})
			require.NoError(t, err)
			assert.Nil(t, workflow)
			assert.Equal(t, tt.wantMints, mints)
			assert.Equal(t, tt.wantForgeCalls, forgeCalls)
			assert.Zero(t, waits)
			queue.AssertCalled(t, "Error", mock.Anything, "30", mock.Anything)
			mockStore.AssertCalled(t, "WorkflowUpdate", mock.MatchedBy(func(workflow *model.Workflow) bool {
				return workflow.ID == 30 && workflow.Error != ""
			}))
		})
	}
}

func TestRPCNextSkipsMintForPublicRepoByDefault(t *testing.T) {
	previous := server.Config.Pipeline.AuthenticatePublicRepos
	server.Config.Pipeline.AuthenticatePublicRepos = false
	t.Cleanup(func() { server.Config.Pipeline.AuthenticatePublicRepos = previous })
	repo := &model.Repo{ID: 10, ForgeID: 5, Timeout: 55, FullName: "test-org/test-repo"}
	mints := 0
	setReadOnlyNetrcForge(t, repo, func(context.Context, *model.Repo) (*model.Netrc, error) {
		mints++
		return nil, errors.New("must not mint for public repo")
	})
	s, ctx, _, _ := setupNetrcNext(t, repo, nil)
	workflow, err := s.Next(ctx, rpc.Filter{Labels: map[string]string{}})
	require.NoError(t, err)
	require.NotNil(t, workflow)
	assert.Zero(t, mints)
	assertNoNetrc(t, workflow)
}

func TestRegisterAgent(t *testing.T) {
	t.Run("When existing agent Name is empty it should update Name with hostname from metadata", func(t *testing.T) {
		store := store_mocks.NewMockStore(t)
		storeAgent := new(model.Agent)
		storeAgent.ID = 1337
		updatedAgent := model.Agent{
			ID:          1337,
			Created:     0,
			Updated:     0,
			Name:        "hostname",
			OwnerID:     0,
			Token:       "",
			LastContact: 0,
			Platform:    "platform",
			Backend:     "backend",
			Capacity:    2,
			Version:     "version",
			NoSchedule:  false,
		}

		store.On("AgentFind", int64(1337)).Once().Return(storeAgent, nil)
		store.On("AgentUpdate", &updatedAgent).Once().Return(nil)
		grpc := RPC{
			store: store,
		}
		ctx := metadata.NewIncomingContext(
			t.Context(),
			metadata.Pairs("hostname", "hostname"),
		)
		ctx = context.WithValue(ctx, agentIDKey, int64(1337))
		agentID, err := grpc.RegisterAgent(ctx, rpc.AgentInfo{
			Version:  "version",
			Platform: "platform",
			Backend:  "backend",
			Capacity: 2,
		})
		require.NoError(t, err)

		assert.EqualValues(t, 1337, agentID)
	})

	t.Run("When existing agent hostname is present it should not update the hostname", func(t *testing.T) {
		store := store_mocks.NewMockStore(t)
		storeAgent := new(model.Agent)
		storeAgent.ID = 1337
		storeAgent.Name = "originalHostname"
		updatedAgent := model.Agent{
			ID:          1337,
			Created:     0,
			Updated:     0,
			Name:        "originalHostname",
			OwnerID:     0,
			Token:       "",
			LastContact: 0,
			Platform:    "platform",
			Backend:     "backend",
			Capacity:    2,
			Version:     "version",
			NoSchedule:  false,
		}

		store.On("AgentFind", int64(1337)).Once().Return(storeAgent, nil)
		store.On("AgentUpdate", &updatedAgent).Once().Return(nil)
		grpc := RPC{
			store: store,
		}
		ctx := metadata.NewIncomingContext(
			t.Context(),
			metadata.Pairs("hostname", "newHostname"),
		)
		ctx = context.WithValue(ctx, agentIDKey, int64(1337))
		agentID, err := grpc.RegisterAgent(ctx, rpc.AgentInfo{
			Version:  "version",
			Platform: "platform",
			Backend:  "backend",
			Capacity: 2,
		})
		require.NoError(t, err)

		assert.EqualValues(t, 1337, agentID)
	})

	t.Run("When the name is empty it uses the hostname from the request body over metadata", func(t *testing.T) {
		store := store_mocks.NewMockStore(t)
		store.On("AgentFind", int64(1337)).Once().Return(&model.Agent{ID: 1337}, nil)
		store.On("AgentUpdate", mock.MatchedBy(func(a *model.Agent) bool { return a.Name == "body-host" })).Once().Return(nil)
		grpc := RPC{store: store}
		ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("hostname", "metadata-host"))
		ctx = context.WithValue(ctx, agentIDKey, int64(1337))

		_, err := grpc.RegisterAgent(ctx, rpc.AgentInfo{Hostname: "body-host", Platform: "platform"})
		require.NoError(t, err)
	})

	t.Run("When the name is empty and no hostname is sent anywhere the name stays empty", func(t *testing.T) {
		store := store_mocks.NewMockStore(t)
		store.On("AgentFind", int64(1337)).Once().Return(&model.Agent{ID: 1337}, nil)
		store.On("AgentUpdate", mock.MatchedBy(func(a *model.Agent) bool { return a.Name == "" })).Once().Return(nil)
		grpc := RPC{store: store}
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1337))

		_, err := grpc.RegisterAgent(ctx, rpc.AgentInfo{Platform: "platform"})
		require.NoError(t, err)
	})

	t.Run("When the agent already has a name a different hostname does not rename it", func(t *testing.T) {
		store := store_mocks.NewMockStore(t)
		store.On("AgentFind", int64(1337)).Once().Return(&model.Agent{ID: 1337, Name: "mattlinuxpro-cd"}, nil)
		store.On("AgentUpdate", mock.MatchedBy(func(a *model.Agent) bool { return a.Name == "mattlinuxpro-cd" })).Once().Return(nil)
		grpc := RPC{store: store}
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1337))

		_, err := grpc.RegisterAgent(ctx, rpc.AgentInfo{Hostname: "mattlinuxpro"})
		require.NoError(t, err)
	})
}

func TestRPCNext(t *testing.T) {
	t.Run("When the agent sends no labels it should poll with the server labels", func(t *testing.T) {
		agent := defaultAgent()
		serverLabels, err := agent.GetServerLabels()
		require.NoError(t, err)

		store := store_mocks.NewMockStore(t)
		store.On("AgentFind", agent.ID).Once().Return(agent, nil)
		sched := scheduler_mocks.NewMockScheduler(t)
		sched.On("Poll", mock.Anything, agent.ID, rpc.Filter{Labels: serverLabels}, mock.Anything).Once().Return(nil, nil)
		grpc := RPC{
			store:     store,
			scheduler: sched,
		}

		ctx := context.WithValue(t.Context(), agentIDKey, agent.ID)
		workflow, err := grpc.Next(ctx, rpc.Filter{})
		require.NoError(t, err)
		assert.Nil(t, workflow)
	})
}

func TestCompleteChildrenIfParentCompleted(t *testing.T) {
	t.Run("When a service step is still running it should update state so workflow finishes as success", func(t *testing.T) {
		successStep := &model.Step{
			ID:      1,
			State:   model.StatusSuccess,
			Started: 1234567800,
		}
		runningService := &model.Step{
			ID:      2,
			State:   model.StatusRunning,
			Started: 1234567800,
		}
		workflow := model.Workflow{
			ID:       7,
			State:    model.StatusRunning,
			Children: []*model.Step{successStep, runningService},
		}

		mockStore := store_mocks.NewMockStore(t)
		mockStore.On("StepUpdate", mock.Anything).Return(nil)
		mockStore.On("WorkflowUpdate", mock.Anything).Return(nil)

		s := RPC{store: mockStore}
		s.completeChildrenIfParentCompleted(&workflow, 1234567900, false)

		assert.Equal(t, model.StatusSuccess, runningService.State)
		assert.Equal(t, int64(1234567900), runningService.Finished)

		result, err := pipeline.UpdateWorkflowStatusToDone(mockStore, workflow, rpc.WorkflowState{
			Started:  1234567800,
			Finished: 1234567900,
		})
		require.NoError(t, err)
		assert.Equal(t, model.StatusSuccess, result.State)
	})

	t.Run("When the workflow was canceled a running step is killed and a pending step canceled", func(t *testing.T) {
		interrupted := &model.Step{
			ID:      3,
			State:   model.StatusRunning,
			Type:    model.StepTypeCommands,
			Started: 1234567800,
		}
		pending := &model.Step{ID: 4, State: model.StatusPending, Type: model.StepTypeCommands}
		workflow := model.Workflow{
			ID:       8,
			State:    model.StatusRunning,
			Children: []*model.Step{interrupted, pending},
		}

		mockStore := store_mocks.NewMockStore(t)
		mockStore.On("StepUpdate", mock.MatchedBy(func(s *model.Step) bool {
			return s.ID == 3 && s.State == model.StatusKilled && s.Finished == 1234567900
		})).Once().Return(nil)
		mockStore.On("StepUpdate", mock.MatchedBy(func(s *model.Step) bool {
			return s.ID == 4 && s.State == model.StatusCanceled && s.Finished == 0
		})).Once().Return(nil)

		s := RPC{store: mockStore}
		s.completeChildrenIfParentCompleted(&workflow, 1234567900, true)

		assert.Equal(t, model.StatusKilled, interrupted.State)
		assert.Equal(t, int64(1234567900), interrupted.Finished)
		assert.Equal(t, model.StatusCanceled, pending.State)
		assert.Zero(t, pending.Finished)
		mockStore.AssertExpectations(t)
	})
}

func TestUpdateAgentLastWork(t *testing.T) {
	t.Run("When last work was never updated it should update last work timestamp", func(t *testing.T) {
		agent := model.Agent{
			LastWork: 0,
		}
		store := store_mocks.NewMockStore(t)
		rpc := RPC{
			store: store,
		}
		store.On("AgentUpdate", mock.Anything).Once().Return(nil)

		err := rpc.updateAgentLastWork(&agent)
		assert.NoError(t, err)

		assert.NotZero(t, agent.LastWork)
	})

	t.Run("When last work was updated over a minute ago it should update last work timestamp", func(t *testing.T) {
		lastWork := time.Now().Add(-time.Hour).Unix()
		agent := model.Agent{
			LastWork: lastWork,
		}
		store := store_mocks.NewMockStore(t)
		rpc := RPC{
			store: store,
		}
		store.On("AgentUpdate", mock.Anything).Once().Return(nil)

		err := rpc.updateAgentLastWork(&agent)
		assert.NoError(t, err)

		assert.NotEqual(t, lastWork, agent.LastWork)
	})

	t.Run("When last work was updated in the last minute it should not update last work timestamp again", func(t *testing.T) {
		lastWork := time.Now().Add(-time.Second * 30).Unix()
		agent := model.Agent{
			LastWork: lastWork,
		}
		rpc := RPC{}

		err := rpc.updateAgentLastWork(&agent)
		assert.NoError(t, err)

		assert.Equal(t, lastWork, agent.LastWork)
	})
}

// newTestRPC creates an RPC instance with common test infrastructure.
func newTestRPC(t *testing.T, mockStore *store_mocks.MockStore, q queue.Queue) RPC {
	t.Helper()

	pipelineTime := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "woodpecker_test",
		Name:      "pipeline_time_" + t.Name(),
	}, []string{"repo", "branch", "status", "pipeline"})
	pipelineCount := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "woodpecker_test",
		Name:      "pipeline_count_" + t.Name(),
	}, []string{"repo", "branch", "status", "pipeline"})

	return RPC{
		store:         mockStore,
		scheduler:     scheduler.NewScheduler(t.Context(), mockStore, q, memory.New()),
		logger:        logging.New(),
		pipelineTime:  pipelineTime,
		pipelineCount: pipelineCount,
	}
}

// defaultAgent returns a system agent (OrgID=-1) that can access any repo.
func defaultAgent() *model.Agent {
	return &model.Agent{
		ID:    1,
		Name:  "test-agent",
		OrgID: model.IDNotSet,
	}
}

// orgAgent999 returns an agent scoped to a specific org.
func orgAgent999() *model.Agent {
	return &model.Agent{
		ID:    2,
		Name:  "org-agent",
		OrgID: 999,
	}
}

func defaultRepo() *model.Repo {
	return &model.Repo{
		ID:       10,
		OrgID:    100,
		FullName: "test-org/test-repo",
	}
}

func defaultPipeline(status model.StatusValue) *model.Pipeline {
	return &model.Pipeline{
		ID:     20,
		RepoID: 10,
		Status: status,
		Branch: "main",
	}
}

func defaultWorkflow(state model.StatusValue) *model.Workflow {
	return &model.Workflow{
		ID:         30,
		PipelineID: 20,
		AgentID:    1,
		State:      state,
		Name:       "test-workflow",
	}
}

func defaultStep(state model.StatusValue) *model.Step {
	return &model.Step{
		ID:         40,
		UUID:       "step-uuid-123",
		PipelineID: 20,
		State:      state,
	}
}

func TestRPCUpdate(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		mockLogStore := log_mocks.NewMockService(t)
		origLogStore := server.Config.Services.LogStore
		server.Config.Services.LogStore = mockLogStore
		t.Cleanup(func() { server.Config.Services.LogStore = origLogStore })

		agent := defaultAgent()
		repo := defaultRepo()
		pipeline := defaultPipeline(model.StatusRunning)
		workflow := defaultWorkflow(model.StatusRunning)
		step := defaultStep(model.StatusRunning)

		mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("AgentFind", int64(1)).Return(agent, nil)
		mockStore.On("StepByUUID", "step-uuid-123").Return(step, nil)
		mockStore.On("GetRepo", int64(10)).Return(repo, nil)
		// pipeline.UpdateStepStatus calls StepUpdate
		mockStore.On("StepUpdate", mock.Anything).Return(nil)
		mockStore.On("WorkflowGetTree", mock.Anything).Return([]*model.Workflow{workflow}, nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		err := rpcInst.Update(ctx, "30", rpc.StepState{
			StepUUID: "step-uuid-123",
			Started:  100,
			Exited:   false,
		})
		assert.NoError(t, err)
	})

	t.Run("allow terminal step update when workflow already finished", func(t *testing.T) {
		// When the workflow is already finished, a step update that moves the
		// step to a terminal state (e.g. reporting exit code) should be allowed.
		mockStore := store_mocks.NewMockStore(t)
		mockLogStore := log_mocks.NewMockService(t)
		origLogStore := server.Config.Services.LogStore
		server.Config.Services.LogStore = mockLogStore
		t.Cleanup(func() { server.Config.Services.LogStore = origLogStore })

		agent := defaultAgent()
		pipeline := defaultPipeline(model.StatusRunning)
		workflow := defaultWorkflow(model.StatusSuccess) // finished
		step := defaultStep(model.StatusRunning)

		mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("AgentFind", int64(1)).Return(agent, nil)
		mockStore.On("StepByUUID", "step-uuid-123").Return(step, nil)
		mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)
		mockStore.On("StepUpdate", mock.Anything).Return(nil)
		mockStore.On("WorkflowGetTree", mock.Anything).Return([]*model.Workflow{workflow}, nil)
		mockLogStore.On("StepFinished", mock.Anything, mock.Anything).Return(nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		// Step reports exit → it will transition to success/failure (terminal)
		err := rpcInst.Update(ctx, "30", rpc.StepState{
			StepUUID: "step-uuid-123",
			Exited:   true,
			ExitCode: 0,
		})
		assert.NoError(t, err)
	})

	t.Run("reject non-terminal step update when workflow already finished", func(t *testing.T) {
		// When the workflow is already finished, a step update that would keep
		// the step in a non-terminal state (e.g. just started, no exit) is rejected.
		mockStore := store_mocks.NewMockStore(t)
		agent := defaultAgent()
		pipeline := defaultPipeline(model.StatusRunning)
		workflow := defaultWorkflow(model.StatusSuccess) // finished
		step := defaultStep(model.StatusRunning)

		mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("AgentFind", int64(1)).Return(agent, nil)
		mockStore.On("StepByUUID", "step-uuid-123").Return(step, nil)
		mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		// Step reports started but not exited → still running (non-terminal)
		err := rpcInst.Update(ctx, "30", rpc.StepState{StepUUID: "step-uuid-123"})
		assert.ErrorIs(t, err, ErrAgentIllegalWorkflowReRunStateChange)
	})

	t.Run("reject step update when workflow blocked", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		agent := defaultAgent()
		pipeline := defaultPipeline(model.StatusBlocked)
		workflow := defaultWorkflow(model.StatusBlocked)
		step := defaultStep(model.StatusRunning)

		mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("AgentFind", int64(1)).Return(agent, nil)
		mockStore.On("StepByUUID", "step-uuid-123").Return(step, nil)
		mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		err := rpcInst.Update(ctx, "30", rpc.StepState{StepUUID: "step-uuid-123"})
		assert.ErrorIs(t, err, ErrAgentIllegalWorkflowReRunStateChange)
	})

	t.Run("reject step belongs to different pipeline", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		agent := defaultAgent()
		pipeline := defaultPipeline(model.StatusRunning)
		workflow := defaultWorkflow(model.StatusRunning)
		step := &model.Step{
			ID:         40,
			UUID:       "step-uuid-123",
			PipelineID: 999, // different pipeline!
			State:      model.StatusRunning,
		}

		mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("AgentFind", int64(1)).Return(agent, nil)
		mockStore.On("StepByUUID", "step-uuid-123").Return(step, nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		err := rpcInst.Update(ctx, "30", rpc.StepState{StepUUID: "step-uuid-123"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not belong to current pipeline")
	})

	t.Run("reject agent from wrong org", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		agent := orgAgent999()
		repo := defaultRepo() // org 100
		pipeline := defaultPipeline(model.StatusRunning)
		workflow := defaultWorkflow(model.StatusRunning)
		step := defaultStep(model.StatusRunning)

		mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("AgentFind", int64(2)).Return(agent, nil)
		mockStore.On("StepByUUID", "step-uuid-123").Return(step, nil)
		mockStore.On("GetRepo", int64(10)).Return(repo, nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(2))

		err := rpcInst.Update(ctx, "30", rpc.StepState{StepUUID: "step-uuid-123"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not allowed to interact")
	})

	t.Run("reject invalid workflow ID", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		err := rpcInst.Update(ctx, "not-a-number", rpc.StepState{StepUUID: "step-uuid-123"})
		assert.Error(t, err)
	})

	t.Run("reject nonexistent workflow", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		mockStore.On("WorkflowLoad", int64(999)).Return(nil, errors.New("not found"))

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		err := rpcInst.Update(ctx, "999", rpc.StepState{StepUUID: "step-uuid-123"})
		assert.Error(t, err)
	})

	t.Run("reject nonexistent step UUID", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		agent := defaultAgent()
		pipeline := defaultPipeline(model.StatusRunning)
		workflow := defaultWorkflow(model.StatusRunning)

		mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("AgentFind", int64(1)).Return(agent, nil)
		mockStore.On("StepByUUID", "nonexistent").Return(nil, errors.New("not found"))

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		err := rpcInst.Update(ctx, "30", rpc.StepState{StepUUID: "nonexistent"})
		assert.Error(t, err)
	})

	t.Run("reject missing agent metadata", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		pipeline := defaultPipeline(model.StatusRunning)
		workflow := defaultWorkflow(model.StatusRunning)

		mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		// no agent_id in metadata
		ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs())

		err := rpcInst.Update(ctx, "30", rpc.StepState{StepUUID: "step-uuid-123"})
		assert.Error(t, err)
	})
}

func TestRPCInit(t *testing.T) {
	t.Run("happy path - pending pipeline", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		agent := defaultAgent()
		repo := defaultRepo()
		pipeline := defaultPipeline(model.StatusPending)
		workflow := defaultWorkflow(model.StatusPending)

		mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
		mockStore.On("AgentFind", int64(1)).Return(agent, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("GetRepo", int64(10)).Return(repo, nil)
		// pipeline.UpdateToStatusRunning -> UpdatePipeline
		mockStore.On("UpdatePipeline", mock.Anything).Return(nil)
		// updateForgeStatus -> GetUser returns error so forge interaction is skipped
		mockStore.On("GetUser", mock.Anything).Return(nil, errors.New("user not found"))
		// pipeline.UpdateWorkflowStatusToRunning -> WorkflowUpdate
		mockStore.On("WorkflowUpdate", mock.Anything).Return(nil)
		// pubsub deferred -> WorkflowGetTree
		mockStore.On("WorkflowGetTree", mock.Anything).Return([]*model.Workflow{workflow}, nil)
		// updateAgentLastWork -> AgentUpdate
		mockStore.On("AgentUpdate", mock.Anything).Return(nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		err := rpcInst.Init(ctx, "30", rpc.WorkflowState{Started: 100})
		assert.NoError(t, err)
	})

	t.Run("happy path - already running pipeline", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		agent := defaultAgent()
		repo := defaultRepo()
		pipeline := defaultPipeline(model.StatusRunning) // another workflow already started it
		workflow := defaultWorkflow(model.StatusPending)

		mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
		mockStore.On("AgentFind", int64(1)).Return(agent, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("GetRepo", int64(10)).Return(repo, nil)
		// updateForgeStatus -> GetUser returns error so forge interaction is skipped
		mockStore.On("GetUser", mock.Anything).Return(nil, errors.New("user not found"))
		mockStore.On("WorkflowUpdate", mock.Anything).Return(nil)
		mockStore.On("WorkflowGetTree", mock.Anything).Return([]*model.Workflow{workflow}, nil)
		mockStore.On("AgentUpdate", mock.Anything).Return(nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		err := rpcInst.Init(ctx, "30", rpc.WorkflowState{Started: 100})
		assert.NoError(t, err)
	})

	t.Run("reject workflow already finished", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		agent := defaultAgent()
		pipeline := defaultPipeline(model.StatusRunning)
		workflow := defaultWorkflow(model.StatusSuccess)

		mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
		mockStore.On("AgentFind", int64(1)).Return(agent, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		err := rpcInst.Init(ctx, "30", rpc.WorkflowState{Started: 100})
		assert.ErrorIs(t, err, ErrAgentIllegalWorkflowReRunStateChange)
	})

	t.Run("reject workflow blocked", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		agent := defaultAgent()
		pipeline := defaultPipeline(model.StatusRunning)
		workflow := defaultWorkflow(model.StatusBlocked)

		mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
		mockStore.On("AgentFind", int64(1)).Return(agent, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		err := rpcInst.Init(ctx, "30", rpc.WorkflowState{Started: 100})
		assert.ErrorIs(t, err, ErrAgentIllegalWorkflowRun)
	})

	t.Run("reject agent wrong org", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		agent := orgAgent999()
		pipeline := defaultPipeline(model.StatusRunning)
		workflow := defaultWorkflow(model.StatusPending)

		mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
		mockStore.On("AgentFind", int64(2)).Return(agent, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(2))

		err := rpcInst.Init(ctx, "30", rpc.WorkflowState{Started: 100})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not allowed to interact")
	})

	t.Run("reject invalid workflow ID", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		err := rpcInst.Init(ctx, "not-a-number", rpc.WorkflowState{})
		assert.Error(t, err)
	})
}

func TestRPCDone(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		mockQueue := queue_mocks.NewMockQueue(t)
		mockLogStore := log_mocks.NewMockService(t)
		origLogStore := server.Config.Services.LogStore
		server.Config.Services.LogStore = mockLogStore
		t.Cleanup(func() { server.Config.Services.LogStore = origLogStore })

		agent := defaultAgent()
		repo := defaultRepo()
		pipeline := defaultPipeline(model.StatusRunning)
		workflow := defaultWorkflow(model.StatusRunning)
		workflow.Children = []*model.Step{}

		mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
		mockStore.On("StepListFromWorkflowFind", mock.Anything).Return([]*model.Step{}, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("GetRepo", int64(10)).Return(repo, nil)
		mockStore.On("AgentFind", int64(1)).Return(agent, nil)
		mockStore.On("WorkflowUpdate", mock.Anything).Return(nil)
		mockStore.On("WorkflowGetTree", mock.Anything).Return([]*model.Workflow{}, nil)
		mockStore.On("UpdatePipeline", mock.Anything).Return(nil)
		mockStore.On("GetUser", mock.Anything).Return(nil, errors.New("user not found"))
		mockStore.On("AgentUpdate", mock.Anything).Return(nil)
		mockQueue.On("Done", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		rpcInst := newTestRPC(t, mockStore, mockQueue)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		err := rpcInst.Done(ctx, "30", rpc.WorkflowState{Started: 100, Finished: 200})
		assert.NoError(t, err)
	})

	t.Run("skipped last workflow keeps pipeline finish time", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		mockQueue := queue_mocks.NewMockQueue(t)

		agent := defaultAgent()
		repo := defaultRepo()
		pipeline := defaultPipeline(model.StatusRunning)
		// a dependent workflow that never ran, because the workflow it depends on failed
		workflow := defaultWorkflow(model.StatusPending)
		workflow.Children = []*model.Step{}
		failedWorkflow := &model.Workflow{ID: 31, PipelineID: 20, State: model.StatusFailure, Started: 100, Finished: 200}
		// workflow tree as stored after this Done call marked the workflow skipped
		storedWorkflow := &model.Workflow{ID: 30, PipelineID: 20, State: model.StatusSkipped}

		mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
		mockStore.On("StepListFromWorkflowFind", mock.Anything).Return([]*model.Step{}, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("GetRepo", int64(10)).Return(repo, nil)
		mockStore.On("AgentFind", int64(1)).Return(agent, nil)
		mockStore.On("WorkflowUpdate", mock.Anything).Return(nil)
		mockStore.On("WorkflowGetTree", mock.Anything).Return([]*model.Workflow{failedWorkflow, storedWorkflow}, nil)
		mockStore.On("UpdatePipeline", mock.Anything).Return(nil)
		mockStore.On("GetUser", mock.Anything).Return(nil, errors.New("user not found"))
		mockStore.On("AgentUpdate", mock.Anything).Return(nil)
		mockQueue.On("Done", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		rpcInst := newTestRPC(t, mockStore, mockQueue)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		// skipped workflows report neither a start nor a finish time
		err := rpcInst.Done(ctx, "30", rpc.WorkflowState{})
		require.NoError(t, err)

		mockStore.AssertCalled(t, "UpdatePipeline", mock.MatchedBy(func(p *model.Pipeline) bool {
			return p.Status == model.StatusFailure && p.Finished == 200
		}))
	})

	t.Run("reject workflow already finished", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		agent := defaultAgent()
		pipeline := defaultPipeline(model.StatusRunning)
		workflow := defaultWorkflow(model.StatusSuccess)

		mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
		mockStore.On("StepListFromWorkflowFind", mock.Anything).Return([]*model.Step{}, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)
		mockStore.On("AgentFind", int64(1)).Return(agent, nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		err := rpcInst.Done(ctx, "30", rpc.WorkflowState{Finished: 200})
		assert.ErrorIs(t, err, ErrAgentIllegalWorkflowReRunStateChange)
	})

	t.Run("reject workflow blocked", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		agent := defaultAgent()
		pipeline := defaultPipeline(model.StatusRunning)
		workflow := defaultWorkflow(model.StatusBlocked)

		mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
		mockStore.On("StepListFromWorkflowFind", mock.Anything).Return([]*model.Step{}, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)
		mockStore.On("AgentFind", int64(1)).Return(agent, nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		err := rpcInst.Done(ctx, "30", rpc.WorkflowState{Finished: 200})
		assert.ErrorIs(t, err, ErrAgentIllegalWorkflowRun)
	})

	t.Run("reject agent wrong org", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		agent := orgAgent999()
		pipeline := defaultPipeline(model.StatusRunning)
		workflow := defaultWorkflow(model.StatusRunning)

		mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
		mockStore.On("StepListFromWorkflowFind", mock.Anything).Return([]*model.Step{}, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)
		mockStore.On("AgentFind", int64(2)).Return(agent, nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(2))

		err := rpcInst.Done(ctx, "30", rpc.WorkflowState{Finished: 200})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not allowed to interact")
	})

	t.Run("reject invalid workflow ID", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		err := rpcInst.Done(ctx, "invalid", rpc.WorkflowState{})
		assert.Error(t, err)
	})
}

func TestRPCLog(t *testing.T) {
	// helper: a pipeline whose Finished timestamp is far enough in the past
	// that it is outside the drain window, so log appending is rejected.
	stalePipeline := func(status model.StatusValue) *model.Pipeline {
		p := defaultPipeline(status)
		p.Finished = time.Now().Add(-(logStreamDelayAllowed + time.Minute)).Unix()
		return p
	}

	// helper: a pipeline that finished very recently (within drain window).
	recentPipeline := func(status model.StatusValue) *model.Pipeline {
		p := defaultPipeline(status)
		p.Finished = time.Now().Add(-30 * time.Second).Unix()
		return p
	}

	t.Run("happy path: step running, pipeline running", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		mockLogStore := log_mocks.NewMockService(t)
		origLogStore := server.Config.Services.LogStore
		server.Config.Services.LogStore = mockLogStore
		t.Cleanup(func() { server.Config.Services.LogStore = origLogStore })

		agent := defaultAgent()
		pipeline := defaultPipeline(model.StatusRunning)
		step := defaultStep(model.StatusRunning)

		mockStore.On("StepByUUID", "step-uuid-123").Return(step, nil)
		mockStore.On("WorkflowByStep", mock.Anything).Return(defaultWorkflow(model.StatusRunning), nil)
		mockStore.On("WorkflowLoad", int64(30)).Return(defaultWorkflow(model.StatusRunning), nil)
		mockStore.On("AgentFind", int64(1)).Return(agent, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)
		mockStore.On("AgentUpdate", mock.Anything).Return(nil)
		mockLogStore.On("LogAppend", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		entries := []*rpc.LogEntry{
			{StepUUID: "step-uuid-123", Line: 0, Data: []byte("hello")},
			{StepUUID: "step-uuid-123", Line: 1, Data: []byte("world")},
		}
		err := rpcInst.Log(ctx, "step-uuid-123", entries)
		assert.NoError(t, err)
	})

	t.Run("allow: step finished but pipeline still running (logs draining)", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		mockLogStore := log_mocks.NewMockService(t)
		origLogStore := server.Config.Services.LogStore
		server.Config.Services.LogStore = mockLogStore
		t.Cleanup(func() { server.Config.Services.LogStore = origLogStore })

		agent := defaultAgent()
		pipeline := defaultPipeline(model.StatusRunning) // pipeline still running
		step := defaultStep(model.StatusSuccess)         // but step already finished

		mockStore.On("StepByUUID", "step-uuid-123").Return(step, nil)
		mockStore.On("WorkflowByStep", mock.Anything).Return(defaultWorkflow(model.StatusRunning), nil)
		mockStore.On("WorkflowLoad", int64(30)).Return(defaultWorkflow(model.StatusRunning), nil)
		mockStore.On("AgentFind", int64(1)).Return(agent, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)
		mockStore.On("AgentUpdate", mock.Anything).Return(nil)
		mockLogStore.On("LogAppend", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		err := rpcInst.Log(ctx, "step-uuid-123", []*rpc.LogEntry{
			{StepUUID: "step-uuid-123", Data: []byte("late log")},
		})
		assert.NoError(t, err)
	})

	t.Run("allow: step running even though pipeline finished stale (step takes priority)", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		mockLogStore := log_mocks.NewMockService(t)
		origLogStore := server.Config.Services.LogStore
		server.Config.Services.LogStore = mockLogStore
		t.Cleanup(func() { server.Config.Services.LogStore = origLogStore })

		agent := defaultAgent()
		pipeline := stalePipeline(model.StatusSuccess) // finished long ago
		step := defaultStep(model.StatusRunning)       // but step is still running

		mockStore.On("StepByUUID", "step-uuid-123").Return(step, nil)
		mockStore.On("WorkflowByStep", mock.Anything).Return(defaultWorkflow(model.StatusRunning), nil)
		mockStore.On("WorkflowLoad", int64(30)).Return(defaultWorkflow(model.StatusRunning), nil)
		mockStore.On("AgentFind", int64(1)).Return(agent, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)
		mockStore.On("AgentUpdate", mock.Anything).Return(nil)
		mockLogStore.On("LogAppend", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		err := rpcInst.Log(ctx, "step-uuid-123", []*rpc.LogEntry{
			{StepUUID: "step-uuid-123", Data: []byte("running log")},
		})
		assert.NoError(t, err)
	})

	t.Run("allow: pipeline finished recently — within drain window", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		mockLogStore := log_mocks.NewMockService(t)
		origLogStore := server.Config.Services.LogStore
		server.Config.Services.LogStore = mockLogStore
		t.Cleanup(func() { server.Config.Services.LogStore = origLogStore })

		agent := defaultAgent()
		pipeline := recentPipeline(model.StatusSuccess) // finished 30s ago
		step := defaultStep(model.StatusSuccess)

		mockStore.On("StepByUUID", "step-uuid-123").Return(step, nil)
		mockStore.On("WorkflowByStep", mock.Anything).Return(defaultWorkflow(model.StatusRunning), nil)
		mockStore.On("WorkflowLoad", int64(30)).Return(defaultWorkflow(model.StatusRunning), nil)
		mockStore.On("AgentFind", int64(1)).Return(agent, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)
		mockStore.On("AgentUpdate", mock.Anything).Return(nil)
		mockLogStore.On("LogAppend", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		err := rpcInst.Log(ctx, "step-uuid-123", []*rpc.LogEntry{
			{StepUUID: "step-uuid-123", Data: []byte("drain log")},
		})
		assert.NoError(t, err)
	})

	t.Run("reject: pipeline finished stale and step not running", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		agent := defaultAgent()
		pipeline := stalePipeline(model.StatusSuccess)
		step := defaultStep(model.StatusSuccess)

		mockStore.On("StepByUUID", "step-uuid-123").Return(step, nil)
		mockStore.On("WorkflowByStep", mock.Anything).Return(defaultWorkflow(model.StatusRunning), nil)
		mockStore.On("WorkflowLoad", int64(30)).Return(defaultWorkflow(model.StatusRunning), nil)
		mockStore.On("AgentFind", int64(1)).Return(agent, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		err := rpcInst.Log(ctx, "step-uuid-123", []*rpc.LogEntry{
			{StepUUID: "step-uuid-123", Data: []byte("test")},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "can not alter logs")
		assert.ErrorIs(t, err, ErrAgentIllegalLogStreaming)
	})

	t.Run("reject: pipeline failed stale and step not running", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		agent := defaultAgent()
		pipeline := stalePipeline(model.StatusFailure)
		step := defaultStep(model.StatusFailure)

		mockStore.On("StepByUUID", "step-uuid-123").Return(step, nil)
		mockStore.On("WorkflowByStep", mock.Anything).Return(defaultWorkflow(model.StatusRunning), nil)
		mockStore.On("WorkflowLoad", int64(30)).Return(defaultWorkflow(model.StatusRunning), nil)
		mockStore.On("AgentFind", int64(1)).Return(agent, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		err := rpcInst.Log(ctx, "step-uuid-123", []*rpc.LogEntry{
			{StepUUID: "step-uuid-123", Data: []byte("test")},
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrAgentIllegalLogStreaming)
	})

	t.Run("reject: step pending (not running), pipeline not running, outside drain window", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		agent := defaultAgent()
		pipeline := stalePipeline(model.StatusKilled)
		step := defaultStep(model.StatusPending)

		mockStore.On("StepByUUID", "step-uuid-123").Return(step, nil)
		mockStore.On("WorkflowByStep", mock.Anything).Return(defaultWorkflow(model.StatusRunning), nil)
		mockStore.On("WorkflowLoad", int64(30)).Return(defaultWorkflow(model.StatusRunning), nil)
		mockStore.On("AgentFind", int64(1)).Return(agent, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		err := rpcInst.Log(ctx, "step-uuid-123", []*rpc.LogEntry{
			{StepUUID: "step-uuid-123", Data: []byte("test")},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "can not alter logs")
		assert.ErrorIs(t, err, ErrAgentIllegalLogStreaming)
	})

	t.Run("reject: step already succeeded, pipeline succeeded stale", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		agent := defaultAgent()
		pipeline := stalePipeline(model.StatusSuccess)
		step := defaultStep(model.StatusSuccess)

		mockStore.On("StepByUUID", "step-uuid-123").Return(step, nil)
		mockStore.On("WorkflowByStep", mock.Anything).Return(defaultWorkflow(model.StatusRunning), nil)
		mockStore.On("WorkflowLoad", int64(30)).Return(defaultWorkflow(model.StatusRunning), nil)
		mockStore.On("AgentFind", int64(1)).Return(agent, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		err := rpcInst.Log(ctx, "step-uuid-123", []*rpc.LogEntry{
			{StepUUID: "step-uuid-123", Data: []byte("test")},
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrAgentIllegalLogStreaming)
	})

	t.Run("reject: step killed, pipeline killed stale", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		agent := defaultAgent()
		pipeline := stalePipeline(model.StatusKilled)
		step := defaultStep(model.StatusKilled)

		mockStore.On("StepByUUID", "step-uuid-123").Return(step, nil)
		mockStore.On("WorkflowByStep", mock.Anything).Return(defaultWorkflow(model.StatusRunning), nil)
		mockStore.On("WorkflowLoad", int64(30)).Return(defaultWorkflow(model.StatusRunning), nil)
		mockStore.On("AgentFind", int64(1)).Return(agent, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		err := rpcInst.Log(ctx, "step-uuid-123", []*rpc.LogEntry{
			{StepUUID: "step-uuid-123", Data: []byte("test")},
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrAgentIllegalLogStreaming)
	})

	t.Run("reject mismatched step UUID in log entry", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		mockLogStore := log_mocks.NewMockService(t)
		origLogStore := server.Config.Services.LogStore
		server.Config.Services.LogStore = mockLogStore
		t.Cleanup(func() { server.Config.Services.LogStore = origLogStore })

		agent := defaultAgent()
		pipeline := defaultPipeline(model.StatusRunning)
		step := defaultStep(model.StatusRunning)

		mockStore.On("StepByUUID", "step-uuid-123").Return(step, nil)
		mockStore.On("WorkflowByStep", mock.Anything).Return(defaultWorkflow(model.StatusRunning), nil)
		mockStore.On("WorkflowLoad", int64(30)).Return(defaultWorkflow(model.StatusRunning), nil)
		mockStore.On("AgentFind", int64(1)).Return(agent, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)
		mockStore.On("AgentUpdate", mock.Anything).Return(nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		// Second entry has a rogue UUID — agent trying to inject into another step.
		entries := []*rpc.LogEntry{
			{StepUUID: "step-uuid-123", Line: 0, Data: []byte("ok")},
			{StepUUID: "DIFFERENT-UUID", Line: 1, Data: []byte("injected!")},
		}
		err := rpcInst.Log(ctx, "step-uuid-123", entries)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected step UUID")
	})

	t.Run("reject agent wrong org", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		agent := orgAgent999()
		pipeline := defaultPipeline(model.StatusRunning)
		step := defaultStep(model.StatusRunning)

		mockStore.On("StepByUUID", "step-uuid-123").Return(step, nil)
		mockStore.On("WorkflowByStep", mock.Anything).Return(defaultWorkflow(model.StatusRunning), nil)
		mockStore.On("WorkflowLoad", int64(30)).Return(defaultWorkflow(model.StatusRunning), nil)
		mockStore.On("AgentFind", int64(2)).Return(agent, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(2))

		err := rpcInst.Log(ctx, "step-uuid-123", []*rpc.LogEntry{
			{StepUUID: "step-uuid-123", Data: []byte("test")},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not allowed to interact")
	})

	t.Run("reject nonexistent step UUID", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		mockStore.On("StepByUUID", "nonexistent").Return(nil, errors.New("not found"))

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		err := rpcInst.Log(ctx, "nonexistent", []*rpc.LogEntry{
			{StepUUID: "nonexistent", Data: []byte("test")},
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "could not find step")
	})
}

func TestRPCLogAppendError(t *testing.T) {
	errCantOpen := errors.New("unable to open database file")
	duplicate := fmt.Errorf("%w: %w", types.ErrInsertDuplicateDetected, errors.New("UNIQUE constraint failed"))

	tests := []struct {
		name      string
		appendErr error
		wantErr   error
	}{
		{name: "store error is returned", appendErr: errCantOpen, wantErr: errCantOpen},
		{name: "duplicate entries are ignored", appendErr: duplicate},
		{name: "joined duplicate chunks are ignored", appendErr: errors.Join(duplicate, duplicate)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStore := store_mocks.NewMockStore(t)
			mockLogStore := log_mocks.NewMockService(t)
			origLogStore := server.Config.Services.LogStore
			server.Config.Services.LogStore = mockLogStore
			t.Cleanup(func() { server.Config.Services.LogStore = origLogStore })

			mockStore.On("StepByUUID", "step-uuid-123").Return(defaultStep(model.StatusRunning), nil)
			mockStore.On("WorkflowByStep", mock.Anything).Return(defaultWorkflow(model.StatusRunning), nil)
			mockStore.On("WorkflowLoad", int64(30)).Return(defaultWorkflow(model.StatusRunning), nil)
			mockStore.On("AgentFind", int64(1)).Return(defaultAgent(), nil)
			mockStore.On("GetPipeline", int64(20)).Return(defaultPipeline(model.StatusRunning), nil)
			mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)
			mockStore.On("AgentUpdate", mock.Anything).Return(nil)
			mockLogStore.On("LogAppend", mock.Anything, mock.Anything, mock.Anything).Return(tt.appendErr)

			rpcInst := newTestRPC(t, mockStore, nil)
			ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

			err := rpcInst.Log(ctx, "step-uuid-123", []*rpc.LogEntry{
				{StepUUID: "step-uuid-123", Line: 0, Data: []byte("hello")},
			})
			if tt.wantErr == nil {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

func TestRPCExtend(t *testing.T) {
	t.Run("reject agent wrong org via permission check", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		agent := orgAgent999()
		workflow := defaultWorkflow(model.StatusRunning)
		pipeline := defaultPipeline(model.StatusRunning)

		mockStore.On("AgentFind", int64(2)).Return(agent, nil)
		mockStore.On("AgentUpdate", mock.Anything).Return(nil)
		// checkAgentPermissionByWorkflow with nil pipeline/repo -> loads from store
		mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(2))

		err := rpcInst.Extend(ctx, "30")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not allowed to interact")
	})
}

func TestRPCWait(t *testing.T) {
	t.Run("reject agent wrong org", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		agent := orgAgent999()
		workflow := defaultWorkflow(model.StatusRunning)
		pipeline := defaultPipeline(model.StatusRunning)

		mockStore.On("AgentFind", int64(2)).Return(agent, nil)
		// checkAgentPermissionByWorkflow loads from store
		mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(2))

		_, err := rpcInst.Wait(ctx, "30")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not allowed to interact")
	})
}

func TestRPCUpdateStepTypeMetric(t *testing.T) {
	t.Run("failure counter uses type=commands for a commands step", func(t *testing.T) {
		// Each subtest gets its own unregistered metric instances so counters
		// do not bleed between subtests.
		origFailure := metric.FailurePipelineStepInfoCount
		origDuration := metric.StepDurationRecord
		t.Cleanup(func() {
			metric.FailurePipelineStepInfoCount = origFailure
			metric.StepDurationRecord = origDuration
		})
		metric.FailurePipelineStepInfoCount = prometheus.NewCounterVec(
			prometheus.CounterOpts{Namespace: "woodpecker_test", Name: "step_failures_commands", Help: "test"},
			[]string{"workflow", "repo", "step", "type"},
		)
		metric.StepDurationRecord = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{Namespace: "woodpecker_test", Name: "step_duration_commands", Help: "test"},
			[]string{"workflow", "repo", "step", "type"},
		)

		mockStore := store_mocks.NewMockStore(t)
		mockLogStore := log_mocks.NewMockService(t)
		origLogStore := server.Config.Services.LogStore
		server.Config.Services.LogStore = mockLogStore
		t.Cleanup(func() { server.Config.Services.LogStore = origLogStore })

		workflow := defaultWorkflow(model.StatusRunning)
		step := &model.Step{
			ID:         50,
			UUID:       "step-uuid-commands",
			PipelineID: 20,
			Name:       "build",
			State:      model.StatusRunning,
			Started:    100,
			Type:       model.StepTypeCommands,
		}

		mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
		mockStore.On("GetPipeline", int64(20)).Return(defaultPipeline(model.StatusRunning), nil)
		mockStore.On("AgentFind", int64(1)).Return(defaultAgent(), nil)
		mockStore.On("StepByUUID", "step-uuid-commands").Return(step, nil)
		mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)
		mockStore.On("StepUpdate", mock.Anything).Return(nil)
		mockStore.On("WorkflowGetTree", mock.Anything).Return([]*model.Workflow{workflow}, nil)
		mockLogStore.On("StepFinished", mock.Anything, mock.Anything).Return(nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		err := rpcInst.Update(ctx, "30", rpc.StepState{
			StepUUID: "step-uuid-commands",
			Exited:   true,
			Finished: 200,
			ExitCode: 1,
		})
		require.NoError(t, err)

		cStep, err := metric.FailurePipelineStepInfoCount.GetMetricWithLabelValues(workflow.Name, defaultRepo().FullName, step.Name, "commands")
		require.NoError(t, err)
		assert.Equal(t, float64(1), testutil.ToFloat64(cStep))

		cSvc, err := metric.FailurePipelineStepInfoCount.GetMetricWithLabelValues(workflow.Name, defaultRepo().FullName, step.Name, "service")
		require.NoError(t, err)
		assert.Zero(t, testutil.ToFloat64(cSvc))
	})

	t.Run("failure counter uses type=service for a service step", func(t *testing.T) {
		origFailure := metric.FailurePipelineStepInfoCount
		origDuration := metric.StepDurationRecord
		t.Cleanup(func() {
			metric.FailurePipelineStepInfoCount = origFailure
			metric.StepDurationRecord = origDuration
		})
		metric.FailurePipelineStepInfoCount = prometheus.NewCounterVec(
			prometheus.CounterOpts{Namespace: "woodpecker_test", Name: "step_failures_service", Help: "test"},
			[]string{"workflow", "repo", "step", "type"},
		)
		metric.StepDurationRecord = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{Namespace: "woodpecker_test", Name: "step_duration_service", Help: "test"},
			[]string{"workflow", "repo", "step", "type"},
		)

		mockStore := store_mocks.NewMockStore(t)
		mockLogStore := log_mocks.NewMockService(t)
		origLogStore := server.Config.Services.LogStore
		server.Config.Services.LogStore = mockLogStore
		t.Cleanup(func() { server.Config.Services.LogStore = origLogStore })

		workflow := defaultWorkflow(model.StatusRunning)
		step := &model.Step{
			ID:         51,
			UUID:       "step-uuid-service",
			PipelineID: 20,
			Name:       "redis",
			State:      model.StatusRunning,
			Started:    100,
			Type:       model.StepTypeService,
		}

		mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
		mockStore.On("GetPipeline", int64(20)).Return(defaultPipeline(model.StatusRunning), nil)
		mockStore.On("AgentFind", int64(1)).Return(defaultAgent(), nil)
		mockStore.On("StepByUUID", "step-uuid-service").Return(step, nil)
		mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)
		mockStore.On("StepUpdate", mock.Anything).Return(nil)
		mockStore.On("WorkflowGetTree", mock.Anything).Return([]*model.Workflow{workflow}, nil)
		mockLogStore.On("StepFinished", mock.Anything, mock.Anything).Return(nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		err := rpcInst.Update(ctx, "30", rpc.StepState{
			StepUUID: "step-uuid-service",
			Exited:   true,
			Finished: 200,
			ExitCode: 1,
		})
		require.NoError(t, err)

		cSvc, err := metric.FailurePipelineStepInfoCount.GetMetricWithLabelValues(workflow.Name, defaultRepo().FullName, step.Name, "service")
		require.NoError(t, err)
		assert.Equal(t, float64(1), testutil.ToFloat64(cSvc))

		cStep, err := metric.FailurePipelineStepInfoCount.GetMetricWithLabelValues(workflow.Name, defaultRepo().FullName, step.Name, "commands")
		require.NoError(t, err)
		assert.Zero(t, testutil.ToFloat64(cStep))
	})

	t.Run("failure counter is not incremented when step exits successfully", func(t *testing.T) {
		origFailure := metric.FailurePipelineStepInfoCount
		origDuration := metric.StepDurationRecord
		t.Cleanup(func() {
			metric.FailurePipelineStepInfoCount = origFailure
			metric.StepDurationRecord = origDuration
		})
		metric.FailurePipelineStepInfoCount = prometheus.NewCounterVec(
			prometheus.CounterOpts{Namespace: "woodpecker_test", Name: "step_failures_success_exit", Help: "test"},
			[]string{"workflow", "repo", "step", "type"},
		)
		metric.StepDurationRecord = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{Namespace: "woodpecker_test", Name: "step_duration_success_exit", Help: "test"},
			[]string{"workflow", "repo", "step", "type"},
		)

		mockStore := store_mocks.NewMockStore(t)
		mockLogStore := log_mocks.NewMockService(t)
		origLogStore := server.Config.Services.LogStore
		server.Config.Services.LogStore = mockLogStore
		t.Cleanup(func() { server.Config.Services.LogStore = origLogStore })

		workflow := defaultWorkflow(model.StatusRunning)
		step := &model.Step{
			ID:         52,
			UUID:       "step-uuid-success",
			PipelineID: 20,
			Name:       "test",
			State:      model.StatusRunning,
			Started:    100,
			Type:       model.StepTypeCommands,
		}

		mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
		mockStore.On("GetPipeline", int64(20)).Return(defaultPipeline(model.StatusRunning), nil)
		mockStore.On("AgentFind", int64(1)).Return(defaultAgent(), nil)
		mockStore.On("StepByUUID", "step-uuid-success").Return(step, nil)
		mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)
		mockStore.On("StepUpdate", mock.Anything).Return(nil)
		mockStore.On("WorkflowGetTree", mock.Anything).Return([]*model.Workflow{workflow}, nil)
		mockLogStore.On("StepFinished", mock.Anything, mock.Anything).Return(nil)

		rpcInst := newTestRPC(t, mockStore, nil)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		err := rpcInst.Update(ctx, "30", rpc.StepState{
			StepUUID: "step-uuid-success",
			Exited:   true,
			Finished: 200,
			ExitCode: 0,
		})
		require.NoError(t, err)

		c, err := metric.FailurePipelineStepInfoCount.GetMetricWithLabelValues(workflow.Name, defaultRepo().FullName, step.Name, "step")
		require.NoError(t, err)
		assert.Zero(t, testutil.ToFloat64(c))
	})
}
