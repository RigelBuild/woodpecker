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
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	backend_types "go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types" //nolint:depguard // rpc.Workflow.Config is a backend config; tests must build one
	"go.woodpecker-ci.org/woodpecker/v3/rpc"
	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
	forge_mocks "go.woodpecker-ci.org/woodpecker/v3/server/forge/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
	queue_mocks "go.woodpecker-ci.org/woodpecker/v3/server/queue/mocks"
	manager_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/mocks"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

type readOnlyNetrcForge struct {
	forge.Forge
	readOnlyNetrc func(context.Context, *model.Repo) (*model.Netrc, error)
}

func (f *readOnlyNetrcForge) ReadOnlyNetrc(ctx context.Context, repo *model.Repo) (*model.Netrc, error) {
	return f.readOnlyNetrc(ctx, repo)
}

func setForgeForRepo(t *testing.T, repo *model.Repo, forgeClient forge.Forge) {
	t.Helper()
	manager := manager_mocks.NewMockManager(t)
	manager.On("ForgeFromRepo", repo).Return(forgeClient, nil)
	previousManager := server.Config.Services.Manager
	server.Config.Services.Manager = manager
	t.Cleanup(func() { server.Config.Services.Manager = previousManager })
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
		{name: "59 minutes for 55 minute timeout", remaining: 59 * time.Minute, want: true},
		{name: "exact two minute margin", remaining: 57 * time.Minute, want: true},
		{name: "less than two minute margin", remaining: 56*time.Minute + time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, usableFor(now.Add(tt.remaining), now, 55*time.Minute))
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

	t.Run("strips the approver token when expiry is too soon", func(t *testing.T) {
		setReadOnlyNetrcForge(t, repo, func(_ context.Context, _ *model.Repo) (*model.Netrc, error) {
			return &model.Netrc{Machine: "github.com", Login: "x-access-token", Password: "new-token", ExpiresAt: now.Add(56 * time.Minute)}, nil
		})
		workflow := makeWorkflow()
		s := RPC{now: func() time.Time { return now }}
		require.NoError(t, s.injectNetrc(t.Context(), workflow, repo))
		assertNoNetrc(t, workflow)
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
	t.Run("nonpositive timeout strips netrc values", func(t *testing.T) {
		workflow := makeWorkflow()
		workflow.Timeout = 0
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
	t.Run("timeout above safe duration skips mint", func(t *testing.T) {
		manager := manager_mocks.NewMockManager(t)
		manager.On("ForgeFromRepo", repo).Return(&readOnlyNetrcForge{readOnlyNetrc: func(_ context.Context, _ *model.Repo) (*model.Netrc, error) {
			return &model.Netrc{Machine: "github.com", Login: "x-access-token", Password: "token", ExpiresAt: now.Add(time.Hour)}, nil
		}}, nil).Maybe()
		previousManager := server.Config.Services.Manager
		server.Config.Services.Manager = manager
		t.Cleanup(func() { server.Config.Services.Manager = previousManager })
		workflow := makeWorkflow()
		workflow.Timeout = maxWorkflowTimeoutMinutes + 1
		s := RPC{now: func() time.Time { return now }}
		require.NoError(t, s.injectNetrc(t.Context(), workflow, repo))
		assertNoNetrc(t, workflow)
	})
	t.Run("maximum safe timeout reaches lifetime check without overflow", func(t *testing.T) {
		mints := 0
		setReadOnlyNetrcForge(t, repo, func(_ context.Context, _ *model.Repo) (*model.Netrc, error) {
			mints++
			return &model.Netrc{Machine: "github.com", Login: "x-access-token", Password: "token", ExpiresAt: now.Add(time.Hour)}, nil
		})
		workflow := makeWorkflow()
		workflow.Timeout = maxWorkflowTimeoutMinutes
		s := RPC{now: func() time.Time { return now }}
		require.NoError(t, s.injectNetrc(t.Context(), workflow, repo))
		assert.Equal(t, 1, mints)
		assertNoNetrc(t, workflow)
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

func TestRPCNextStripsNetrcOnFailures(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "workflow lookup fails"},
		{name: "pipeline lookup fails"},
		{name: "repo lookup fails"},
		{name: "forge lacks read-only netrc"},
		{name: "netrc mint fails"},
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
			case "forge lacks read-only netrc":
				mockStore.On("WorkflowLoad", int64(30)).Return(&model.Workflow{ID: 30, PipelineID: 20}, nil).Twice()
				mockStore.On("GetPipeline", int64(20)).Return(&model.Pipeline{ID: 20, RepoID: repo.ID}, nil).Once()
				mockStore.On("GetRepo", repo.ID).Return(repo, nil).Once()
				unsupported := forge_mocks.NewMockForge(t)
				unsupported.On("Name").Return("unsupported")
				setForgeForRepo(t, repo, unsupported)
			case "netrc mint fails":
				mockStore.On("WorkflowLoad", int64(30)).Return(&model.Workflow{ID: 30, PipelineID: 20}, nil).Twice()
				mockStore.On("GetPipeline", int64(20)).Return(&model.Pipeline{ID: 20, RepoID: repo.ID}, nil).Once()
				mockStore.On("GetRepo", repo.ID).Return(repo, nil).Once()
				setReadOnlyNetrcForge(t, repo, func(_ context.Context, _ *model.Repo) (*model.Netrc, error) {
					return nil, assert.AnError
				})
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
