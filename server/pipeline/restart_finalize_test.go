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
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
	forge_mocks "go.woodpecker-ci.org/woodpecker/v3/server/forge/mocks"
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pubsub/memory"
	"go.woodpecker-ci.org/woodpecker/v3/server/queue"
	"go.woodpecker-ci.org/woodpecker/v3/server/scheduler"
	config_service_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/config/mocks"
	manager_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/mocks"
	registry_service_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/registry/mocks"
	secret_service_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/secret/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/datastore"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

// restartFilteredYAML has one workflow that only runs on tag events. A push
// restart therefore filters every workflow out and leaves zero to persist.
var restartFilteredYAML = []byte("when:\n  - event: tag\n\nsteps:\n  - name: test\n    image: alpine\n    commands:\n      - echo hi\n")

// restartRunnableYAML runs on push, so the restart reaches item creation.
var restartRunnableYAML = []byte("when:\n  event: push\n\nsteps:\n  - name: test\n    image: alpine\n    commands:\n      - echo test\n")

// finalizeFixture is a real store holding a user, a repo, and one finished
// push pipeline that Restart will copy. The config service returns configs.
type finalizeFixture struct {
	store   store.Store
	user    *model.User
	repo    *model.Repo
	last    *model.Pipeline
	configs []*forge_types.FileMeta
}

func newFinalizeFixture(t *testing.T) *finalizeFixture {
	t.Helper()
	s := datastore.NewTestStore(t)
	f := &finalizeFixture{store: s}

	f.user = &model.User{Login: "octocat", ForgeRemoteID: "1"}
	require.NoError(t, s.CreateUser(f.user))
	f.repo = &model.Repo{UserID: f.user.ID, Owner: "octocat", Name: "hello", FullName: "octocat/hello", ForgeRemoteID: "1", ForgeID: 1}
	require.NoError(t, s.CreateRepo(f.repo))

	f.last = &model.Pipeline{
		RepoID: f.repo.ID, Number: 1, Status: model.StatusSuccess, Event: model.EventPush,
		Branch: "main", Ref: "refs/heads/main", Commit: "deadbeef",
	}
	require.NoError(t, s.CreatePipeline(f.last))

	mockForge := forge_mocks.NewMockForge(t)
	mockForge.On("Name").Return("github").Maybe()
	mockForge.On("URL").Return("https://github.com").Maybe()
	mockForge.On("Netrc", mock.Anything, mock.Anything).Return(&model.Netrc{}, nil).Maybe()
	mockForge.On("Status", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	configService := config_service_mocks.NewMockService(t)
	configService.EXPECT().Fetch(mock.Anything, mockForge, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(context.Context, forge.Forge, *model.User, *model.Repo, *model.Pipeline, []*forge_types.FileMeta, bool) ([]*forge_types.FileMeta, error) {
			return f.configs, nil
		}).Maybe()
	secretService := secret_service_mocks.NewMockService(t)
	secretService.On("SecretListPipeline", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*model.Secret{}, nil).Maybe()
	registryService := registry_service_mocks.NewMockService(t)
	registryService.On("RegistryListPipeline", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*model.Registry{}, nil).Maybe()

	manager := manager_mocks.NewMockManager(t)
	manager.On("ForgeFromRepo", mock.Anything).Return(mockForge, nil).Maybe()
	manager.On("ConfigServiceFromRepo", mock.Anything).Return(configService).Maybe()
	manager.On("SecretServiceFromRepo", mock.Anything).Return(secretService).Maybe()
	manager.On("RegistryServiceFromRepo", mock.Anything).Return(registryService).Maybe()
	manager.On("EnvironmentService").Return(nil).Maybe()

	originalManager := server.Config.Services.Manager
	server.Config.Services.Manager = manager
	t.Cleanup(func() { server.Config.Services.Manager = originalManager })

	q := queue.WithTaskStore(t.Context(), queue.NewMemoryQueue(t.Context()), s)
	originalScheduler := server.Config.Services.Scheduler
	server.Config.Services.Scheduler = scheduler.NewScheduler(t.Context(), s, q, memory.New())
	t.Cleanup(func() { server.Config.Services.Scheduler = originalScheduler })

	return f
}

// latestPipeline returns the newest pipeline row of the fixture's repo, which
// is the one Restart just saved. A failed restart returns no pipeline, so the
// test reads the row back from the store.
func (f *finalizeFixture) latestPipeline(t *testing.T) *model.Pipeline {
	t.Helper()
	pipelines, err := f.store.GetPipelineList(f.repo, &model.ListOptionsWithAll{ListOptions: &model.ListOptions{Page: 1, PerPage: 50}, All: true}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, pipelines)
	latest := pipelines[0]
	for _, p := range pipelines {
		if p.ID > latest.ID {
			latest = p
		}
	}
	stored, err := f.store.GetPipeline(latest.ID)
	require.NoError(t, err)
	return stored
}

// TestRestartFilteredToNoWorkflowsIsNotLeftPending covers case (a): the filters
// drop every workflow, so Restart must finalize the saved pipeline to error
// with a message and set finished. It must not stay pending.
func TestRestartFilteredToNoWorkflowsIsNotLeftPending(t *testing.T) {
	f := newFinalizeFixture(t)
	f.configs = []*forge_types.FileMeta{{Name: ".woodpecker.yaml", Data: restartFilteredYAML}}

	restarted, err := Restart(t.Context(), f.store, f.last, f.user, f.repo, nil)
	require.ErrorIs(t, err, errNoWorkflowsToRun)
	assert.Nil(t, restarted)

	stored := f.latestPipeline(t)
	assert.Equal(t, model.StatusError, stored.Status, "a restart with zero workflows must finalize to error, not stay pending")
	assert.NotZero(t, stored.Finished, "the finalized restart must set finished")
	require.Len(t, stored.Errors, 1)
	assert.Equal(t, errNoWorkflowsToRun.Error(), stored.Errors[0].Message)
	assert.Empty(t, stored.Workflows, "no workflow rows are expected for an all-filtered restart")
}

// TestRestartPersistenceFailuresMarkPipelineError covers cases (b) to (d): a
// failure after the new pipeline row is saved must finalize it to error.
func TestRestartPersistenceFailuresMarkPipelineError(t *testing.T) {
	failure := errors.New("database is locked")

	tests := []struct {
		name         string
		setupFailure func(*store_mocks.MockStore)
	}{
		{
			name: "find or persist pipeline config",
			setupFailure: func(s *store_mocks.MockStore) {
				s.On("ConfigsPersist", mock.Anything, mock.Anything).Return(nil, failure).Once()
			},
		},
		{
			name: "link pipeline config",
			setupFailure: func(s *store_mocks.MockStore) {
				s.On("ConfigsPersist", mock.Anything, mock.Anything).Return([]*model.Config{{ID: 1}}, nil).Once()
				s.On("PipelineConfigsCreate", mock.Anything).Return(failure).Once()
			},
		},
		{
			name: "create pipeline items",
			setupFailure: func(s *store_mocks.MockStore) {
				s.On("ConfigsPersist", mock.Anything, mock.Anything).Return([]*model.Config{{ID: 1}}, nil).Once()
				s.On("PipelineConfigsCreate", mock.Anything).Return(nil).Once()
				s.On("GetPipelineLastBefore", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Maybe()
				s.On("WorkflowsCreate", mock.Anything).Return(failure).Once()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &model.Repo{ID: 1, UserID: 1, FullName: "test/repo"}
			user := &model.User{ID: 1, Login: "testuser"}
			last := &model.Pipeline{ID: 9, Number: 1, Status: model.StatusSuccess, Event: model.EventPush, Branch: "main", Commit: "abc123", Ref: "refs/heads/main"}

			st := store_mocks.NewMockStore(t)
			st.On("ConfigsForPipeline", last.ID).Return([]*model.Config{}, nil).Once()
			st.On("GetPipeline", last.ID).Return(last, nil).Once()
			st.On("CreatePipeline", mock.Anything).Return(nil).Once()
			st.On("GetPipelineLastBefore", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Maybe()
			// The saved row must be finalized to error with the cause recorded.
			st.On("UpdatePipeline", mock.MatchedBy(func(p *model.Pipeline) bool {
				return p.Status == model.StatusError && p.Finished != 0 && len(p.Errors) > 0 &&
					strings.Contains(p.Errors[0].Message, failure.Error())
			})).Return(nil).Once()
			tt.setupFailure(st)

			forge := forge_mocks.NewMockForge(t)
			forge.On("Name").Return("github").Maybe()
			forge.On("URL").Return("https://github.com").Maybe()
			forge.On("Netrc", mock.Anything, mock.Anything).Return(&model.Netrc{}, nil).Maybe()
			forge.On("Status", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

			configService := config_service_mocks.NewMockService(t)
			configService.On("Fetch", mock.Anything, forge, user, repo, mock.Anything, mock.Anything, true).
				Return([]*forge_types.FileMeta{{Name: ".woodpecker.yml", Data: restartRunnableYAML}}, nil).Once()

			secretService := secret_service_mocks.NewMockService(t)
			secretService.On("SecretListPipeline", mock.Anything, repo, mock.Anything, mock.Anything).Return([]*model.Secret{}, nil).Maybe()
			registryService := registry_service_mocks.NewMockService(t)
			registryService.On("RegistryListPipeline", mock.Anything, repo, mock.Anything, mock.Anything).Return([]*model.Registry{}, nil).Maybe()

			manager := manager_mocks.NewMockManager(t)
			manager.On("ForgeFromRepo", repo).Return(forge, nil).Once()
			manager.On("ConfigServiceFromRepo", repo).Return(configService).Once()
			manager.On("SecretServiceFromRepo", repo).Return(secretService).Maybe()
			manager.On("RegistryServiceFromRepo", repo).Return(registryService).Maybe()
			manager.On("EnvironmentService").Return(nil).Maybe()

			originalManager := server.Config.Services.Manager
			server.Config.Services.Manager = manager
			t.Cleanup(func() { server.Config.Services.Manager = originalManager })

			originalScheduler := server.Config.Services.Scheduler
			server.Config.Services.Scheduler = scheduler.NewScheduler(t.Context(), st, nil, memory.New())
			t.Cleanup(func() { server.Config.Services.Scheduler = originalScheduler })

			_, err := Restart(t.Context(), st, last, user, repo, nil)
			require.Error(t, err)
			st.AssertExpectations(t)
		})
	}
}
