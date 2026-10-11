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
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	forge_mocks "go.woodpecker-ci.org/woodpecker/v3/server/forge/mocks"
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pubsub/memory"
	"go.woodpecker-ci.org/woodpecker/v3/server/scheduler"
	config_service_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/config/mocks"
	manager_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/mocks"
	registry_service_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/registry/mocks"
	secret_service_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/secret/mocks"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

func TestCreatePersistenceFailuresMarkPipelineError(t *testing.T) {
	failure := errors.New("database is locked")
	yamls := []*forge_types.FileMeta{{
		Name: ".woodpecker.yml",
		Data: []byte("when:\n  event: push\nsteps:\n  - name: test\n    image: alpine\n    commands:\n      - echo test\n"),
	}}

	tests := []struct {
		name         string
		setupFailure func(*store_mocks.MockStore)
	}{
		{
			name: "persist pipeline config",
			setupFailure: func(store *store_mocks.MockStore) {
				store.On("ConfigPersist", mock.Anything).Return(nil, failure).Once()
			},
		},
		{
			name: "link pipeline config",
			setupFailure: func(store *store_mocks.MockStore) {
				store.On("ConfigPersist", mock.Anything).Return(&model.Config{ID: 1}, nil).Once()
				store.On("PipelineConfigCreate", mock.Anything).Return(failure).Once()
			},
		},
		{
			name: "create pipeline items",
			setupFailure: func(store *store_mocks.MockStore) {
				store.On("ConfigPersist", mock.Anything).Return(&model.Config{ID: 1}, nil).Once()
				store.On("PipelineConfigCreate", mock.Anything).Return(nil).Once()
				store.On("GetPipelineLastBefore", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Maybe()
				store.On("WorkflowsCreate", mock.Anything).Return(failure).Once()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &model.Repo{ID: 1, UserID: 1, FullName: "test/repo"}
			user := &model.User{ID: 1, Login: "testuser"}
			pipeline := &model.Pipeline{
				Number: 1,
				Event:  model.EventPush,
				Branch: "main",
				Commit: "abc123",
				Ref:    "refs/heads/main",
			}

			store := store_mocks.NewMockStore(t)
			store.On("GetUser", repo.UserID).Return(user, nil).Once()
			store.On("CreatePipeline", mock.Anything).Return(nil).Once()
			store.On("UpdatePipeline", mock.MatchedBy(func(p *model.Pipeline) bool {
				if p == nil || p.Status != model.StatusError || len(p.Errors) == 0 {
					return false
				}
				return strings.Contains(p.Errors[0].Message, failure.Error())
			})).Return(nil).Once()
			tt.setupFailure(store)

			forge := forge_mocks.NewMockForge(t)
			forge.On("Name").Return("github").Maybe()
			forge.On("URL").Return("https://github.com").Maybe()
			forge.On("Netrc", mock.Anything, mock.Anything).Return(&model.Netrc{}, nil).Maybe()
			forge.On("Status", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

			configService := config_service_mocks.NewMockService(t)
			configService.On("Fetch", mock.Anything, forge, user, repo, mock.Anything, mock.Anything, false).Return(yamls, nil).Once()

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
			server.Config.Services.Scheduler = scheduler.NewScheduler(t.Context(), store, nil, memory.New())
			t.Cleanup(func() { server.Config.Services.Scheduler = originalScheduler })

			_, err := Create(t.Context(), store, repo, pipeline)
			require.NoError(t, err)
			store.AssertCalled(t, "UpdatePipeline", mock.MatchedBy(func(p *model.Pipeline) bool {
				return p.Status == model.StatusError && len(p.Errors) > 0 && strings.Contains(p.Errors[0].Message, failure.Error())
			}))
		})
	}
}
