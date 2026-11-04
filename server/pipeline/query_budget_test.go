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
	"fmt"
	"testing"

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
)

// budgetFixture runs Create and Restart against a real store, with a forge,
// config service and queue that never touch the database themselves.
type budgetFixture struct {
	store   store.Store
	counter *datastore.QueryCounter
	user    *model.User
	repo    *model.Repo
	// configs is what the config service returns on the next fetch.
	configs []*forge_types.FileMeta
}

func newBudgetFixture(t *testing.T) *budgetFixture {
	t.Helper()
	s := datastore.NewTestStore(t)
	f := &budgetFixture{store: s}

	f.user = &model.User{Login: "octocat", ForgeRemoteID: "1"}
	require.NoError(t, s.CreateUser(f.user))
	f.repo = &model.Repo{UserID: f.user.ID, Owner: "octocat", Name: "hello", FullName: "octocat/hello", ForgeRemoteID: "1", ForgeID: 1}
	require.NoError(t, s.CreateRepo(f.repo))

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

	f.counter = datastore.CountQueries(t, s)
	return f
}

// setConfigs makes the next fetch return n config files, one workflow with two
// steps each. The run number keeps the data apart between calls.
func (f *budgetFixture) setConfigs(run, n int) {
	f.configs = make([]*forge_types.FileMeta, 0, n)
	for i := range n {
		f.configs = append(f.configs, &forge_types.FileMeta{
			Name: fmt.Sprintf(".woodpecker/wf-%03d.yaml", i),
			Data: fmt.Appendf(nil, "steps:\n  - name: build\n    image: alpine\n    commands: [echo %d-%d]\n  - name: test\n    image: alpine\n    commands: [echo]\n", run, i),
		})
	}
}

func (f *budgetFixture) create(t *testing.T) *model.Pipeline {
	t.Helper()
	created, err := Create(t.Context(), f.store, f.repo, &model.Pipeline{
		Event:  model.EventPush,
		Branch: "main",
		Ref:    "refs/heads/main",
		Commit: "deadbeef",
	})
	require.NoError(t, err)
	require.Equal(t, model.StatusPending, created.Status)
	require.Len(t, created.Workflows, len(f.configs))
	return created
}

func TestQueryBudgetCreate(t *testing.T) {
	f := newBudgetFixture(t)
	run := 0
	datastore.AssertQueryBudget(t, func(n int) int64 {
		run++
		f.setConfigs(run, n)
		return f.counter.Count(func() { f.create(t) })
	})
}

func TestQueryBudgetRestart(t *testing.T) {
	f := newBudgetFixture(t)
	run := 0
	datastore.AssertQueryBudget(t, func(n int) int64 {
		run++
		f.setConfigs(run, n)
		// the API hands Restart a pipeline loaded from the store, without workflows
		last, err := f.store.GetPipeline(f.create(t).ID)
		require.NoError(t, err)
		return f.counter.Count(func() {
			restarted, err := Restart(t.Context(), f.store, last, f.user, f.repo, nil)
			require.NoError(t, err)
			require.Len(t, restarted.Workflows, n)
		})
	})
}
