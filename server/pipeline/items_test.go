// Copyright 2023 Woodpecker Authors
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
<<<<<<< conflict 1 of 2
+++++++ vsrqopkp 79d82d67 "Merge pull request #149 from RigelBuild/ci-pipelines/rig-8574-release-3-17-0-rigel-23"
	"context"
%%%%%%% diff from: ouulyvmm f4d3d580 "Update pre-commit hook rbubley/mirrors-prettier to v3.9.10 (#7277)"
\\\\\\\        to: yyprtmsp 1a845577 "Update module gitlab.com/gitlab-org/api/client-go/v3 to v3.47.0 (#7541)"
+	"errors"
>>>>>>> conflict 1 of 2 ends
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	backend_types "go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types" //nolint:depguard // needed to construct builder.Item.Config in tests; will be resolved when backend-specific fields move to BackendOptions (see enrichPipelineItemSteps TODO)
	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/builder"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/metadata"
	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
	forge_mocks "go.woodpecker-ci.org/woodpecker/v3/server/forge/mocks"
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	manager_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/mocks"
	registry_service_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/registry/mocks"
	secret_service_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/secret/mocks"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

type readOnlyNetrcForge struct {
	forge.Forge
	netrc         *model.Netrc
	readOnlyNetrc func(context.Context, *model.Repo) (*model.Netrc, error)
}

func (f *readOnlyNetrcForge) ReadOnlyNetrc(ctx context.Context, repo *model.Repo) (*model.Netrc, error) {
	if f.readOnlyNetrc != nil {
		return f.readOnlyNetrc(ctx, repo)
	}
	return f.netrc, nil
}

func TestSetPipelineStepsOnPipeline(t *testing.T) {
	t.Parallel()

	pipeline := &model.Pipeline{
		ID:    1,
		Event: model.EventPush,
	}

	pipelineItems := []*builder.Item{{
		Workflow: &builder.Workflow{
			ID:  1,
			PID: 1,
		},
		Config: &backend_types.Config{
			Stages: []*backend_types.Stage{
				{
					Steps: []*backend_types.Step{
						{
							Name: "clone",
						},
					},
				},
				{
					Steps: []*backend_types.Step{
						{
							Name: "step",
						},
					},
				},
			},
		},
	}}

	s := store_mocks.NewMockStore(t)
	s.On("WorkflowsCreate", mock.Anything).Return(nil)

	pipeline, err := saveWorkflowsFromPipelineBuilder(s, pipeline, pipelineItems, false)
	require.NoError(t, err)
	if len(pipeline.Workflows) != 1 {
		t.Fatal("Should generate three in total")
	}
	if pipeline.Workflows[0].PipelineID != 1 {
		t.Fatal("Should set workflow's pipeline ID")
	}
	if pipeline.Workflows[0].Children[0].PPID != 1 {
		t.Fatal("Should set step PPID")
	}
}

func TestWorkflowsFromPipelineBuilderDetached(t *testing.T) {
	tests := []struct {
		name     string
		typeName backend_types.StepType
		detached bool
		want     bool
	}{
		{name: "detached commands", typeName: backend_types.StepTypeCommands, detached: true, want: true},
		{name: "service", typeName: backend_types.StepTypeService, detached: true, want: true},
		{name: "plain commands", typeName: backend_types.StepTypeCommands, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := &builder.Item{
				Workflow: &builder.Workflow{PID: 1},
				Config: &backend_types.Config{Stages: []*backend_types.Stage{{
					Steps: []*backend_types.Step{{Name: "step", Type: tt.typeName, Detached: tt.detached}},
				}}},
			}
			got := workflowsFromPipelineBuilder(&model.Pipeline{ID: 1}, []*builder.Item{item})
			require.Len(t, got, 1)
			require.Len(t, got[0].Children, 1)
			assert.Equal(t, tt.want, got[0].Children[0].Detached)
		})
	}
}

func TestSaveWorkflowsSkipped(t *testing.T) {
	t.Parallel()

	pipeline := &model.Pipeline{ID: 1, Event: model.EventPush}

	pipelineItems := []*builder.Item{
		{
			Workflow: &builder.Workflow{ID: 1, PID: 1, Name: "run"},
			Config: &backend_types.Config{
				Stages: []*backend_types.Stage{
					{Steps: []*backend_types.Step{{Name: "build"}}},
				},
			},
			OnMetadataEdit: true,
		},
		{
			Workflow:       &builder.Workflow{ID: 2, PID: 2, Name: "skip"},
			Skipped:        true,
			OnMetadataEdit: false,
		},
	}

	s := store_mocks.NewMockStore(t)
	s.On("WorkflowsCreate", mock.Anything).Return(nil)

	pipeline, err := saveWorkflowsFromPipelineBuilder(s, pipeline, pipelineItems, false)
	require.NoError(t, err)
	require.Len(t, pipeline.Workflows, 2)

	byName := map[string]*model.Workflow{}
	for _, wf := range pipeline.Workflows {
		byName[wf.Name] = wf
	}
	require.Contains(t, byName, "run")
	require.Contains(t, byName, "skip")
	assert.Equal(t, model.StatusPending, byName["run"].State)
	assert.NotEmpty(t, byName["run"].Children)
	assert.Equal(t, model.StatusSkipped, byName["skip"].State)
	assert.Empty(t, byName["skip"].Children, "skipped workflow should carry no steps")
	// The build-time OnMetadataEdit signal must survive onto the persisted
	// model.Workflow: it is the only carrier of "this is a meta gate" by the
	// time CI (meta) is reported, so a regression in the items.go mapping seam
	// would silently stop the meta status from ever posting at pull_request open.
	assert.True(t, byName["run"].OnMetadataEdit, "OnMetadataEdit must survive onto the persisted workflow")
	assert.False(t, byName["skip"].OnMetadataEdit, "non-gate workflow must not be marked OnMetadataEdit")
}

func TestSaveWorkflowsReplaceExisting(t *testing.T) {
	t.Parallel()

	pipeline := &model.Pipeline{
		ID:    1,
		Event: model.EventPush,
		// a gated pipeline already carries persisted workflows on approval
		Workflows: []*model.Workflow{{ID: 99, PID: 1}},
	}

	pipelineItems := []*builder.Item{{
		Workflow: &builder.Workflow{ID: 1, PID: 1},
		Config: &backend_types.Config{
			Stages: []*backend_types.Stage{
				{Steps: []*backend_types.Step{{Name: "clone"}}},
			},
		},
	}}

	s := store_mocks.NewMockStore(t)
	s.On("WorkflowsReplace", mock.Anything, mock.Anything).Return(nil)

	pipeline, err := saveWorkflowsFromPipelineBuilder(s, pipeline, pipelineItems, true)
	require.NoError(t, err)
	assert.Len(t, pipeline.Workflows, 1)
	assert.Equal(t, int64(1), pipeline.Workflows[0].PipelineID)
}

func TestSaveWorkflowsRejectsExistingWithoutReplace(t *testing.T) {
	t.Parallel()

	pipeline := &model.Pipeline{
		ID:        1,
		Event:     model.EventPush,
		Workflows: []*model.Workflow{{ID: 99, PID: 1}},
	}

	s := store_mocks.NewMockStore(t)

	_, err := saveWorkflowsFromPipelineBuilder(s, pipeline, nil, false)
	require.Error(t, err)
}

<<<<<<< conflict 2 of 2
+++++++ vsrqopkp 79d82d67 "Merge pull request #149 from RigelBuild/ci-pipelines/rig-8574-release-3-17-0-rigel-23"
func TestParsePipelineGlobalExtensionNetrc(t *testing.T) {
	previousSecret := server.Config.Pipeline.SecretExtensionNetrc
	previousRegistry := server.Config.Pipeline.RegistryExtensionNetrc
	server.Config.Pipeline.SecretExtensionNetrc = true
	server.Config.Pipeline.RegistryExtensionNetrc = true
	t.Cleanup(func() {
		server.Config.Pipeline.SecretExtensionNetrc = previousSecret
		server.Config.Pipeline.RegistryExtensionNetrc = previousRegistry
	})
	pipeline := &model.Pipeline{ID: 1, Event: model.EventPush}
	user := &model.User{ID: 1}
	repo := &model.Repo{ID: 1, Visibility: model.VisibilityPublic}
	mockForge := forge_mocks.NewMockForge(t)
	userNetrc := &model.Netrc{Login: "user-oauth-token", Password: "x-oauth-basic", Machine: "github.com", Type: model.ForgeTypeGithub}
	extensionNetrc := &model.Netrc{Machine: "github.com", Login: "x-access-token", Password: "read-only-token"}
	mockForge.On("Netrc", user, repo).Return(userNetrc, nil)
	mockForge.On("Name").Return("github")
	mockForge.On("URL").Return("https://github.com")
	forgeClient := &readOnlyNetrcForge{Forge: mockForge, netrc: extensionNetrc}
	store := store_mocks.NewMockStore(t)
	store.On("GetPipelineLastBefore", repo, pipeline.Branch, pipeline.Number).Return(&model.Pipeline{}, nil)
	secretService := secret_service_mocks.NewMockService(t)
	secretService.On("SecretListPipeline", mock.Anything, repo, pipeline, extensionNetrc).Return([]*model.Secret{}, nil).Once()
	registryService := registry_service_mocks.NewMockService(t)
	registryService.On("RegistryListPipeline", mock.Anything, repo, pipeline, extensionNetrc).Return([]*model.Registry{}, nil).Once()
	manager := manager_mocks.NewMockManager(t)
	previousManager := server.Config.Services.Manager
	server.Config.Services.Manager = manager
	t.Cleanup(func() { server.Config.Services.Manager = previousManager })
	manager.On("SecretServiceFromRepo", repo).Return(secretService)
	manager.On("RegistryServiceFromRepo", repo).Return(registryService)
	manager.On("EnvironmentService").Return(nil)
	yaml := &forge_types.FileMeta{Name: "woodpecker.yml", Data: []byte("when:\n  - event: push\nsteps:\n  build:\n    image: alpine\n")}
	_, err := parsePipeline(t.Context(), forgeClient, store, pipeline, user, repo, []*forge_types.FileMeta{yaml}, nil, nil)
	require.NoError(t, err)
}

func TestParsePipelineExtensionNetrcMintTimeout(t *testing.T) {
	previousSecret := server.Config.Pipeline.SecretExtensionNetrc
	previousRegistry := server.Config.Pipeline.RegistryExtensionNetrc
	previousTimeout := readOnlyNetrcMintTimeout
	server.Config.Pipeline.SecretExtensionNetrc = true
	server.Config.Pipeline.RegistryExtensionNetrc = false
	readOnlyNetrcMintTimeout = 10 * time.Millisecond
	t.Cleanup(func() {
		server.Config.Pipeline.SecretExtensionNetrc = previousSecret
		server.Config.Pipeline.RegistryExtensionNetrc = previousRegistry
		readOnlyNetrcMintTimeout = previousTimeout
	})

	pipeline := &model.Pipeline{ID: 1, Event: model.EventPush}
	user := &model.User{ID: 1}
	repo := &model.Repo{ID: 1, Visibility: model.VisibilityPublic}
	mockForge := forge_mocks.NewMockForge(t)
	mockForge.On("Netrc", user, repo).Return(&model.Netrc{Machine: "github.com", Login: "x-access-token", Type: model.ForgeTypeGithub}, nil)
	mockForge.On("Name").Return("github")
	mockForge.On("URL").Return("https://github.com")
	mintDone := make(chan error, 1)
	forgeClient := &readOnlyNetrcForge{
		Forge: mockForge,
		readOnlyNetrc: func(ctx context.Context, _ *model.Repo) (*model.Netrc, error) {
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > readOnlyNetrcMintTimeout {
				mintDone <- context.Canceled
				return nil, context.Canceled
			}
			<-ctx.Done()
			mintDone <- ctx.Err()
			return nil, ctx.Err()
		},
	}
	store := store_mocks.NewMockStore(t)
	store.On("GetPipelineLastBefore", repo, pipeline.Branch, pipeline.Number).Return(&model.Pipeline{}, nil)
	secretService := secret_service_mocks.NewMockService(t)
	secretService.On("SecretListPipeline", mock.Anything, repo, pipeline, (*model.Netrc)(nil)).Return([]*model.Secret{}, nil).Once()
	registryService := registry_service_mocks.NewMockService(t)
	registryService.On("RegistryListPipeline", mock.Anything, repo, pipeline, (*model.Netrc)(nil)).Return([]*model.Registry{}, nil).Once()
	manager := manager_mocks.NewMockManager(t)
	previousManager := server.Config.Services.Manager
	server.Config.Services.Manager = manager
	t.Cleanup(func() { server.Config.Services.Manager = previousManager })
	manager.On("SecretServiceFromRepo", repo).Return(secretService)
	manager.On("RegistryServiceFromRepo", repo).Return(registryService)
	manager.On("EnvironmentService").Return(nil)

	yaml := &forge_types.FileMeta{Name: "woodpecker.yml", Data: []byte("when:\n  - event: push\nsteps:\n  build:\n    image: alpine\n")}
	_, err := parsePipeline(t.Context(), forgeClient, store, pipeline, user, repo, []*forge_types.FileMeta{yaml}, nil, nil)
	require.NoError(t, err)
	require.ErrorIs(t, <-mintDone, context.DeadlineExceeded)
}

func TestParsePipelineTrustedSecurityNoUserTokenForCustomClonePluginOrExtensions(t *testing.T) {
	previousPlugins := server.Config.Pipeline.TrustedClonePlugins
	server.Config.Pipeline.TrustedClonePlugins = []string{"custom/clone:latest"}
	t.Cleanup(func() { server.Config.Pipeline.TrustedClonePlugins = previousPlugins })
	pipeline := &model.Pipeline{ID: 1, Event: model.EventPush}
	user := &model.User{ID: 1}
	repo := &model.Repo{
		ID:                     1,
		Visibility:             model.VisibilityPublic,
		SecretExtensionNetrc:   true,
		RegistryExtensionNetrc: true,
		Trusted:                model.TrustedConfiguration{Security: true},
	}
	mockForge := forge_mocks.NewMockForge(t)
	userNetrc := &model.Netrc{Login: "user-oauth-token", Password: "x-oauth-basic", Machine: "github.com", Type: model.ForgeTypeGithub}
	extensionNetrc := &model.Netrc{Machine: "github.com", Login: "x-access-token", Password: "read-only-token"}
	mockForge.On("Netrc", user, repo).Return(userNetrc, nil)
	mockForge.On("Name").Return("github")
	mockForge.On("URL").Return("https://github.com")
	forge := &readOnlyNetrcForge{Forge: mockForge, netrc: extensionNetrc}

	store := store_mocks.NewMockStore(t)
	store.On("GetPipelineLastBefore", repo, pipeline.Branch, pipeline.Number).Return(&model.Pipeline{}, nil)
	secretService := secret_service_mocks.NewMockService(t)
	secretService.On("SecretListPipeline", mock.Anything, repo, pipeline, extensionNetrc).Return([]*model.Secret{}, nil).Once()
	registryService := registry_service_mocks.NewMockService(t)
	registryService.On("RegistryListPipeline", mock.Anything, repo, pipeline, extensionNetrc).Return([]*model.Registry{}, nil).Once()
	manager := manager_mocks.NewMockManager(t)
	previousManager := server.Config.Services.Manager
	server.Config.Services.Manager = manager
	t.Cleanup(func() { server.Config.Services.Manager = previousManager })
	manager.On("SecretServiceFromRepo", repo).Return(secretService)
	manager.On("RegistryServiceFromRepo", repo).Return(registryService)
	manager.On("EnvironmentService").Return(nil)

	yaml := &forge_types.FileMeta{Name: "woodpecker.yml", Data: []byte(`
when:
  - event: push
clone:
  - name: custom clone
    image: custom/clone:latest
steps:
  - name: custom plugin
    image: custom/plugin:latest
    settings:
      token: plugin-token
`)}
	items, err := parsePipeline(t.Context(), forge, store, pipeline, user, repo, []*forge_types.FileMeta{yaml}, nil, nil)
	require.NoError(t, err)
	var sawClone, sawPlugin bool
	for _, stage := range items[0].Config.Stages {
		for _, step := range stage.Steps {
			sawClone = sawClone || step.Name == "custom clone"
			sawPlugin = sawPlugin || step.Name == "custom plugin"
			assert.NotContains(t, step.Environment, "CI_NETRC_PASSWORD")
			for _, value := range step.Environment {
				assert.NotContains(t, value, "user-oauth-token")
				assert.NotContains(t, value, "x-oauth-basic")
			}
		}
	}
	assert.True(t, sawClone)
	assert.True(t, sawPlugin)
}

%%%%%%% diff from: ouulyvmm f4d3d580 "Update pre-commit hook rbubley/mirrors-prettier to v3.9.10 (#7277)"
\\\\\\\        to: yyprtmsp 1a845577 "Update module gitlab.com/gitlab-org/api/client-go/v3 to v3.47.0 (#7541)"
+// Create, Approve and Restart all use the returned pipeline before they check
+// err, so a failure to persist the workflows must not return a nil pipeline.
+func TestCreatePipelineItemsKeepsPipelineOnSaveError(t *testing.T) {
+	pipeline := &model.Pipeline{ID: 1, Event: model.EventPush}
+	repo := &model.Repo{ID: 1}
+	yamls := []*forge_types.FileMeta{{
+		Name: "woodpecker.yml",
+		Data: []byte("steps:\n  - name: test\n    image: alpine\n"),
+	}}
+
+	forge := forge_mocks.NewMockForge(t)
+	forge.On("Netrc", mock.Anything, mock.Anything).Return(&model.Netrc{}, nil)
+	forge.On("Config").Return(metadata.Forge{Type: "github", URL: "https://github.com", Host: "github.com"})
+
+	store := store_mocks.NewMockStore(t)
+	store.On("GetPipelineLastBefore", mock.Anything, mock.Anything, mock.Anything).Return(&model.Pipeline{}, nil)
+	store.On("WorkflowsCreate", mock.Anything).Return(errors.New("database is locked"))
+
+	mockManager := manager_mocks.NewMockManager(t)
+	server.Config.Services.Manager = mockManager
+	secretService := secret_service_mocks.NewMockService(t)
+	secretService.On("SecretListPipeline", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
+	mockManager.On("SecretServiceFromRepo", mock.Anything).Return(secretService, nil)
+	registryService := registry_service_mocks.NewMockService(t)
+	registryService.On("RegistryListPipeline", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
+	mockManager.On("RegistryServiceFromRepo", mock.Anything).Return(registryService, nil)
+	mockManager.On("EnvironmentService").Return(nil, nil)
+
+	got, items, parseErr, err := createPipelineItems(t.Context(), forge, store, pipeline, &model.User{ID: 1}, repo, yamls, nil, false)
+	assert.ErrorContains(t, err, "database is locked")
+	assert.False(t, pipeline_errors.HasBlockingErrors(parseErr))
+	assert.Nil(t, items)
+	assert.Same(t, pipeline, got)
+}
+
>>>>>>> conflict 2 of 2 ends
func TestParsePipeline(t *testing.T) {
	t.Parallel()

	pipeline := &model.Pipeline{
		ID:    1,
		Event: model.EventPush,
		AdditionalVariables: map[string]string{
			"ADDITIONAL": "value",
		},
	}

	user := &model.User{
		ID: 1,
	}

	repo := &model.Repo{ID: 1, Visibility: model.VisibilityPublic}
	yamls := []*forge_types.FileMeta{
		{
			Name: "woodpecker.yml",
			Data: []byte(`
when:
  - event: push

steps:
  - name: test
    image: alpine
    environment:
      HELLO:
        from_secret: hello
    commands:
      - echo "hello world"
`),
		},
	}

	envs := map[string]string{
		"FOO": "bar",
	}

	forge := forge_mocks.NewMockForge(t)
	forge.On("Netrc", mock.Anything, mock.Anything).Return(&model.Netrc{
		Login:    "user-oauth-token",
		Password: "x-oauth-basic",
		Machine:  "github.com",
		Type:     model.ForgeTypeGithub,
	}, nil)
	forge.On("Config").Return(metadata.Forge{Type: "github", URL: "https://github.com", Host: "github.com"})

	store := store_mocks.NewMockStore(t)
	store.On("GetPipelineLastBefore", mock.Anything, mock.Anything, pipeline.Number).Return(&model.Pipeline{}, nil)

	mockManager := manager_mocks.NewMockManager(t)
	server.Config.Services.Manager = mockManager

	secretService := secret_service_mocks.NewMockService(t)
	secretService.On("SecretListPipeline", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*model.Secret{
		{
			Name:  "hello",
			Value: "secret world",
		},
	}, nil)
	mockManager.On("SecretServiceFromRepo", mock.Anything).Return(secretService, nil)

	registryService := registry_service_mocks.NewMockService(t)
	registryService.On("RegistryListPipeline", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*model.Registry{
		{
			Address:  "docker.io",
			Username: "user",
			Password: "password",
		},
	}, nil)
	mockManager.On("RegistryServiceFromRepo", mock.Anything).Return(registryService, nil)

	mockManager.On("EnvironmentService").Return(nil, nil)

	pipelineItems, err := parsePipeline(t.Context(), forge, store, pipeline, user, repo, yamls, envs, nil)
	assert.NoError(t, err)

	assert.Len(t, pipelineItems, 1)
	assert.Equal(t, "test", pipelineItems[0].Config.Stages[0].Steps[0].Name)
	assert.Equal(t, "alpine", pipelineItems[0].Config.Stages[0].Steps[0].Image)
	step := pipelineItems[0].Config.Stages[0].Steps[0]
	assert.Equal(t, []string{`echo "hello world"`}, step.Commands)
	assert.Equal(t, "value", step.Environment["ADDITIONAL"])
	assert.Equal(t, "bar", step.Environment["FOO"])
	assert.Equal(t, "secret world", step.Environment["HELLO"])
	assert.Equal(t, "x-access-token", step.Environment["CI_NETRC_USERNAME"])
	assert.Equal(t, "github.com", step.Environment["CI_NETRC_MACHINE"])
	assert.NotContains(t, step.Environment, "CI_NETRC_PASSWORD")
	assert.NotContains(t, step.Environment, "user-oauth-token")
	assert.NotContains(t, step.Environment, "x-oauth-basic")
}

// A panic while building the pipeline must not crash the server. The panic is
// injected through the forge mock, so the test does not depend on a parser bug.
func TestParsePipelineRecoversPanic(t *testing.T) {
	pipeline := &model.Pipeline{ID: 1, Event: model.EventPush}
	repo := &model.Repo{ID: 1}

	forge := forge_mocks.NewMockForge(t)
	forge.On("Netrc", mock.Anything, mock.Anything).Return(&model.Netrc{}, nil)
	// Config is first called by GetWorkflowMetadata inside PipelineBuilder.Build.
	forge.On("Config").Panic("injected test panic")

	store := store_mocks.NewMockStore(t)
	store.On("GetPipelineLastBefore", mock.Anything, mock.Anything, mock.Anything).Return(&model.Pipeline{}, nil)

	mockManager := manager_mocks.NewMockManager(t)
	server.Config.Services.Manager = mockManager
	secretService := secret_service_mocks.NewMockService(t)
	secretService.On("SecretListPipeline", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	mockManager.On("SecretServiceFromRepo", mock.Anything).Return(secretService, nil)
	registryService := registry_service_mocks.NewMockService(t)
	registryService.On("RegistryListPipeline", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	mockManager.On("RegistryServiceFromRepo", mock.Anything).Return(registryService, nil)
	mockManager.On("EnvironmentService").Return(nil, nil)

	yamls := []*forge_types.FileMeta{{
		Name: "woodpecker.yml",
		Data: []byte("steps:\n  - name: test\n    image: alpine\n"),
	}}

	var items []*builder.Item
	var err error
	require.NotPanics(t, func() {
		items, err = parsePipeline(t.Context(), forge, store, pipeline, &model.User{ID: 1}, repo, yamls, nil)
	})
	assert.Nil(t, items)
	assert.True(t, pipeline_errors.HasBlockingErrors(err))
	assert.ErrorContains(t, err, "injected test panic")
	assert.ErrorContains(t, err, "please report it at https://github.com/woodpecker-ci/woodpecker/issues")
}
