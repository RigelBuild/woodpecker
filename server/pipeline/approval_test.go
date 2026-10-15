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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/builder"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml/compiler"
	"go.woodpecker-ci.org/woodpecker/v3/server"
	forge_mocks "go.woodpecker-ci.org/woodpecker/v3/server/forge/mocks"
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	manager_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/mocks"
	registry_service_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/registry/mocks"
	secret_service_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/secret/mocks"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

const (
	gatedSecretName  = "PULUMI_GATED_ACCESS_TOKEN"
	gatedSecretValue = "gated-pulumi-token-value"
)

func TestSecretReleased(t *testing.T) {
	t.Parallel()

	approvers := []string{"mattwilkinsonn"}
	tests := []struct {
		name      string
		approval  *Approval
		approvers []string
		want      bool
	}{
		{name: "approver browser session releases", approval: &Approval{Login: "mattwilkinsonn", Session: true}, approvers: approvers, want: true},
		{name: "approver token approval withholds", approval: &Approval{Login: "mattwilkinsonn", Session: false}, approvers: approvers, want: false},
		{name: "non-approver session withholds", approval: &Approval{Login: "someone", Session: true}, approvers: approvers, want: false},
		{name: "nil approval withholds", approval: nil, approvers: approvers, want: false},
		{name: "empty approver list releases nothing", approval: &Approval{Login: "mattwilkinsonn", Session: true}, approvers: nil, want: false},
		{name: "empty login never matches", approval: &Approval{Login: "", Session: true}, approvers: []string{""}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, secretReleased(tt.approval, tt.approvers))
		})
	}
}

func parseWithSecrets(t *testing.T, pipeline *model.Pipeline, secs []*model.Secret, secErr error, approval *Approval, yaml string) ([]*builder.Item, error) {
	t.Helper()

	user := &model.User{ID: 1, Login: "test-owner"}
	repo := &model.Repo{ID: 1, Visibility: model.VisibilityPrivate}

	forge := forge_mocks.NewMockForge(t)
	forge.On("Netrc", mock.Anything, mock.Anything).Return(&model.Netrc{Machine: "github.com", Type: model.ForgeTypeGithub}, nil)
	forge.On("Name").Return("github").Maybe()
	forge.On("URL").Return("https://github.com").Maybe()

	store := store_mocks.NewMockStore(t)
	store.On("GetPipelineLastBefore", mock.Anything, mock.Anything, mock.Anything).Return(&model.Pipeline{}, nil)

	manager := manager_mocks.NewMockManager(t)
	previousManager := server.Config.Services.Manager
	server.Config.Services.Manager = manager
	t.Cleanup(func() { server.Config.Services.Manager = previousManager })

	secretService := secret_service_mocks.NewMockService(t)
	secretService.On("SecretListPipeline", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(secs, secErr)
	manager.On("SecretServiceFromRepo", mock.Anything).Return(secretService)
	registryService := registry_service_mocks.NewMockService(t)
	registryService.On("RegistryListPipeline", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*model.Registry{}, nil).Maybe()
	manager.On("RegistryServiceFromRepo", mock.Anything).Return(registryService).Maybe()
	manager.On("EnvironmentService").Return(nil).Maybe()

	return parsePipeline(t.Context(), forge, store, pipeline, user, repo,
		[]*forge_types.FileMeta{{Name: "woodpecker.yml", Data: []byte(yaml)}}, nil, approval)
}

func useApprovers(t *testing.T, approvers ...string) {
	t.Helper()
	previous := server.Config.Pipeline.SecretApprovers
	server.Config.Pipeline.SecretApprovers = approvers
	t.Cleanup(func() { server.Config.Pipeline.SecretApprovers = previous })
}

func requestingYAML(secretName string) string {
	return `
when:
  - event: [push, pull_request]

steps:
  - name: deploy
    image: alpine
    environment:
      TOKEN:
        from_secret: ` + secretName + `
    commands:
      - echo deploy
`
}

func gatedSecret() *model.Secret {
	return &model.Secret{ID: 1, RepoID: 1, Name: gatedSecretName, Value: gatedSecretValue, RequireApproval: true}
}

func otherSecret() *model.Secret {
	return &model.Secret{ID: 2, RepoID: 1, Name: "OTHER", Value: "other-value"}
}

func pushPipeline(branch string) *model.Pipeline {
	return &model.Pipeline{ID: 1, Event: model.EventPush, Branch: branch, Ref: "refs/heads/" + branch}
}

func assertApprovalError(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	assert.True(t, compiler.OnlyApprovalErrors(err), "only approval errors expected, got %v", err)
	errs := pipeline_errors.GetPipelineErrors(err)
	require.Len(t, errs, 1)
	assert.Equal(t, pipeline_errors.PipelineErrorTypeApproval, errs[0].Type)
	assert.Equal(t, &pipeline_errors.ApprovalErrorData{Name: gatedSecretName}, errs[0].Data)
}

func assertNoSecretValue(t *testing.T, items []*builder.Item, value string) {
	t.Helper()
	for _, item := range items {
		for _, sec := range item.Config.Secrets {
			assert.NotEqual(t, value, sec.Value, "withheld secret %q reached config.Secrets", sec.Name)
		}
		for _, stage := range item.Config.Stages {
			for _, step := range stage.Steps {
				for _, v := range step.Environment {
					assert.NotContains(t, v, value)
				}
				for _, v := range step.SecretMapping {
					assert.NotContains(t, v, value)
				}
			}
		}
	}
}

func TestParsePipelineGatedSecretWithheldWithoutApproval(t *testing.T) {
	useApprovers(t, "test-owner")

	_, err := parseWithSecrets(t, pushPipeline("main"), []*model.Secret{gatedSecret()}, nil, nil, requestingYAML(gatedSecretName))
	assertApprovalError(t, err)
}

func TestParsePipelineGatedSecretReleasedToApproverSession(t *testing.T) {
	useApprovers(t, "test-owner")

	items, err := parseWithSecrets(t, pushPipeline("main"), []*model.Secret{gatedSecret()}, nil,
		&Approval{Login: "test-owner", Session: true}, requestingYAML(gatedSecretName))
	require.NoError(t, err)
	require.Len(t, items, 1)
	step := items[0].Config.Stages[len(items[0].Config.Stages)-1].Steps[0]
	assert.Equal(t, gatedSecretValue, step.SecretMapping["TOKEN"])
}

func TestParsePipelineGatedSecretTokenApprovalWithholds(t *testing.T) {
	useApprovers(t, "test-owner")

	_, err := parseWithSecrets(t, pushPipeline("main"), []*model.Secret{gatedSecret()}, nil,
		&Approval{Login: "test-owner", Session: false}, requestingYAML(gatedSecretName))
	assertApprovalError(t, err)
}

func TestParsePipelineGatedSecretEmptyApproversReleasesNothing(t *testing.T) {
	useApprovers(t)

	_, err := parseWithSecrets(t, pushPipeline("main"), []*model.Secret{gatedSecret()}, nil,
		&Approval{Login: "test-owner", Session: true}, requestingYAML(gatedSecretName))
	assertApprovalError(t, err)
}

func TestParsePipelineWithheldValueNeverReachesConfigSecrets(t *testing.T) {
	useApprovers(t, "test-owner")

	items, err := parseWithSecrets(t, pushPipeline("main"), []*model.Secret{gatedSecret(), otherSecret()}, nil, nil, requestingYAML("OTHER"))
	require.NoError(t, err)
	require.Len(t, items, 1)
	assertNoSecretValue(t, items, gatedSecretValue)
	var names []string
	for _, sec := range items[0].Config.Secrets {
		names = append(names, sec.Name)
	}
	assert.Contains(t, names, "OTHER")
	assert.NotContains(t, names, gatedSecretName)
}

func TestParsePipelineTrunkMergeRefWithholdsEvenWhenApproved(t *testing.T) {
	useApprovers(t, "test-owner")

	for _, p := range []*model.Pipeline{
		{ID: 1, Event: model.EventPush, Branch: "trunk-merge/main/1", Ref: "refs/heads/trunk-merge/main/1"},
		{ID: 1, Event: model.EventPush, Branch: "main", Ref: "refs/heads/trunk-merge/main/1"},
	} {
		t.Run(p.Branch+" "+p.Ref, func(t *testing.T) {
			items, err := parseWithSecrets(t, p, []*model.Secret{gatedSecret()}, nil,
				&Approval{Login: "test-owner", Session: true}, requestingYAML(gatedSecretName))
			assertApprovalError(t, err)
			assertNoSecretValue(t, items, gatedSecretValue)
		})
	}
}

func TestParsePipelineSecretServiceErrorWithholds(t *testing.T) {
	useApprovers(t, "test-owner")

	items, err := parseWithSecrets(t, pushPipeline("main"), nil, errors.New("secret backend down"),
		&Approval{Login: "test-owner", Session: true}, requestingYAML(gatedSecretName))
	require.Error(t, err)
	assert.Nil(t, items)
}

func TestParsePipelineBranchFilter(t *testing.T) {
	branchSecret := func() *model.Secret {
		return &model.Secret{
			ID: 3, RepoID: 1, Name: "PULUMI_ACCESS_TOKEN", Value: "branch-filtered-value",
			Events:   []model.WebhookEvent{model.EventPush, model.EventCron, model.EventManual},
			Branches: []string{"main"},
		}
	}

	t.Run("non-main push is rejected", func(t *testing.T) {
		items, err := parseWithSecrets(t, pushPipeline("feature"), []*model.Secret{branchSecret()}, nil, nil, requestingYAML("PULUMI_ACCESS_TOKEN"))
		require.Error(t, err)
		assert.False(t, compiler.OnlyApprovalErrors(err), "a branch mismatch is not an approval block")
		assertNoSecretValue(t, items, "branch-filtered-value")
	})

	t.Run("main push is allowed", func(t *testing.T) {
		items, err := parseWithSecrets(t, pushPipeline("main"), []*model.Secret{branchSecret()}, nil, nil, requestingYAML("PULUMI_ACCESS_TOKEN"))
		require.NoError(t, err)
		require.Len(t, items, 1)
		step := items[0].Config.Stages[len(items[0].Config.Stages)-1].Steps[0]
		assert.Equal(t, "branch-filtered-value", step.SecretMapping["TOKEN"])
	})
}

func TestCreateNewOutOfOldResetsReview(t *testing.T) {
	t.Parallel()

	old := &model.Pipeline{ID: 4, Number: 2, Status: model.StatusSuccess, Reviewer: "test-owner", Reviewed: 1700000000}
	fresh := createNewOutOfOld(old)
	assert.Empty(t, fresh.Reviewer)
	assert.Zero(t, fresh.Reviewed)
}
