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
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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
	"go.woodpecker-ci.org/woodpecker/v3/server/services/secret"
	secret_service_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/secret/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/services/utils"
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
	secretService := secret_service_mocks.NewMockService(t)
	secretService.On("SecretListPipeline", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(secs, secErr)
	return parseWithService(t, pipeline, secretService, approval, yaml)
}

func parseWithService(t *testing.T, pipeline *model.Pipeline, secretService secret.Service, approval *Approval, yaml string) ([]*builder.Item, error) {
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

	manager.On("SecretServiceFromRepo", mock.Anything).Return(secretService)
	registryService := registry_service_mocks.NewMockService(t)
	registryService.On("RegistryListPipeline", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*model.Registry{}, nil).Maybe()
	manager.On("RegistryServiceFromRepo", mock.Anything).Return(registryService).Maybe()
	manager.On("EnvironmentService").Return(nil).Maybe()

	return parsePipeline(
		t.Context(),
		forge,
		store,
		pipeline,
		user,
		repo,
		[]*forge_types.FileMeta{{Name: "woodpecker.yml", Data: []byte(yaml)}},
		nil,
		approval,
	)
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

// A merge-queue run can never release a gated secret, so it fails like a filter
// mismatch rather than blocking forever on an approval that cannot help.
func TestParsePipelineTrunkMergeRunDropsGatedSecret(t *testing.T) {
	useApprovers(t, "test-owner")

	for _, p := range []*model.Pipeline{
		{ID: 1, Event: model.EventPush, Branch: "trunk-merge/main/1", Ref: "refs/heads/trunk-merge/main/1"},
		{ID: 1, Event: model.EventPush, Branch: "main", Ref: "refs/heads/trunk-merge/main/1"},
		// GitHub sets Branch to the base and Ref to the PR ref; only Refspec names the head.
		{ID: 1, Event: model.EventPull, Branch: "main", Ref: "refs/pull/5/head", Refspec: "trunk-merge/main/5:main"},
		// Trunk's trial branches are merge-queue runs too.
		{ID: 1, Event: model.EventPush, Branch: "trunk-temp/pr-2266/89b21c36", Ref: "refs/heads/trunk-temp/pr-2266/89b21c36"},
		{ID: 1, Event: model.EventPull, Branch: "main", Ref: "refs/pull/5/head", Refspec: "trunk-temp/pr-5/abc:main"},
	} {
		for _, approval := range []*Approval{nil, {Login: "test-owner", Session: true}} {
			t.Run(p.Branch+" "+p.Ref+" "+p.Refspec, func(t *testing.T) {
				items, err := parseWithSecrets(
					t,
					p,
					[]*model.Secret{gatedSecret()},
					nil,
					approval,
					requestingYAML(gatedSecretName),
				)
				require.Error(t, err)
				assert.False(t, compiler.OnlyApprovalErrors(err), "a merge-queue run must not block for approval")
				assertNoSecretValue(t, items, gatedSecretValue)
			})
		}
	}
}

// A gated secret the run's event may not use is dropped, not reported as withheld.
func TestParsePipelineGatedSecretEventMismatchDoesNotBlock(t *testing.T) {
	useApprovers(t, "test-owner")
	pushOnly := gatedSecret()
	pushOnly.Events = []model.WebhookEvent{model.EventPush}
	pull := &model.Pipeline{ID: 1, Event: model.EventPull, Branch: "main", Ref: "refs/pull/5/head", Refspec: "feature:main"}

	items, err := parseWithSecrets(
		t,
		pull,
		[]*model.Secret{pushOnly},
		nil,
		nil,
		requestingYAML(gatedSecretName),
	)
	require.Error(t, err)
	assert.False(t, compiler.OnlyApprovalErrors(err), "an event-ineligible secret must not block for approval")
	assertNoSecretValue(t, items, gatedSecretValue)
}

// Caller-supplied variables can run code in every step, so they never share a
// run with a gated or branch-filtered secret.
func TestParsePipelineCallerVariablesWithholdRestrictedSecrets(t *testing.T) {
	useApprovers(t, "test-owner")
	branchFiltered := &model.Secret{
		ID: 3, RepoID: 1, Name: "PULUMI_ACCESS_TOKEN", Value: "branch-filtered-value",
		Events:   []model.WebhookEvent{model.EventPush, model.EventCron, model.EventManual},
		Branches: []string{"main"},
	}
	manual := &model.Pipeline{
		ID: 1, Event: model.EventPush, Branch: "main", Ref: "refs/heads/main",
		AdditionalVariables: map[string]string{"BASH_ENV": "/tmp/payload"},
	}

	for _, sec := range []*model.Secret{branchFiltered, gatedSecret()} {
		t.Run(sec.Name, func(t *testing.T) {
			items, err := parseWithSecrets(
				t,
				manual,
				[]*model.Secret{sec},
				nil,
				&Approval{Login: "test-owner", Session: true},
				requestingYAML(sec.Name),
			)
			require.Error(t, err)
			assert.False(t, compiler.OnlyApprovalErrors(err))
			assertNoSecretValue(t, items, sec.Value)
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

// An extension outage gates every stored secret, so the run blocks rather than
// silently running with stored values the extension was meant to override.
func TestParsePipelineExtensionErrorBlocksUngatedSecret(t *testing.T) {
	useApprovers(t, "test-owner")
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(ts.Close)
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	client, err := utils.NewHTTPClient(key, "loopback", 10*time.Second)
	require.NoError(t, err)

	ungated := otherSecret()
	dbStore := store_mocks.NewMockStore(t)
	dbStore.On("SecretList", mock.Anything, true, mock.Anything).Return([]*model.Secret{ungated}, nil)
	service := secret.NewCombined(secret.NewDB(dbStore), secret.NewHTTP(ts.URL, client, false))

	items, err := parseWithService(t, pushPipeline("main"), service, nil, requestingYAML(ungated.Name))
	require.Error(t, err)
	assert.True(t, compiler.OnlyApprovalErrors(err), "an extension outage must block, got %v", err)
	assertNoSecretValue(t, items, ungated.Value)
}

func TestCreateNewOutOfOldResetsReview(t *testing.T) {
	t.Parallel()

	old := &model.Pipeline{ID: 4, Number: 2, Status: model.StatusSuccess, Reviewer: "test-owner", Reviewed: 1700000000}
	fresh := createNewOutOfOld(old)
	assert.Empty(t, fresh.Reviewer)
	assert.Zero(t, fresh.Reviewed)
}
