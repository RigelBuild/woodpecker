// Copyright 2023 Woodpecker Authors
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

package secret_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/services/secret"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

var (
	globalSecret = &model.Secret{
		ID:     1,
		OrgID:  0,
		RepoID: 0,
		Name:   "secret",
		Value:  "value-global",
	}

	orgSecret = &model.Secret{
		ID:     2,
		OrgID:  1,
		RepoID: 0,
		Name:   "secret",
		Value:  "value-org",
	}

	repoSecret = &model.Secret{
		ID:     3,
		OrgID:  0,
		RepoID: 1,
		Name:   "secret",
		Value:  "value-repo",
	}
)

func TestSecretListPipeline(t *testing.T) {
	mockStore := store_mocks.NewMockStore(t)

	mockStore.On("SecretList", mock.Anything, mock.Anything, mock.Anything).Once().Return([]*model.Secret{
		globalSecret,
		orgSecret,
		repoSecret,
	}, nil)

	s, err := secret.NewDB(mockStore).SecretListPipeline(t.Context(), &model.Repo{}, &model.Pipeline{}, nil)
	assert.NoError(t, err)

	assert.Len(t, s, 1)
	assert.Equal(t, "value-repo", s[0].Value)

	mockStore.On("SecretList", mock.Anything, mock.Anything, mock.Anything).Once().Return([]*model.Secret{
		globalSecret,
		orgSecret,
	}, nil)

	s, err = secret.NewDB(mockStore).SecretListPipeline(t.Context(), &model.Repo{}, &model.Pipeline{}, nil)
	assert.NoError(t, err)

	assert.Len(t, s, 1)
	assert.Equal(t, "value-org", s[0].Value)

	mockStore.On("SecretList", mock.Anything, mock.Anything, mock.Anything).Once().Return([]*model.Secret{
		globalSecret,
	}, nil)

	s, err = secret.NewDB(mockStore).SecretListPipeline(t.Context(), &model.Repo{}, &model.Pipeline{}, nil)
	assert.NoError(t, err)

	assert.Len(t, s, 1)
	assert.Equal(t, "value-global", s[0].Value)
}

// A same-name secret at a nearer scope supplies the value, but never loosens the
// policy a farther scope set on that name.
func TestSecretListPipelineKeepsStrictestPolicy(t *testing.T) {
	gatedOrg := &model.Secret{
		ID: 2, OrgID: 1, Name: "secret", Value: "value-org",
		Events:          []model.WebhookEvent{model.EventPush},
		Branches:        []string{"main"},
		Images:          []string{"plugins/deploy"},
		RequireApproval: true,
	}
	ungatedRepo := &model.Secret{
		ID: 3, RepoID: 1, Name: "secret", Value: "value-repo",
		Events: []model.WebhookEvent{model.EventPush, model.EventPull},
	}

	mockStore := store_mocks.NewMockStore(t)
	mockStore.On("SecretList", mock.Anything, mock.Anything, mock.Anything).Once().Return([]*model.Secret{gatedOrg, ungatedRepo}, nil)

	s, err := secret.NewDB(mockStore).SecretListPipeline(t.Context(), &model.Repo{}, &model.Pipeline{}, nil)
	assert.NoError(t, err)
	if assert.Len(t, s, 1) {
		assert.Equal(t, "value-repo", s[0].Value)
		assert.True(t, s[0].RequireApproval, "an ungated repo secret must not hide a gated org secret")
		assert.Equal(t, []model.WebhookEvent{model.EventPush}, s[0].Events)
		assert.Equal(t, []string{"main"}, s[0].Branches)
		assert.Equal(t, []string{"plugins/deploy"}, s[0].Images)
	}
	assert.False(t, ungatedRepo.RequireApproval, "the stored row must not be mutated")
}
