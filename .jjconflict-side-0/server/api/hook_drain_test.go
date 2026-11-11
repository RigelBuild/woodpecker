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

package api_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/api"
	forge_mocks "go.woodpecker-ci.org/woodpecker/v3/server/forge/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	manager_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/services/permissions"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/shared/token"
)

func TestPostHookRefusesCreationDuringDrain(t *testing.T) {
	restoreTracker := api.ReplaceWebhookPipelineCreationsForTest()
	api.StartWebhookPipelineCreationDrain()
	t.Cleanup(restoreTracker)
	_manager := manager_mocks.NewMockManager(t)
	_forge := forge_mocks.NewMockForge(t)
	_store := store_mocks.NewMockStore(t)
	server.Config.Services.Manager = _manager
	server.Config.Permissions.Open = true
	server.Config.Permissions.Orgs = permissions.NewOrgs(nil)
	server.Config.Permissions.Admins = permissions.NewAdmins(nil)

	user := &model.User{ID: 123}
	repo := &model.Repo{
		ID:            123,
		ForgeRemoteID: "123",
		Owner:         "owner",
		Name:          "name",
		IsActive:      true,
		UserID:        user.ID,
		Hash:          "secret-123-this-is-a-secret",
	}
	repoToken := token.New(token.HookToken)
	repoToken.Set("repo-id", fmt.Sprintf("%d", repo.ID))
	signedToken, err := repoToken.Sign(repo.Hash)
	require.NoError(t, err)

	header := http.Header{}
	header.Set("Authorization", fmt.Sprintf("Bearer %s", signedToken))
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set("store", _store)
	c.Request = &http.Request{Header: header, URL: &url.URL{Scheme: "https"}}

	_manager.On("ForgeFromRepo", repo).Return(_forge, nil)
	_forge.On("Hook", mock.Anything, mock.Anything).Return(repo, &model.Pipeline{Event: model.EventPush, Message: "[skip ci]"}, nil)
	_store.On("GetRepo", repo.ID).Return(repo, nil)
	_store.On("GetUser", user.ID).Return(user, nil)
	_store.On("UpdateRepo", repo).Return(nil)

	api.PostHook(c)
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, "server is shutting down; webhook was not queued", w.Body.String())
	_store.AssertNotCalled(t, "CreatePipeline", mock.Anything)
}
