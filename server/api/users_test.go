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

package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

<<<<<<< conflict 1 of 3
+++++++ vsrqopkp 79d82d67 "Merge pull request #149 from RigelBuild/ci-pipelines/rig-8574-release-3-17-0-rigel-23"
	"go.woodpecker-ci.org/woodpecker/v3/server"
	forge_mocks "go.woodpecker-ci.org/woodpecker/v3/server/forge/mocks"
%%%%%%% diff from: ouulyvmm f4d3d580 "Update pre-commit hook rbubley/mirrors-prettier to v3.9.10 (#7277)"
\\\\\\\        to: yyprtmsp 1a845577 "Update module gitlab.com/gitlab-org/api/client-go/v3 to v3.47.0 (#7541)"
+	"go.woodpecker-ci.org/woodpecker/v3/server"
>>>>>>> conflict 1 of 3 ends
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
<<<<<<< conflict 2 of 3
+++++++ vsrqopkp 79d82d67 "Merge pull request #149 from RigelBuild/ci-pipelines/rig-8574-release-3-17-0-rigel-23"
	manager_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/mocks"
%%%%%%% diff from: ouulyvmm f4d3d580 "Update pre-commit hook rbubley/mirrors-prettier to v3.9.10 (#7277)"
\\\\\\\        to: yyprtmsp 1a845577 "Update module gitlab.com/gitlab-org/api/client-go/v3 to v3.47.0 (#7541)"
+	"go.woodpecker-ci.org/woodpecker/v3/server/services/permissions"
>>>>>>> conflict 2 of 3 ends
)

func TestPostUser(t *testing.T) {
	s := newTestStore(t)

	t.Run("missing forge_id falls back to the primary forge", func(t *testing.T) {
		tc := newTestContext(t, s)
		withRequest(http.MethodPost, &model.User{Login: "carol"})(tc)

		PostUser(tc.Ctx)

		require.Equal(t, http.StatusOK, tc.Recorder.Code, tc.Recorder.Body.String())

		created := new(model.User)
		tc.decodeJSON(t, created)
		assert.EqualValues(t, model.PrimaryForgeID, created.ForgeID, "user must never be created with forge id 0")

		// the user's org must be forge-scoped as well
		org, err := s.OrgGet(created.OrgID)
		require.NoError(t, err)
		assert.EqualValues(t, model.PrimaryForgeID, org.ForgeID, "org must never be created with forge id 0")
	})

	t.Run("explicit forge_id is kept", func(t *testing.T) {
		tc := newTestContext(t, s)
		withRequest(http.MethodPost, &model.User{Login: "dave", ForgeID: 2})(tc)

		PostUser(tc.Ctx)

		require.Equal(t, http.StatusOK, tc.Recorder.Code, tc.Recorder.Body.String())

		created := new(model.User)
		tc.decodeJSON(t, created)
		assert.EqualValues(t, 2, created.ForgeID)
	})
}
<<<<<<< conflict 3 of 3
+++++++ vsrqopkp 79d82d67 "Merge pull request #149 from RigelBuild/ci-pipelines/rig-8574-release-3-17-0-rigel-23"

// installUserForge wires a mock manager whose ForgeFromUser returns forge, so
// GetRepos and RefreshRepos resolve the forge they page against.
func installUserForge(t *testing.T) *forge_mocks.MockForge {
	t.Helper()
	mgr := manager_mocks.NewMockManager(t)
	_forge := forge_mocks.NewMockForge(t)
	mgr.On("ForgeFromUser", mock.Anything).Return(_forge, nil)
	server.Config.Services.Manager = mgr
	return _forge
}

// The slow forge-paging handlers gained a 499-vs-500 split: a client that
// disconnects mid-paging is reported as 499 (client closed request), a genuine
// forge error as 500. The slowHandlerProgress hook checks the request context
// before each page, so a request whose context is already canceled makes the
// forge call return context.Canceled without any real disconnect timing — the
// same error the abort branch keys on.
func TestGetReposClassifiesClientCancelVsForgeError(t *testing.T) {
	s := newTestStore(t)
	user := &model.User{ID: 1, ForgeID: defaultForgeID, Login: "alice"}

	t.Run("client cancel while listing repos returns 499", func(t *testing.T) {
		installUserForge(t)
		tc := newTestContext(t, s)
		withUser(user)(tc)

		// all=true takes the forge-paging path; a canceled request context
		// trips slowHandlerProgress before the first forge page.
		ctx, cancel := context.WithCancelCause(t.Context())
		cancel(nil)
		tc.Ctx.Request = httptest.NewRequestWithContext(ctx, http.MethodGet, "/user/repos?all=true", nil)

		GetRepos(tc.Ctx)

		assert.Equal(t, statusClientClosedRequest, tc.Recorder.Code, tc.Recorder.Body.String())
	})

	t.Run("forge error while listing repos returns 500", func(t *testing.T) {
		_forge := installUserForge(t)
		_forge.On("Repos", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, assert.AnError)
		tc := newTestContext(t, s)
		withUser(user)(tc)
		tc.Ctx.Request = httptest.NewRequest(http.MethodGet, "/user/repos?all=true", nil)

		GetRepos(tc.Ctx)

		assert.Equal(t, http.StatusInternalServerError, tc.Recorder.Code, tc.Recorder.Body.String())
	})
}

func TestRefreshReposClassifiesClientCancelVsForgeError(t *testing.T) {
	s := newTestStore(t)
	user := &model.User{ID: 1, ForgeID: defaultForgeID, Login: "alice"}

	t.Run("client cancel while syncing permissions returns 499", func(t *testing.T) {
		installUserForge(t)
		tc := newTestContext(t, s)
		withUser(user)(tc)

		ctx, cancel := context.WithCancelCause(t.Context())
		cancel(nil)
		tc.Ctx.Request = httptest.NewRequestWithContext(ctx, http.MethodGet, "/user/repos", nil)

		RefreshRepos(tc.Ctx)

		assert.Equal(t, statusClientClosedRequest, tc.Recorder.Code, tc.Recorder.Body.String())
	})

	t.Run("forge error while syncing permissions returns 500", func(t *testing.T) {
		_forge := installUserForge(t)
		_forge.On("Repos", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, assert.AnError)
		// RefreshRepos logs the forge name on the 500 path.
		_forge.On("Name").Return("mock-forge").Maybe()
		tc := newTestContext(t, s)
		withUser(user)(tc)
		tc.Ctx.Request = httptest.NewRequest(http.MethodGet, "/user/repos", nil)

		RefreshRepos(tc.Ctx)

		assert.Equal(t, http.StatusInternalServerError, tc.Recorder.Code, tc.Recorder.Body.String())
	})
}
%%%%%%% diff from: ouulyvmm f4d3d580 "Update pre-commit hook rbubley/mirrors-prettier to v3.9.10 (#7277)"
\\\\\\\        to: yyprtmsp 1a845577 "Update module gitlab.com/gitlab-org/api/client-go/v3 to v3.47.0 (#7541)"
+
+func TestPatchUser(t *testing.T) {
+	s := newTestStore(t)
+
+	admins := server.Config.Permissions.Admins
+	t.Cleanup(func() { server.Config.Permissions.Admins = admins })
+
+	patch := func(t *testing.T, login string, in *model.User) {
+		t.Helper()
+		tc := newTestContext(t, s)
+		withRequest(http.MethodPatch, in)(tc)
+		withParam("login", login)(tc)
+
+		PatchUser(tc.Ctx)
+
+		require.Equal(t, http.StatusOK, tc.Recorder.Code, tc.Recorder.Body.String())
+	}
+
+	t.Run("revoking a listed admin stops admin at sign in but keeps them listed", func(t *testing.T) {
+		server.Config.Permissions.Admins = permissions.NewAdmins([]string{"Erin"}, false)
+		require.NoError(t, s.CreateUser(&model.User{Login: "erin", ForgeID: model.PrimaryForgeID, ForgeRemoteID: "5", Hash: "erin", Admin: true}))
+
+		patch(t, "erin", &model.User{Login: "erin", ForgeID: model.PrimaryForgeID, ForgeRemoteID: "5", Admin: false})
+
+		// the next sign in must not grant admin again
+		assert.False(t, server.Config.Permissions.Admins.IsAdmin(&model.User{Login: "erin"}))
+		stored, err := s.GetUserByLogin(model.PrimaryForgeID, "erin")
+		require.NoError(t, err)
+		assert.False(t, stored.Admin)
+
+		// the user list must still show that the user is in WOODPECKER_ADMIN
+		tc := newTestContext(t, s)
+		GetUsers(tc.Ctx)
+		require.Equal(t, http.StatusOK, tc.Recorder.Code, tc.Recorder.Body.String())
+		var users []*model.User
+		tc.decodeJSON(t, &users)
+		require.NotEmpty(t, users)
+		for _, u := range users {
+			if u.Login == "erin" {
+				assert.True(t, u.AdminEnv)
+				assert.False(t, u.Admin)
+				assert.True(t, u.AdminRevoked)
+			}
+		}
+	})
+
+	t.Run("a listed user who never signed in is not shown as revoked", func(t *testing.T) {
+		server.Config.Permissions.Admins = permissions.NewAdmins([]string{"grace"}, false)
+		require.NoError(t, s.CreateUser(&model.User{Login: "grace", ForgeID: model.PrimaryForgeID, ForgeRemoteID: "7", Hash: "grace"}))
+
+		tc := newTestContext(t, s)
+		GetUsers(tc.Ctx)
+		require.Equal(t, http.StatusOK, tc.Recorder.Code, tc.Recorder.Body.String())
+		var users []*model.User
+		tc.decodeJSON(t, &users)
+		found := false
+		for _, u := range users {
+			if u.Login == "grace" {
+				found = true
+				assert.True(t, u.AdminEnv)
+				assert.False(t, u.Admin)
+				assert.False(t, u.AdminRevoked, "grace gets admin at the first sign in")
+			}
+		}
+		assert.True(t, found)
+	})
+
+	t.Run("keeping admin leaves the admin list alone", func(t *testing.T) {
+		server.Config.Permissions.Admins = permissions.NewAdmins([]string{"frank"}, false)
+		require.NoError(t, s.CreateUser(&model.User{Login: "frank", ForgeID: model.PrimaryForgeID, ForgeRemoteID: "6", Hash: "frank", Admin: true}))
+
+		patch(t, "frank", &model.User{Login: "frank", ForgeID: model.PrimaryForgeID, ForgeRemoteID: "6", Admin: true})
+
+		assert.True(t, server.Config.Permissions.Admins.IsAdmin(&model.User{Login: "frank"}))
+	})
+}
>>>>>>> conflict 3 of 3 ends
