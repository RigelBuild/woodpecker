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

package session

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/datastore"
	"go.woodpecker-ci.org/woodpecker/v3/shared/token"
)

func signedToken(t *testing.T, tokenType token.Type, user *model.User) string {
	t.Helper()
	tok := token.New(tokenType)
	tok.Set("user-id", strconv.FormatInt(user.ID, 10))
	raw, err := tok.Sign(user.Hash)
	require.NoError(t, err)
	return raw
}

// runSetUser runs SetUser for one request and reports BrowserSession and the status.
func runSetUser(t *testing.T, s store.Store, req *http.Request) (bool, int) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = req
	store.ToContext(c, s)

	SetUser()(c)
	return BrowserSession(c), rec.Code
}

func TestSetUserBrowserSession(t *testing.T) {
	s := datastore.NewTestStore(t)
	user := &model.User{ForgeID: 1, ForgeRemoteID: "1", Login: "approver", Hash: "approver-hash"}
	require.NoError(t, s.CreateUser(user))

	t.Run("session token with valid CSRF on POST is a browser session", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.AddCookie(&http.Cookie{Name: "user_sess", Value: signedToken(t, token.SessToken, user)})
		req.Header.Set("X-CSRF-TOKEN", signedToken(t, token.CsrfToken, user))

		session, code := runSetUser(t, s, req)
		assert.True(t, session)
		assert.Equal(t, http.StatusOK, code)
	})

	t.Run("API user token is not a browser session", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.Header.Set("Authorization", "Bearer "+signedToken(t, token.UserToken, user))

		session, _ := runSetUser(t, s, req)
		assert.False(t, session)
	})

	t.Run("session token without CSRF is rejected and not a session", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.AddCookie(&http.Cookie{Name: "user_sess", Value: signedToken(t, token.SessToken, user)})

		session, code := runSetUser(t, s, req)
		assert.False(t, session)
		assert.Equal(t, http.StatusUnauthorized, code)
	})

	t.Run("session token on GET skips CSRF so is not a verified session", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.AddCookie(&http.Cookie{Name: "user_sess", Value: signedToken(t, token.SessToken, user)})

		session, _ := runSetUser(t, s, req)
		assert.False(t, session)
	})

	t.Run("anonymous request is not a session", func(t *testing.T) {
		session, _ := runSetUser(t, s, httptest.NewRequest(http.MethodPost, "/", nil))
		assert.False(t, session)
	})
}
