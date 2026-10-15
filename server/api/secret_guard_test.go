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

package api

import (
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

const approverLogin = "mattwilkinsonn"

func useSecretApprovers(t *testing.T, approvers ...string) {
	t.Helper()
	previous := server.Config.Pipeline.SecretApprovers
	server.Config.Pipeline.SecretApprovers = approvers
	t.Cleanup(func() { server.Config.Pipeline.SecretApprovers = previous })
}

// asCaller places a user in the session and marks it a browser session or not.
func asCaller(c *gin.Context, login string, admin, browser bool) {
	c.Set("user", &model.User{ID: 9, Login: login, Admin: admin})
	if browser {
		c.Set("browser-session", true)
	}
}

func pulumiAccessToken() *model.Secret {
	return &model.Secret{
		ID: 1, RepoID: 1, Name: "PULUMI_ACCESS_TOKEN", Value: "token",
		Events:   []model.WebhookEvent{model.EventPush, model.EventCron, model.EventManual},
		Branches: []string{"main"},
	}
}

func gatedRepoSecret() *model.Secret {
	return &model.Secret{
		ID: 2, RepoID: 1, Name: "PULUMI_GATED_ACCESS_TOKEN", Value: "gated",
		Events: []model.WebhookEvent{model.EventPush}, RequireApproval: true,
	}
}

func TestRepoSecretPushOnlyUserCannotWidenPulumiAccessToken(t *testing.T) {
	useSecretApprovers(t, approverLogin)

	t.Run("adding pull_request to events gets 403", func(t *testing.T) {
		events := []model.WebhookEvent{model.EventPush, model.EventCron, model.EventManual, model.EventPull}
		c, rec, svc := newSecretCtxWithService(t, http.MethodPatch, &model.SecretPatch{Events: events})
		c.Params = gin.Params{{Key: "secret", Value: "PULUMI_ACCESS_TOKEN"}}
		asCaller(c, "push-only", false, true)
		svc.On("SecretFind", secretTestRepo, "PULUMI_ACCESS_TOKEN").Return(pulumiAccessToken(), nil)

		PatchSecret(c)

		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("clearing the branch filter gets 403", func(t *testing.T) {
		c, rec, svc := newSecretCtxWithService(t, http.MethodPatch, &model.SecretPatch{Branches: []string{}})
		c.Params = gin.Params{{Key: "secret", Value: "PULUMI_ACCESS_TOKEN"}}
		asCaller(c, "push-only", false, true)
		svc.On("SecretFind", secretTestRepo, "PULUMI_ACCESS_TOKEN").Return(pulumiAccessToken(), nil)

		PatchSecret(c)

		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("adding a branch loosens the filter and gets 403", func(t *testing.T) {
		c, rec, svc := newSecretCtxWithService(t, http.MethodPatch, &model.SecretPatch{Branches: []string{"main", "feature"}})
		c.Params = gin.Params{{Key: "secret", Value: "PULUMI_ACCESS_TOKEN"}}
		asCaller(c, "push-only", false, true)
		svc.On("SecretFind", secretTestRepo, "PULUMI_ACCESS_TOKEN").Return(pulumiAccessToken(), nil)

		PatchSecret(c)

		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("narrowing events is allowed", func(t *testing.T) {
		c, rec, svc := newSecretCtxWithService(t, http.MethodPatch, &model.SecretPatch{Events: []model.WebhookEvent{model.EventPush}})
		c.Params = gin.Params{{Key: "secret", Value: "PULUMI_ACCESS_TOKEN"}}
		asCaller(c, "push-only", false, true)
		svc.On("SecretFind", secretTestRepo, "PULUMI_ACCESS_TOKEN").Return(pulumiAccessToken(), nil)
		svc.On("SecretUpdate", secretTestRepo, mock.Anything).Return(nil)

		PatchSecret(c)

		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("an admin may widen events", func(t *testing.T) {
		events := []model.WebhookEvent{model.EventPush, model.EventCron, model.EventManual, model.EventPull}
		c, rec, svc := newSecretCtxWithService(t, http.MethodPatch, &model.SecretPatch{Events: events})
		c.Params = gin.Params{{Key: "secret", Value: "PULUMI_ACCESS_TOKEN"}}
		asCaller(c, "admin", true, false)
		svc.On("SecretFind", secretTestRepo, "PULUMI_ACCESS_TOKEN").Return(pulumiAccessToken(), nil)
		svc.On("SecretUpdate", secretTestRepo, mock.MatchedBy(func(s *model.Secret) bool {
			return len(s.Events) == 4
		})).Return(nil)

		PatchSecret(c)

		assert.Equal(t, http.StatusOK, rec.Code)
	})
}

func TestRepoSecretGatedChangeRequiresApproverSession(t *testing.T) {
	useSecretApprovers(t, approverLogin)
	newValue := "rotated"

	t.Run("non-approver change gets 403", func(t *testing.T) {
		c, rec, svc := newSecretCtxWithService(t, http.MethodPatch, &model.SecretPatch{Value: &newValue})
		c.Params = gin.Params{{Key: "secret", Value: "PULUMI_GATED_ACCESS_TOKEN"}}
		asCaller(c, "someone", true, true)
		svc.On("SecretFind", secretTestRepo, "PULUMI_GATED_ACCESS_TOKEN").Return(gatedRepoSecret(), nil)

		PatchSecret(c)

		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("approver API token change gets 403", func(t *testing.T) {
		c, rec, svc := newSecretCtxWithService(t, http.MethodPatch, &model.SecretPatch{Value: &newValue})
		c.Params = gin.Params{{Key: "secret", Value: "PULUMI_GATED_ACCESS_TOKEN"}}
		asCaller(c, approverLogin, true, false)
		svc.On("SecretFind", secretTestRepo, "PULUMI_GATED_ACCESS_TOKEN").Return(gatedRepoSecret(), nil)

		PatchSecret(c)

		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("non-approver clearing the gate gets 403", func(t *testing.T) {
		off := false
		c, rec, svc := newSecretCtxWithService(t, http.MethodPatch, &model.SecretPatch{RequireApproval: &off})
		c.Params = gin.Params{{Key: "secret", Value: "PULUMI_GATED_ACCESS_TOKEN"}}
		asCaller(c, "someone", true, true)
		svc.On("SecretFind", secretTestRepo, "PULUMI_GATED_ACCESS_TOKEN").Return(gatedRepoSecret(), nil)

		PatchSecret(c)

		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("approver session change succeeds", func(t *testing.T) {
		c, rec, svc := newSecretCtxWithService(t, http.MethodPatch, &model.SecretPatch{Value: &newValue})
		c.Params = gin.Params{{Key: "secret", Value: "PULUMI_GATED_ACCESS_TOKEN"}}
		asCaller(c, approverLogin, false, true)
		svc.On("SecretFind", secretTestRepo, "PULUMI_GATED_ACCESS_TOKEN").Return(gatedRepoSecret(), nil)
		svc.On("SecretUpdate", secretTestRepo, mock.MatchedBy(func(s *model.Secret) bool {
			return s.Value == "rotated" && s.RequireApproval
		})).Return(nil)

		PatchSecret(c)

		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("non-approver delete gets 403", func(t *testing.T) {
		c, rec, svc := newSecretCtxWithService(t, http.MethodDelete, nil)
		c.Params = gin.Params{{Key: "secret", Value: "PULUMI_GATED_ACCESS_TOKEN"}}
		asCaller(c, "someone", true, true)
		svc.On("SecretFind", secretTestRepo, "PULUMI_GATED_ACCESS_TOKEN").Return(gatedRepoSecret(), nil)

		DeleteSecret(c)

		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("non-approver create of a gated secret gets 403", func(t *testing.T) {
		in := &model.Secret{Name: "NEW_GATED", Value: "v", Events: []model.WebhookEvent{model.EventPush}, RequireApproval: true}
		c, rec := newSecretCtx(t, http.MethodPost, in)
		asCaller(c, "someone", true, true)

		PostSecret(c)

		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("approver session creates gated secret with branches", func(t *testing.T) {
		in := &model.Secret{
			Name: "NEW_GATED", Value: "v", Events: []model.WebhookEvent{model.EventPush},
			RequireApproval: true, Branches: []string{"main"},
		}
		c, rec, svc := newSecretCtxWithService(t, http.MethodPost, in)
		asCaller(c, approverLogin, false, true)
		svc.On("SecretCreate", secretTestRepo, mock.MatchedBy(func(s *model.Secret) bool {
			return s.RequireApproval && len(s.Branches) == 1 && s.Branches[0] == "main"
		})).Return(nil)

		PostSecret(c)

		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), `"require_approval":true`)
		assert.Contains(t, rec.Body.String(), `"branches":["main"]`)
	})
}

func TestOrgSecretGatedAndFilterGuards(t *testing.T) {
	useSecretApprovers(t, approverLogin)
	gated := func() *model.Secret {
		return &model.Secret{ID: 3, OrgID: orgSecretID, Name: "GATED", Value: "v", Events: []model.WebhookEvent{model.EventPush}, RequireApproval: true}
	}
	filtered := func() *model.Secret {
		s := pulumiAccessToken()
		s.RepoID, s.OrgID = 0, orgSecretID
		return s
	}

	t.Run("non-approver change gets 403", func(t *testing.T) {
		v := "x"
		c, rec, svc := newOrgSecretCtxWithService(t, http.MethodPatch, &model.SecretPatch{Value: &v})
		c.Params = gin.Params{{Key: "secret", Value: "GATED"}}
		asCaller(c, "org-admin", false, true)
		svc.On("OrgSecretFind", orgSecretID, "GATED").Return(gated(), nil)

		PatchOrgSecret(c)

		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("non-approver delete gets 403", func(t *testing.T) {
		c, rec, svc := newOrgSecretCtxWithService(t, http.MethodDelete, nil)
		c.Params = gin.Params{{Key: "secret", Value: "GATED"}}
		asCaller(c, "org-admin", false, true)
		svc.On("OrgSecretFind", orgSecretID, "GATED").Return(gated(), nil)

		DeleteOrgSecret(c)

		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("non-admin widening events gets 403", func(t *testing.T) {
		events := []model.WebhookEvent{model.EventPush, model.EventCron, model.EventManual, model.EventPull}
		c, rec, svc := newOrgSecretCtxWithService(t, http.MethodPatch, &model.SecretPatch{Events: events})
		c.Params = gin.Params{{Key: "secret", Value: "PULUMI_ACCESS_TOKEN"}}
		asCaller(c, "org-admin", false, true)
		svc.On("OrgSecretFind", orgSecretID, "PULUMI_ACCESS_TOKEN").Return(filtered(), nil)

		PatchOrgSecret(c)

		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("non-admin clearing branches gets 403", func(t *testing.T) {
		c, rec, svc := newOrgSecretCtxWithService(t, http.MethodPatch, &model.SecretPatch{Branches: []string{}})
		c.Params = gin.Params{{Key: "secret", Value: "PULUMI_ACCESS_TOKEN"}}
		asCaller(c, "org-admin", false, true)
		svc.On("OrgSecretFind", orgSecretID, "PULUMI_ACCESS_TOKEN").Return(filtered(), nil)

		PatchOrgSecret(c)

		assert.Equal(t, http.StatusForbidden, rec.Code)
	})
}

func TestGlobalSecretGatedGuards(t *testing.T) {
	useSecretApprovers(t, approverLogin)
	gated := func() *model.Secret {
		return &model.Secret{ID: 4, Name: "GATED", Value: "v", Events: []model.WebhookEvent{model.EventPush}, RequireApproval: true}
	}

	t.Run("non-approver admin change gets 403", func(t *testing.T) {
		v := "x"
		c, rec, svc := newGlobalSecretCtxWithService(t, http.MethodPatch, &model.SecretPatch{Value: &v})
		c.Params = gin.Params{{Key: "secret", Value: "GATED"}}
		asCaller(c, "admin", true, true)
		svc.On("GlobalSecretFind", "GATED").Return(gated(), nil)

		PatchGlobalSecret(c)

		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("non-approver admin delete gets 403", func(t *testing.T) {
		c, rec, svc := newGlobalSecretCtxWithService(t, http.MethodDelete, nil)
		c.Params = gin.Params{{Key: "secret", Value: "GATED"}}
		asCaller(c, "admin", true, true)
		svc.On("GlobalSecretFind", "GATED").Return(gated(), nil)

		DeleteGlobalSecret(c)

		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("approver session delete succeeds", func(t *testing.T) {
		c, _, svc := newGlobalSecretCtxWithService(t, http.MethodDelete, nil)
		c.Params = gin.Params{{Key: "secret", Value: "GATED"}}
		asCaller(c, approverLogin, true, true)
		svc.On("GlobalSecretFind", "GATED").Return(gated(), nil)
		svc.On("GlobalSecretDelete", "GATED").Return(nil)

		DeleteGlobalSecret(c)

		assert.Equal(t, http.StatusNoContent, c.Writer.Status())
	})
}

func TestSecretApprovalSessionFlag(t *testing.T) {
	gin.SetMode(gin.TestMode)

	c, _ := gin.CreateTestContext(nil)
	assert.Nil(t, secretApproval(c), "no user, no approval")

	asCaller(c, approverLogin, false, false)
	assert.Equal(t, approverLogin, secretApproval(c).Login)
	assert.False(t, secretApproval(c).Session, "an API token approval is not a session")

	c.Set("browser-session", true)
	assert.True(t, secretApproval(c).Session)
}
