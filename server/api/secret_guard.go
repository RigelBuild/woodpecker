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
	"errors"
	"net/http"
	"slices"

	"github.com/gin-gonic/gin"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
	"go.woodpecker-ci.org/woodpecker/v3/server/router/middleware/session"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

// secretApproval builds the approval for the caller. Session is true only for a
// CSRF-verified browser session, so an API or JWT token never releases a secret.
func secretApproval(c *gin.Context) *pipeline.Approval {
	user := session.User(c)
	if user == nil {
		return nil
	}
	return &pipeline.Approval{Login: user.Login, Session: session.BrowserSession(c)}
}

// isSecretApprover reports whether the caller may create, change or delete a gated secret.
func isSecretApprover(c *gin.Context) bool {
	approval := secretApproval(c)
	return approval != nil && approval.Session && slices.Contains(server.Config.Pipeline.SecretApprovers, approval.Login)
}

// denyGatedSecretChange writes 403 when a gated secret is touched by anyone but an
// approver's browser session. Either side being gated counts, so a gate cannot be
// added or removed by a non-approver.
func denyGatedSecretChange(c *gin.Context, before, after *model.Secret) bool {
	gated := (before != nil && before.RequireApproval) || (after != nil && after.RequireApproval)
	if gated && !isSecretApprover(c) {
		c.String(http.StatusForbidden, "Only a secret approver's browser session can change a gated secret")
		return true
	}
	return false
}

// denySecretFilterWidening writes 403 when a non-admin widens a secret's events or
// loosens or clears its branch or image filter, which would expose it to more runs.
func denySecretFilterWidening(c *gin.Context, admin bool, before, after *model.Secret) bool {
	if admin {
		return false
	}
	if eventsWidened(before.Events, after.Events) ||
		filterLoosened(before.Branches, after.Branches) ||
		filterLoosened(before.Images, after.Images) {
		c.String(http.StatusForbidden, "Only an admin can widen a secret's events or loosen its branch or image filter")
		return true
	}
	return false
}

func eventsWidened(before, after []model.WebhookEvent) bool {
	for _, event := range after {
		if !slices.Contains(before, event) {
			return true
		}
	}
	return false
}

// filterLoosened treats an empty list as "everything", so clearing a filter loosens it.
func filterLoosened(before, after []string) bool {
	if len(before) == 0 {
		return false
	}
	if len(after) == 0 {
		return true
	}
	for _, item := range after {
		if !slices.Contains(before, item) {
			return true
		}
	}
	return false
}

// writeSecretUpdateErr reports a failed update; a stale read gets 409 so the
// caller re-reads instead of overwriting a newer row.
func writeSecretUpdateErr(c *gin.Context, name string, err error) {
	if errors.Is(err, types.ErrUpdateConflict) {
		c.String(http.StatusConflict, "Secret %q changed since it was read, retry the update", name)
		return
	}
	c.String(http.StatusInternalServerError, "Error updating secret %q. %s", name, err)
}

// deleteGuardedSecret deletes the row the gate was checked against. If the row
// changed first, the gate is checked again on the current row before reporting 409.
func deleteGuardedSecret(c *gin.Context, checked *model.Secret, remove func(*model.Secret) error, reload func() (*model.Secret, error)) {
	err := remove(checked)
	if errors.Is(err, types.ErrUpdateConflict) {
		current, findErr := reload()
		if findErr != nil {
			handleDBError(c, findErr)
			return
		}
		if denyGatedSecretChange(c, current, nil) {
			return
		}
		c.String(http.StatusConflict, "Secret %q changed since it was read, retry the delete", checked.Name)
		return
	}
	if err != nil {
		handleDBError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

// applySecretPatch copies the patch onto a stored secret. Branches replace the
// list when sent, so an empty list clears the filter.
func applySecretPatch(secret *model.Secret, in *model.SecretPatch) {
	if in.Value != nil && *in.Value != "" {
		secret.Value = *in.Value
	}
	if in.Events != nil {
		secret.Events = in.Events
	}
	if in.Images != nil {
		secret.Images = in.Images
	}
	if in.RequireApproval != nil {
		secret.RequireApproval = *in.RequireApproval
	}
	if in.Branches != nil {
		secret.Branches = in.Branches
	}
	if in.Note != nil {
		secret.Note = *in.Note
	}
}

// isServerAdmin is the admin the filter guard trusts. Org and global routes already
// require org or server admin, so only a server admin keeps the rule meaningful at
// every level.
func isServerAdmin(c *gin.Context) bool {
	user := session.User(c)
	return user != nil && user.Admin
}
