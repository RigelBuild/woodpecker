// Copyright 2022 Woodpecker Authors
// Copyright 2018 Drone.IO Inc.
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

package datastore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

func TestSecretFind(t *testing.T) {
	store, closer := newTestStore(t, new(model.Secret))
	defer closer()

	err := store.SecretCreate(&model.Secret{
		RepoID: 1,
		Name:   "password",
		Value:  "correct-horse-battery-staple",
		Images: []string{"golang", "node"},
		Events: []model.WebhookEvent{"push", "tag"},
	})
	assert.NoError(t, err)

	secret, err := store.SecretFind(&model.Repo{ID: 1}, "password")
	assert.NoError(t, err)
	assert.EqualValues(t, 1, secret.RepoID)
	assert.Equal(t, "password", secret.Name)
	assert.Equal(t, "correct-horse-battery-staple", secret.Value)
	assert.Equal(t, model.EventPush, secret.Events[0])
	assert.Equal(t, model.EventTag, secret.Events[1])
	assert.Equal(t, "golang", secret.Images[0])
	assert.Equal(t, "node", secret.Images[1])
}

func TestSecretList(t *testing.T) {
	store, closer := newTestStore(t, new(model.Secret))
	defer closer()

	createTestSecrets(t, store)

	list, err := store.SecretList(&model.Repo{ID: 1, OrgID: 12}, false, &model.ListOptionsWithAll{ListOptions: &model.ListOptions{Page: 1, PerPage: 50}})
	assert.NoError(t, err)
	assert.Len(t, list, 2)
}

func TestSecretListAll(t *testing.T) {
	store, closer := newTestStore(t, new(model.Secret))
	defer closer()

	createTestSecrets(t, store)

	list, err := store.SecretListAll()
	assert.NoError(t, err)
	assert.Len(t, list, 4)
}

func TestSecretPipelineList(t *testing.T) {
	store, closer := newTestStore(t, new(model.Secret))
	defer closer()

	createTestSecrets(t, store)

	list, err := store.SecretList(&model.Repo{ID: 1, OrgID: 12}, true, &model.ListOptionsWithAll{ListOptions: &model.ListOptions{Page: 1, PerPage: 50}})
	assert.NoError(t, err)
	assert.Len(t, list, 4)
}

func TestSecretUpdate(t *testing.T) {
	store, closer := newTestStore(t, new(model.Secret))
	defer closer()

	secret := &model.Secret{
		RepoID: 1,
		Name:   "foo",
		Value:  "baz",
	}
	assert.NoError(t, store.SecretCreate(secret))
	secret.Value = "qux"
	assert.EqualValues(t, 1, secret.ID)
	assert.NoError(t, store.SecretUpdate(secret))
	updated, err := store.SecretFind(&model.Repo{ID: 1}, "foo")
	assert.NoError(t, err)
	assert.Equal(t, "qux", updated.Value)
}

func TestSecretDelete(t *testing.T) {
	store, closer := newTestStore(t, new(model.Secret))
	defer closer()

	secret := &model.Secret{
		RepoID: 1,
		Name:   "foo",
		Value:  "baz",
	}
	assert.NoError(t, store.SecretCreate(secret))

	assert.NoError(t, store.SecretDelete(secret))
	_, err := store.SecretFind(&model.Repo{ID: 1}, "foo")
	assert.Error(t, err)
}

func TestSecretIndexes(t *testing.T) {
	store, closer := newTestStore(t, new(model.Secret))
	defer closer()

	assert.NoError(t, store.SecretCreate(&model.Secret{
		RepoID: 1,
		Name:   "foo",
		Value:  "bar",
	}))

	// fail due to duplicate name
	assert.Error(t, store.SecretCreate(&model.Secret{
		RepoID: 1,
		Name:   "foo",
		Value:  "baz",
	}))
}

func createTestSecrets(t *testing.T, store *storage) {
	assert.NoError(t, store.SecretCreate(&model.Secret{
		OrgID: 12,
		Name:  "usr",
		Value: "sec",
	}))
	assert.NoError(t, store.SecretCreate(&model.Secret{
		RepoID: 1,
		Name:   "foo",
		Value:  "bar",
	}))
	assert.NoError(t, store.SecretCreate(&model.Secret{
		RepoID: 1,
		Name:   "baz",
		Value:  "qux",
	}))
	assert.NoError(t, store.SecretCreate(&model.Secret{
		Name:  "global",
		Value: "val",
	}))
}

func TestOrgSecretFind(t *testing.T) {
	store, closer := newTestStore(t, new(model.Secret))
	defer closer()

	err := store.SecretCreate(&model.Secret{
		OrgID:  12,
		Name:   "password",
		Value:  "correct-horse-battery-staple",
		Images: []string{"golang", "node"},
		Events: []model.WebhookEvent{"push", "tag"},
	})
	assert.NoError(t, err)

	secret, err := store.OrgSecretFind(12, "password")
	assert.NoError(t, err)
	assert.EqualValues(t, 12, secret.OrgID)
	assert.Equal(t, "password", secret.Name)
	assert.Equal(t, "correct-horse-battery-staple", secret.Value)
	assert.Equal(t, model.EventPush, secret.Events[0])
	assert.Equal(t, model.EventTag, secret.Events[1])
	assert.Equal(t, "golang", secret.Images[0])
	assert.Equal(t, "node", secret.Images[1])
}

func TestOrgSecretList(t *testing.T) {
	store, closer := newTestStore(t, new(model.Secret))
	defer closer()

	createTestSecrets(t, store)

	list, err := store.OrgSecretList(12, &model.ListOptionsWithAll{All: true})
	assert.NoError(t, err)
	assert.Len(t, list, 1)

	assert.True(t, list[0].IsOrganization())
}

func TestGlobalSecretFind(t *testing.T) {
	store, closer := newTestStore(t, new(model.Secret))
	defer closer()

	err := store.SecretCreate(&model.Secret{
		Name:   "password",
		Value:  "correct-horse-battery-staple",
		Images: []string{"golang", "node"},
		Events: []model.WebhookEvent{"push", "tag"},
	})
	assert.NoError(t, err)

	secret, err := store.GlobalSecretFind("password")
	assert.NoError(t, err)
	assert.Equal(t, "password", secret.Name)
	assert.Equal(t, "correct-horse-battery-staple", secret.Value)
	assert.Equal(t, model.EventPush, secret.Events[0])
	assert.Equal(t, model.EventTag, secret.Events[1])
	assert.Equal(t, "golang", secret.Images[0])
	assert.Equal(t, "node", secret.Images[1])
}

func TestGlobalSecretList(t *testing.T) {
	store, closer := newTestStore(t, new(model.Secret))
	defer closer()

	createTestSecrets(t, store)

	list, err := store.GlobalSecretList(&model.ListOptionsWithAll{All: true})
	assert.NoError(t, err)
	assert.Len(t, list, 1)

	assert.True(t, list[0].IsGlobal())
}

// A PATCH writes back a row it read earlier, so a write based on a stale read must
// fail instead of silently undoing a concurrent gate change.
func TestSecretUpdateRejectsStaleRow(t *testing.T) {
	store, closer := newTestStore(t, new(model.Secret))
	defer closer()

	require.NoError(t, store.SecretCreate(&model.Secret{RepoID: 1, Name: "foo", Value: "baz", Events: []model.WebhookEvent{model.EventPush}}))
	approverRead, err := store.SecretFind(&model.Repo{ID: 1}, "foo")
	require.NoError(t, err)
	staleRead, err := store.SecretFind(&model.Repo{ID: 1}, "foo")
	require.NoError(t, err)

	approverRead.RequireApproval = true
	require.NoError(t, store.SecretUpdate(approverRead))

	staleRead.Value = "stale"
	assert.ErrorIs(t, store.SecretUpdate(staleRead), types.ErrUpdateConflict)

	stored, err := store.SecretFind(&model.Repo{ID: 1}, "foo")
	require.NoError(t, err)
	assert.True(t, stored.RequireApproval, "a stale write must not remove the gate")
	assert.Equal(t, "baz", stored.Value)

	stored.Value = "fresh"
	assert.NoError(t, store.SecretUpdate(stored), "a write based on the current row succeeds")
}

// A delete is guarded against the row the caller read, so a stale read must not
// remove a secret that was gated after the read.
func TestSecretDeleteRejectsStaleRow(t *testing.T) {
	store, closer := newTestStore(t, new(model.Secret))
	defer closer()

	require.NoError(t, store.SecretCreate(&model.Secret{RepoID: 1, Name: "foo", Value: "baz", Events: []model.WebhookEvent{model.EventPush}}))
	staleRead, err := store.SecretFind(&model.Repo{ID: 1}, "foo")
	require.NoError(t, err)
	approverRead, err := store.SecretFind(&model.Repo{ID: 1}, "foo")
	require.NoError(t, err)
	approverRead.RequireApproval = true
	require.NoError(t, store.SecretUpdate(approverRead))

	assert.ErrorIs(t, store.SecretDelete(staleRead), types.ErrUpdateConflict)
	stored, err := store.SecretFind(&model.Repo{ID: 1}, "foo")
	require.NoError(t, err, "a stale delete must leave the row")
	assert.True(t, stored.RequireApproval)

	require.NoError(t, store.SecretDelete(stored), "a delete of the current row succeeds")
	assert.ErrorIs(t, store.SecretDelete(stored), types.ErrRecordNotExist)
}
