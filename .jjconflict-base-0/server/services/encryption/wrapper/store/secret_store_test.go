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

package store_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/services/encryption/wrapper/store"
	store_types "go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

// prefixEncryption is a reversible stand-in cipher for wrapper tests.
type prefixEncryption struct{}

func (prefixEncryption) Encrypt(plaintext, _ string) (string, error) { return "enc:" + plaintext, nil }

func (prefixEncryption) Decrypt(ciphertext, _ string) (string, error) {
	return strings.TrimPrefix(ciphertext, "enc:"), nil
}

func (prefixEncryption) Disable() error { return nil }

// versionedSecrets keeps one row per ID and applies the datastore's optimistic
// lock: insert stores version 1, and an update must carry the stored version.
type versionedSecrets struct {
	model.SecretStore
	rows map[int64]model.Secret
}

func (s *versionedSecrets) SecretCreate(secret *model.Secret) error {
	secret.ID = int64(len(s.rows) + 1)
	secret.Version = 1
	s.rows[secret.ID] = *secret
	return nil
}

func (s *versionedSecrets) SecretUpdate(secret *model.Secret) error {
	if s.rows[secret.ID].Version != secret.Version {
		return store_types.ErrUpdateConflict
	}
	secret.Version++
	s.rows[secret.ID] = *secret
	return nil
}

func (s *versionedSecrets) SecretDelete(secret *model.Secret) error {
	delete(s.rows, secret.ID)
	return nil
}

// With encryption on, create writes the row twice; the second write must carry
// the version the first one stored, or the optimistic lock rejects it.
func TestEncryptedSecretCreateCarriesStoredVersion(t *testing.T) {
	rows := &versionedSecrets{rows: map[int64]model.Secret{}}
	s := store.NewSecretStore(rows)
	require.NoError(t, s.SetEncryptionService(prefixEncryption{}))

	secret := &model.Secret{RepoID: 1, Name: "TOKEN", Value: "plain"}
	require.NoError(t, s.SecretCreate(secret))
	assert.Equal(t, "plain", secret.Value)
	require.Len(t, rows.rows, 1, "a failed create must not leave or drop rows silently")
	stored := rows.rows[secret.ID]
	assert.Equal(t, "enc:plain", stored.Value)
	assert.Equal(t, stored.Version, secret.Version, "the caller's copy must match the stored version")
}
