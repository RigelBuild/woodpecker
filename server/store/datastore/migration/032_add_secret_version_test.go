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

package migration

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// TestMigrateGivesOldSecretsAUsableVersion migrates a database whose secret rows
// predate the version column. Every old row must get a non-zero version, so the
// optimistic lock works on it: a write from a stale read affects no row.
func TestMigrateGivesOldSecretsAUsableVersion(t *testing.T) {
	engine, closeDB := testDB(t, false)
	defer closeDB()

	require.NoError(t, Migrate(t.Context(), engine, true))

	var secrets []*model.Secret
	require.NoError(t, engine.Find(&secrets))
	require.NotEmpty(t, secrets, "the fixture must carry secret rows from before the column existed")
	for _, s := range secrets {
		assert.NotZero(t, s.Version, "secret %d has no usable version", s.ID)
	}

	first := secrets[0]
	stale := *first
	first.Note = "fresh write"
	affected, err := engine.ID(first.ID).AllCols().Update(first)
	require.NoError(t, err)
	require.EqualValues(t, 1, affected, "a write from the current row must land")

	stale.Note = "stale write"
	affected, err = engine.ID(stale.ID).AllCols().Update(&stale)
	require.NoError(t, err)
	assert.Zero(t, affected, "a write from a stale read must affect no row")
}
