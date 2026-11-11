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

func TestMigrateStepOutputs(t *testing.T) {
	engine, closeDB := testDB(t, false)
	defer closeDB()
	require.NoError(t, Migrate(t.Context(), engine, true))
	var steps []model.Step
	require.NoError(t, engine.Find(&steps))
	require.NotEmpty(t, steps)
	assert.Nil(t, steps[0].Outputs)
	steps[0].Outputs = map[string]string{"key": "value"}
	steps[0].OutputsState = "reported"
	_, err := engine.ID(steps[0].ID).AllCols().Update(&steps[0])
	require.NoError(t, err)
	var got model.Step
	found, err := engine.ID(steps[0].ID).Get(&got)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, steps[0].Outputs, got.Outputs)
	assert.Equal(t, "reported", got.OutputsState)
}
