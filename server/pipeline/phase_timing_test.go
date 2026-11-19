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

package pipeline

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// TestLogPhaseDebugLine pins the shape of one phase-duration line: debug
// level, the phase name, a duration, and the repo and pipeline identity.
func TestLogPhaseDebugLine(t *testing.T) {
	var buf bytes.Buffer
	prev := log.Logger
	log.Logger = zerolog.New(&buf).Level(zerolog.DebugLevel)
	t.Cleanup(func() { log.Logger = prev })

	repo := &model.Repo{ID: 42, FullName: "RigelBuild/orion"}
	pl := &model.Pipeline{ID: 7, Number: 35583}

	done := startPhase(repo, pl, "store-insert")
	done(nil)

	var line map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &line), buf.String())
	assert.Equal(t, "debug", line["level"])
	assert.Equal(t, "store-insert", line["phase"])
	assert.Equal(t, "pipeline phase", line["message"])
	assert.Equal(t, "RigelBuild/orion", line["repo"])
	assert.EqualValues(t, 42, line["repo_id"])
	assert.EqualValues(t, 35583, line["number"])
	assert.Contains(t, line, "duration")
	assert.NotContains(t, line, "error")
}

// TestLogPhaseRecordsError pins that a phase which failed still logs its
// duration and carries the error, so a slow failure stays visible.
func TestLogPhaseRecordsError(t *testing.T) {
	var buf bytes.Buffer
	prev := log.Logger
	log.Logger = zerolog.New(&buf).Level(zerolog.DebugLevel)
	t.Cleanup(func() { log.Logger = prev })

	repo := &model.Repo{ID: 42, FullName: "RigelBuild/orion"}
	pl := &model.Pipeline{ID: 7, Number: 35583}

	done := startPhase(repo, pl, "config-fetch")
	done(errors.New("forge timeout"))

	var line map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &line), buf.String())
	assert.Equal(t, "config-fetch", line["phase"])
	assert.Equal(t, "pipeline phase", line["message"])
	assert.Equal(t, "RigelBuild/orion", line["repo"])
	assert.Equal(t, "forge timeout", line["error"])
	assert.Contains(t, line, "duration")
}
