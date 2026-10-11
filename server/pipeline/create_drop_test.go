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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

// The server runs at info level, so a dropped pipeline must log at info or above with
// enough identity to explain the drop from the journal alone.
func TestDropCreatedPipelineLogsAtInfo(t *testing.T) {
	var buf bytes.Buffer
	prev := log.Logger
	log.Logger = zerolog.New(&buf).Level(zerolog.InfoLevel)
	t.Cleanup(func() { log.Logger = prev })

	mockStore := store_mocks.NewMockStore(t)
	pl := &model.Pipeline{ID: 7, Number: 35583, Event: model.EventPull, Ref: "refs/pull/3572/head", Commit: "116e079f"}
	mockStore.On("DeletePipeline", pl).Return(nil)

	dropCreatedPipeline(mockStore, &model.Repo{FullName: "RigelBuild/orion"}, pl, "no-workflows", errors.New("why"))

	var line map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &line), buf.String())
	assert.Equal(t, "info", line["level"])
	assert.Equal(t, "RigelBuild/orion", line["repo"])
	assert.EqualValues(t, 35583, line["number"])
	assert.Equal(t, "pull_request", line["event"])
	assert.Equal(t, "refs/pull/3572/head", line["ref"])
	assert.Equal(t, "116e079f", line["commit"])
	assert.Equal(t, "no-workflows", line["reason"])
	assert.Equal(t, "why", line["error"])
	mockStore.AssertExpectations(t)
}

func TestDropCreatedPipelineReportsDeleteFailure(t *testing.T) {
	var buf bytes.Buffer
	prev := log.Logger
	log.Logger = zerolog.New(&buf).Level(zerolog.InfoLevel)
	t.Cleanup(func() { log.Logger = prev })

	mockStore := store_mocks.NewMockStore(t)
	mockStore.On("DeletePipeline", mock.Anything).Return(errors.New("db down"))

	dropCreatedPipeline(mockStore, &model.Repo{FullName: "r"}, &model.Pipeline{Number: 1}, "config-not-found", nil)

	assert.Contains(t, buf.String(), `"level":"error"`)
	assert.Contains(t, buf.String(), "db down")
}
