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

package model

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPipelineToAPIModel(t *testing.T) {
	tests := []struct {
		name             string
		pipeline         Pipeline
		wantTitle        string
		wantMessage      string
		wantSender       string
		wantIsPrerelease bool
	}{
		{
			name:        "cron uses cron name as message and sender",
			pipeline:    Pipeline{Event: EventCron, Cron: "nightly"},
			wantMessage: "nightly",
			wantSender:  "nightly",
		},
		{
			name:        "tag uses tag title in message",
			pipeline:    Pipeline{Event: EventTag, TagTitle: "v1.2.3"},
			wantMessage: "created tag v1.2.3",
		},
		{
			name:        "release without release object falls back to tag title",
			pipeline:    Pipeline{Event: EventRelease, TagTitle: "v2.0.0"},
			wantMessage: "created release v2.0.0",
		},
		{
			name: "release with release object uses release title and prerelease flag",
			pipeline: Pipeline{
				Event:    EventRelease,
				TagTitle: "v2.0.0",
				Release:  &Release{Title: "Release 2.0", IsPrerelease: true},
			},
			wantTitle:        "Release 2.0",
			wantMessage:      "created release Release 2.0",
			wantIsPrerelease: true,
		},
		{
			name:     "push leaves derived fields untouched",
			pipeline: Pipeline{Event: EventPush, Message: "fix bug"},
			// message is the stored commit message, not overwritten
			wantMessage: "fix bug",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := tc.pipeline
			ap := p.ToAPIModel()
			assert.Equal(t, tc.wantTitle, ap.Title)
			assert.Equal(t, tc.wantMessage, ap.Message)
			assert.Equal(t, tc.wantSender, ap.Sender)
			assert.Equal(t, tc.wantIsPrerelease, ap.IsPrerelease)
		})
	}
}

func TestPipelineBeforeJSON(t *testing.T) {
	t.Run("before is carried in the payload when set", func(t *testing.T) {
		raw, err := json.Marshal(Pipeline{
			Commit: "366701fde727cb7a9e7f21eb88264f59f6f9b89c",
			Before: "2f780193b136b72bfea4eeb640786a8c4450c7a2",
		})
		require.NoError(t, err)

		var payload map[string]any
		require.NoError(t, json.Unmarshal(raw, &payload))
		assert.Equal(t, "2f780193b136b72bfea4eeb640786a8c4450c7a2", payload["before"])
	})

	t.Run("before is omitted from the payload when empty", func(t *testing.T) {
		raw, err := json.Marshal(Pipeline{Commit: "366701fde727cb7a9e7f21eb88264f59f6f9b89c"})
		require.NoError(t, err)

		var payload map[string]any
		require.NoError(t, json.Unmarshal(raw, &payload))
		// commit has no omitempty, so its presence proves the payload really was
		// inspected and "before" is absent by the tag, not by a broken decode.
		assert.Contains(t, payload, "commit")
		assert.NotContains(t, payload, "before")
	})
}
