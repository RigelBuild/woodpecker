// Copyright 2024 Woodpecker Authors
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
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline"
)

func TestGenerateNewAgentToken(t *testing.T) {
	token1 := GenerateNewAgentToken()
	token2 := GenerateNewAgentToken()

	assert.NotEmpty(t, token1)
	assert.NotEmpty(t, token2)
	assert.NotEqual(t, token1, token2)
	assert.Len(t, token1, 56)
}

func TestAgent_GetServerLabels(t *testing.T) {
	t.Run("EmptyAgent", func(t *testing.T) {
		agent := &Agent{}
		filters, err := agent.GetServerLabels()
		assert.NoError(t, err)
		assert.Equal(t, map[string]string{
			pipeline.LabelFilterOrg: "0",
		}, filters)
	})

	t.Run("GlobalAgent", func(t *testing.T) {
		agent := &Agent{
			OrgID: IDNotSet,
		}
		filters, err := agent.GetServerLabels()
		assert.NoError(t, err)
		assert.Equal(t, map[string]string{
			pipeline.LabelFilterOrg: "*",
		}, filters)
	})

	t.Run("OrgAgent", func(t *testing.T) {
		agent := &Agent{
			OrgID: 123,
		}
		filters, err := agent.GetServerLabels()
		assert.NoError(t, err)
		assert.Equal(t, map[string]string{
			pipeline.LabelFilterOrg: "123",
		}, filters)
	})

	t.Run("AgentWithFilters", func(t *testing.T) {
		agent := &Agent{
			OrgID:   IDNotSet,
			Filters: map[string]string{"gpu": "true"},
		}
		filters, err := agent.GetServerLabels()
		assert.NoError(t, err)
		assert.Equal(t, map[string]string{
			"gpu":                   "true",
			pipeline.LabelFilterOrg: "*",
		}, filters)
	})

	t.Run("OrgFilterCanNotBeOverwritten", func(t *testing.T) {
		agent := &Agent{
			OrgID:   123,
			Filters: map[string]string{pipeline.LabelFilterOrg: "456"},
		}
		filters, err := agent.GetServerLabels()
		assert.NoError(t, err)
		assert.Equal(t, "123", filters[pipeline.LabelFilterOrg])
	})

	t.Run("DoesNotMutateAgent", func(t *testing.T) {
		agent := &Agent{
			OrgID:   123,
			Filters: map[string]string{"gpu": "true"},
		}
		_, err := agent.GetServerLabels()
		assert.NoError(t, err)
		assert.Equal(t, map[string]string{"gpu": "true"}, agent.Filters)
	})
}

func TestAgentPollLabels(t *testing.T) {
	tests := []struct {
		name  string
		agent *Agent
		want  map[string]string
	}{
		{
			name:  "defaults with wildcard hostname",
			agent: &Agent{OrgID: IDNotSet, Platform: "linux/amd64", Backend: "docker"},
			want: map[string]string{
				pipeline.LabelFilterHostname: "*", pipeline.LabelFilterPlatform: "linux/amd64",
				pipeline.LabelFilterBackend: "docker", pipeline.LabelFilterRepo: "*", pipeline.LabelFilterOrg: "*",
			},
		},
		{
			name: "custom labels override defaults and server filters override both",
			agent: &Agent{
				OrgID: 7, Platform: "linux/amd64", Backend: "docker",
				CustomLabels: map[string]string{pipeline.LabelFilterHostname: "builder-1", "type": "gpu", pipeline.LabelFilterOrg: "9", "pool": "a"},
				Filters:      map[string]string{"pool": "b"},
			},
			want: map[string]string{
				pipeline.LabelFilterHostname: "builder-1", pipeline.LabelFilterPlatform: "linux/amd64",
				pipeline.LabelFilterBackend: "docker", pipeline.LabelFilterRepo: "*", pipeline.LabelFilterOrg: "7",
				"type": "gpu", "pool": "b",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			custom := maps.Clone(tt.agent.CustomLabels)
			got, err := tt.agent.PollLabels()
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
			if got != nil {
				got["mutated"] = "yes"
			}
			assert.Equal(t, custom, tt.agent.CustomLabels, "custom labels unchanged")
		})
	}
}

func TestAgent_CanAccessRepo(t *testing.T) {
	repo := &Repo{ID: 123, OrgID: 12}
	otherRepo := &Repo{ID: 456, OrgID: 45}

	t.Run("EmptyAgent", func(t *testing.T) {
		agent := &Agent{}
		assert.False(t, agent.CanAccessRepo(repo))
	})

	t.Run("GlobalAgent", func(t *testing.T) {
		agent := &Agent{
			OrgID: IDNotSet,
		}

		assert.True(t, agent.CanAccessRepo(repo))
	})

	t.Run("OrgAgent", func(t *testing.T) {
		agent := &Agent{
			OrgID: 12,
		}
		assert.True(t, agent.CanAccessRepo(repo))
		assert.False(t, agent.CanAccessRepo(otherRepo))
	})
}
