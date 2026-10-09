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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

const storedAgentToken = "stored-agent-secret"

func tokenAgent() *model.Agent {
	return &model.Agent{ID: 1, Name: "agent", OrgID: 7, Token: storedAgentToken}
}

func TestAgentResponsesOmitStoredToken(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name    string
		handler gin.HandlerFunc
		body    string
		setup   func(*store_mocks.MockStore, *model.Agent)
	}{
		{
			name:    "GET /agents",
			handler: GetAgents,
			setup: func(s *store_mocks.MockStore, a *model.Agent) {
				s.On("AgentList", mock.Anything).Return([]*model.Agent{a}, nil)
			},
		},
		{
			name:    "GET /agents/:id",
			handler: GetAgent,
			setup: func(s *store_mocks.MockStore, a *model.Agent) {
				s.On("AgentFind", int64(1)).Return(a, nil)
			},
		},
		{
			name:    "PATCH /agents/:id",
			handler: PatchAgent,
			body:    `{"name":"renamed"}`,
			setup: func(s *store_mocks.MockStore, a *model.Agent) {
				s.On("AgentFind", int64(1)).Return(a, nil)
				s.On("AgentUpdate", mock.AnythingOfType("*model.Agent")).Return(nil)
			},
		},
		{
			name:    "GET /orgs/:id/agents",
			handler: GetOrgAgents,
			setup: func(s *store_mocks.MockStore, a *model.Agent) {
				s.On("AgentListForOrg", int64(7), mock.Anything).Return([]*model.Agent{a}, nil)
			},
		},
		{
			name:    "PATCH /orgs/:id/agents/:id",
			handler: PatchOrgAgent,
			body:    `{"name":"renamed"}`,
			setup: func(s *store_mocks.MockStore, a *model.Agent) {
				s.On("AgentFind", int64(1)).Return(a, nil)
				s.On("AgentUpdate", mock.AnythingOfType("*model.Agent")).Return(nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stored := tokenAgent()
			s := store_mocks.NewMockStore(t)
			tt.setup(s, stored)

			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Set("store", s)
			c.Set("org", &model.Org{ID: 7})
			c.Params = gin.Params{{Key: "agent_id", Value: "1"}, {Key: "org_id", Value: "7"}}
			c.Request, _ = http.NewRequest(http.MethodGet, "/", strings.NewReader(tt.body))
			c.Request.Header.Set("Content-Type", "application/json")

			tt.handler(c)
			c.Writer.WriteHeaderNow()

			require.Equal(t, http.StatusOK, w.Code)
			assert.NotContains(t, w.Body.String(), storedAgentToken)
			// The redaction must not write through to the stored agent.
			assert.Equal(t, storedAgentToken, stored.Token)
		})
	}
}

func TestAgentCreateReturnsToken(t *testing.T) {
	gin.SetMode(gin.TestMode)

	for name, handler := range map[string]gin.HandlerFunc{"POST /agents": PostAgent, "POST /orgs/:id/agents": PostOrgAgent} {
		t.Run(name, func(t *testing.T) {
			s := store_mocks.NewMockStore(t)
			s.On("AgentCreate", mock.AnythingOfType("*model.Agent")).Return(nil)

			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Set("store", s)
			c.Set("user", &model.User{ID: 1})
			c.Params = gin.Params{{Key: "org_id", Value: "7"}}
			c.Request, _ = http.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":"new"}`))
			c.Request.Header.Set("Content-Type", "application/json")

			handler(c)
			c.Writer.WriteHeaderNow()

			require.Equal(t, http.StatusOK, w.Code)
			var got model.Agent
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
			assert.NotEmpty(t, got.Token)
		})
	}
}
