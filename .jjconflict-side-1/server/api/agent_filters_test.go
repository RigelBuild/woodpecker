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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pubsub/memory"
	queue_mocks "go.woodpecker-ci.org/woodpecker/v3/server/queue/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/scheduler"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

// A cordon PATCH sends only no_schedule, so omitted filters must survive; the
// UI clears the last filter by sending an empty map.
func TestPatchAgentStoresFilters(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name string
		body string
		want map[string]string
		kick bool
	}{
		{name: "omitted filters are kept", body: `{"no_schedule":false}`, want: map[string]string{"gpu": "true"}},
		{name: "empty map clears filters", body: `{"filters":{}}`, want: map[string]string{}, kick: true},
		{name: "new filters replace old", body: `{"filters":{"gpu":"false"}}`, want: map[string]string{"gpu": "false"}, kick: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			agent := &model.Agent{ID: 1, Name: "test-agent", Filters: map[string]string{"gpu": "true"}}

			var stored *model.Agent
			mockStore := store_mocks.NewMockStore(t)
			mockStore.On("AgentFind", int64(1)).Return(agent, nil)
			mockStore.On("AgentUpdate", mock.AnythingOfType("*model.Agent")).
				Run(func(args mock.Arguments) {
					a, ok := args.Get(0).(*model.Agent)
					require.True(t, ok)
					stored = a
				}).
				Return(nil)

			// NewMockQueue fails the test on an unexpected KickAgentWorkers call.
			mockQueue := queue_mocks.NewMockQueue(t)
			if tc.kick {
				mockQueue.On("KickAgentWorkers", int64(1)).Return()
			}
			server.Config.Services.Scheduler = scheduler.NewScheduler(t.Context(), mockStore, mockQueue, memory.New())
			t.Cleanup(func() { server.Config.Services.Scheduler = nil })

			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Set("store", mockStore)
			c.Params = gin.Params{{Key: "agent_id", Value: "1"}}
			req, err := http.NewRequest(http.MethodPatch, "/", strings.NewReader(tc.body))
			require.NoError(t, err)
			c.Request = req
			c.Request.Header.Set("Content-Type", "application/json")

			PatchAgent(c)
			c.Writer.WriteHeaderNow()

			assert.Equal(t, http.StatusOK, w.Code)
			if assert.NotNil(t, stored) {
				assert.Equal(t, tc.want, stored.Filters)
				assert.Equal(t, "test-agent", stored.Name)
			}
		})
	}
}
