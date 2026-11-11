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

//go:build test

package scenarios

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/e2e/setup"
)

// TestAgentRegisterNamesRowFromRequestBody registers a master-token agent,
// whose row starts nameless, with different body and metadata hostnames. The
// body value must reach the row through the real agent client and server.
func TestAgentRegisterNamesRowFromRequestBody(t *testing.T) {
	env := setup.StartServer(t.Context(), t, nil)

	agent := setup.StartAgent(t, env.GRPCAddr,
		setup.WithHostname("body-host"),
		setup.WithMetadataHostname("metadata-host"),
	)
	setup.WaitForAgentRegistered(t, env.Store, agent)

	row, err := env.Store.AgentFind(agent.AgentID)
	require.NoError(t, err)
	assert.Equal(t, "body-host", row.Name)
}
