// Copyright 2026 Woodpecker Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proto

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestWorkflowStateAgentShutdownWireField(t *testing.T) {
	t.Parallel()

	field := (&WorkflowState{}).ProtoReflect().Descriptor().Fields().ByName("agent_shutdown")
	require.NotNil(t, field)
	assert.Equal(t, protoreflect.FieldNumber(5), field.Number())

	encoded, err := proto.Marshal(&WorkflowState{AgentShutdown: true})
	require.NoError(t, err)

	var decoded WorkflowState
	require.NoError(t, proto.Unmarshal(encoded, &decoded))
	assert.True(t, decoded.GetAgentShutdown())
}
