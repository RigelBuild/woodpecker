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

package core

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	agent_rpc "go.woodpecker-ci.org/woodpecker/v3/agent/rpc"
	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
)

func TestAgentRootContextSignalCause(t *testing.T) {
	t.Parallel()

	signals := make(chan os.Signal, 1)
	ctx := AgentRootContext(t.Context(), signals)
	signals <- syscall.SIGTERM

	select {
	case <-ctx.Done():
		assert.ErrorIs(t, context.Cause(ctx), pipeline_errors.ErrAgentShutdown)
		assert.ErrorIs(t, context.Cause(ctx), pipeline_errors.ErrCancel)
	case <-time.After(time.Second):
		t.Fatal("root context was not canceled by SIGTERM")
	}
}

func TestCheckServerProtoVersion(t *testing.T) {
	t.Parallel()

	assert.Equal(t, int32(17), agent_rpc.ClientGrpcVersion)
	for _, tt := range []struct {
		name        string
		serverProto int32
		wantErr     bool
	}{
		{name: "matching version", serverProto: 17},
		{name: "older version", serverProto: 16, wantErr: true},
		{name: "newer version", serverProto: 18, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := checkServerProtoVersion(tt.serverProto)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
