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

package pipeline

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/urfave/cli/v3"

	"go.woodpecker-ci.org/woodpecker/v3/woodpecker-go/woodpecker"
	"go.woodpecker-ci.org/woodpecker/v3/woodpecker-go/woodpecker/mocks"
)

func TestPipelineQueue(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	mockClient.On("PipelineQueue").Return([]*woodpecker.Feed{{
		RepoID: 2,
		Number: 7,
	}}, nil)

	command := *pipelineQueueCmd
	command.Writer = io.Discard
	command.Action = func(_ context.Context, c *cli.Command) error {
		var out bytes.Buffer
		err := pipelineQueueOutput(c, mockClient, &out)
		assert.NoError(t, err)
		assert.Contains(t, out.String(), "repo:2 #7")
		return nil
	}

	assert.NoError(t, command.Run(t.Context(), []string{"queue"}))
}
