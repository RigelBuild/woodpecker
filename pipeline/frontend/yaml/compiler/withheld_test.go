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

package compiler

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/metadata"
	yaml_types "go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml/types"
)

func withheldWorkflow(secretName string) *yaml_types.Workflow {
	return &yaml_types.Workflow{Steps: yaml_types.ContainerList{ContainerList: []*yaml_types.Container{{
		Name:     "deploy",
		Image:    "bash",
		Commands: []string{"env"},
		Environment: map[string]any{
			"TOKEN": map[string]any{"from_secret": secretName},
		},
	}}}}
}

func TestCompilerWithheldSecretBlocksForApproval(t *testing.T) {
	t.Parallel()

	compiler := New(
		WithMetadata(metadata.Metadata{Curr: metadata.Pipeline{Event: metadata.EventPush}}),
		WithSecret(Secret{Name: "OTHER", Value: "other-value"}),
		WithWithheldSecrets("PULUMI_GATED_ACCESS_TOKEN"),
		WithPrefix("test"),
	)

	conf, err := compiler.Compile(withheldWorkflow("pulumi_gated_access_token"))
	require.Error(t, err)
	assert.Nil(t, conf)

	withheld, ok := errors.AsType[*ErrSecretWithheld](err)
	require.True(t, ok, "error must be ErrSecretWithheld, got %v", err)
	assert.Equal(t, "PULUMI_GATED_ACCESS_TOKEN", withheld.Name)

	pipelineErrs := pipeline_errors.GetPipelineErrors(err)
	require.Len(t, pipelineErrs, 1)
	assert.Equal(t, pipeline_errors.PipelineErrorTypeApproval, pipelineErrs[0].Type)
	assert.False(t, pipelineErrs[0].IsWarning)
	assert.Equal(t, &pipeline_errors.ApprovalErrorData{Name: "PULUMI_GATED_ACCESS_TOKEN"}, pipelineErrs[0].Data)
	assert.True(t, OnlyApprovalErrors(err))
}

func TestCompilerWithheldSecretNeverReachesConfigSecrets(t *testing.T) {
	t.Parallel()

	compiler := New(
		WithMetadata(metadata.Metadata{Curr: metadata.Pipeline{Event: metadata.EventPush}}),
		WithSecret(Secret{Name: "OTHER", Value: "other-value"}),
		WithWithheldSecrets("PULUMI_GATED_ACCESS_TOKEN"),
		WithPrefix("test"),
	)

	conf, err := compiler.Compile(withheldWorkflow("OTHER"))
	require.NoError(t, err)
	for _, sec := range conf.Secrets {
		assert.NotEqual(t, "PULUMI_GATED_ACCESS_TOKEN", sec.Name)
	}
}

func TestOnlyApprovalErrors(t *testing.T) {
	t.Parallel()

	approval := &ErrSecretWithheld{Name: "A"}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "approval", err: approval, want: true},
		{name: "approval and warning", err: multierr.Append(approval, &pipeline_errors.PipelineError{IsWarning: true}), want: true},
		{name: "approval and compiler error", err: multierr.Append(approval, &pipeline_errors.PipelineError{Type: pipeline_errors.PipelineErrorTypeCompiler}), want: false},
		{name: "generic error", err: errors.New("boom"), want: false},
		{name: "warning only", err: &pipeline_errors.PipelineError{IsWarning: true}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, OnlyApprovalErrors(tt.err))
		})
	}
}
