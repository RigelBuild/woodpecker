// Copyright 2023 Woodpecker Authors
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

package model

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStepStatus(t *testing.T) {
	step := &Step{State: StatusPending}
	assert.Equal(t, step.Running(), true)
	step.State = StatusRunning
	assert.Equal(t, step.Running(), true)

	step.Failure = FailureIgnore
	step.State = StatusError
	assert.Equal(t, step.Failing(), true)
	step.State = StatusFailure
	assert.Equal(t, step.Failing(), true)
	step.Failure = FailureFail
	step.State = StatusError
	assert.Equal(t, step.Failing(), true)
	step.State = StatusFailure
	assert.Equal(t, step.Failing(), true)
	step.State = StatusPending
	assert.Equal(t, step.Failing(), false)
	step.State = StatusSuccess
	assert.Equal(t, step.Failing(), false)
}

func TestStepOutputsJSON(t *testing.T) {
	for _, tt := range []struct {
		name    string
		outputs map[string]string
		want    string
	}{
		{name: "absent"},
		{name: "empty", outputs: map[string]string{}, want: `{}`},
		{name: "present", outputs: map[string]string{"key": "value"}, want: `{"key":"value"}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(Step{Outputs: tt.outputs})
			require.NoError(t, err)
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(data, &fields))
			if tt.outputs == nil {
				assert.NotContains(t, fields, "outputs")
			} else {
				assert.JSONEq(t, tt.want, string(fields["outputs"]))
			}
		})
	}
}
