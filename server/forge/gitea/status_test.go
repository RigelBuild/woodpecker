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

package gitea

import (
	"testing"

	"code.gitea.io/sdk/gitea"
	"github.com/stretchr/testify/assert"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

func TestGetStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		status model.StatusValue
		want   gitea.StatusState
	}{
		{model.StatusPending, gitea.StatusPending},
		{model.StatusBlocked, gitea.StatusPending},
		{model.StatusCreated, gitea.StatusPending},
		{model.StatusRunning, gitea.StatusPending},
		{model.StatusSuccess, gitea.StatusSuccess},
		{model.StatusFailure, gitea.StatusFailure},
		{model.StatusKilled, gitea.StatusFailure},
		{model.StatusDeclined, gitea.StatusWarning},
		{model.StatusError, gitea.StatusError},
		{model.StatusValue("bogus"), gitea.StatusFailure},
	}

	for _, tt := range tests {
		assert.Equalf(t, tt.want, getStatus(tt.status), "status %q", tt.status)
	}
}
