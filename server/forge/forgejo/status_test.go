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

package forgejo

import (
	"testing"

	"codeberg.org/mvdkleijn/forgejo-sdk/forgejo/v3"
	"github.com/stretchr/testify/assert"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

func TestGetStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		status model.StatusValue
		want   forgejo.StatusState
	}{
		{model.StatusPending, forgejo.StatusPending},
		{model.StatusBlocked, forgejo.StatusPending},
		{model.StatusCreated, forgejo.StatusPending},
		{model.StatusRunning, forgejo.StatusPending},
		{model.StatusSuccess, forgejo.StatusSuccess},
		{model.StatusFailure, forgejo.StatusFailure},
		{model.StatusKilled, forgejo.StatusFailure},
		{model.StatusDeclined, forgejo.StatusWarning},
		{model.StatusError, forgejo.StatusError},
		{model.StatusValue("bogus"), forgejo.StatusFailure},
	}

	for _, tt := range tests {
		assert.Equalf(t, tt.want, getStatus(tt.status), "status %q", tt.status)
	}
}
