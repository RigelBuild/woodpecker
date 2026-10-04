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

package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

func TestSupersedes(t *testing.T) {
	pr := func(id, number int64, commit string) *model.Pipeline {
		return &model.Pipeline{ID: id, Number: number, Commit: commit, Event: model.EventPull, Refspec: "feat:main"}
	}
	push := func(id, number int64, commit, branch string) *model.Pipeline {
		return &model.Pipeline{ID: id, Number: number, Commit: commit, Event: model.EventPush, Branch: branch}
	}
	other := pr(2, 2, "bbb")
	restart := pr(3, 3, "aaa")
	restart.Parent = 1
	staleRestart := pr(1, 1, "aaa")
	staleRestart.Parent = 1
	other.Refspec = "other:main"

	tests := []struct {
		name           string
		pipeline, prev *model.Pipeline
		want           bool
	}{
		{"newer push to the PR cancels the older one", pr(2, 2, "bbb"), pr(1, 1, "aaa"), true},
		{"older pipeline never cancels a newer one", pr(1, 1, "aaa"), pr(2, 2, "bbb"), false},
		{"two pipelines for one commit both run", pr(2, 2, "aaa"), pr(1, 1, "aaa"), false},
		{"the reverse pair also leaves both running", pr(1, 1, "aaa"), pr(2, 2, "aaa"), false},
		{"a restart cancels the older run of its commit", restart, pr(1, 1, "aaa"), true},
		{"an older restart never cancels a newer run", staleRestart, pr(2, 2, "aaa"), false},
		{"itself", pr(1, 1, "aaa"), pr(1, 1, "aaa"), false},
		{"a different PR", other, pr(1, 1, "aaa"), false},
		{"a different event", pr(2, 2, "bbb"), push(1, 1, "aaa", "main"), false},
		{"newer push on the branch", push(2, 2, "bbb", "main"), push(1, 1, "aaa", "main"), true},
		{"push on another branch", push(2, 2, "bbb", "main"), push(1, 1, "aaa", "dev"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, supersedes(tt.pipeline, tt.prev))
		})
	}
}
