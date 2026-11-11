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

package metadata

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
)

func TestEnviron(t *testing.T) {
	m := Metadata{
		Sys: System{Name: "wp"},
		Curr: Pipeline{
			Event: EventRelease,
			Release: Release{
				Title:        "v1.2.3",
				IsPrerelease: true,
			},
			Commit: Commit{
				Ref:       "refs/tags/v1.2.3",
				Timestamp: 1722617519,
			},
		},
		Prev: Pipeline{
			Event: EventPullMetadata,
			Commit: Commit{
				Refspec:   "branch-a:branch-b",
				Timestamp: 1722610173,
			},
		},
	}

	envs := m.Environ()
	assert.Equal(t, "wp", envs["CI"])
	assert.Equal(t, "release", envs["CI_PIPELINE_EVENT"])
	assert.Equal(t, "pull_request_metadata", envs["CI_PREV_PIPELINE_EVENT"])
	assert.Equal(t, "v1.2.3", envs["CI_PIPELINE_RELEASE_TITLE"])
	assert.Equal(t, "true", envs["CI_PIPELINE_RELEASE_PRE"])
	assert.Equal(t, "true", envs["CI_COMMIT_PRERELEASE"]) // deprecated alias
	assert.Equal(t, "branch-a", envs["CI_PREV_COMMIT_SOURCE_BRANCH"])
	assert.Equal(t, "branch-b", envs["CI_PREV_COMMIT_TARGET_BRANCH"])
	assert.Equal(t, "[]", envs["CI_PIPELINE_FILES"])
	assert.Equal(t, "v1.2.3", envs["CI_COMMIT_TAG"])
	assert.Equal(t, "1722617519", envs["CI_COMMIT_TIMESTAMP"])
	assert.Equal(t, "1722610173", envs["CI_PREV_COMMIT_TIMESTAMP"])

	m = Metadata{
		Sys: System{Name: "wp"},
		Curr: Pipeline{
			Event: EventPull,
			Commit: Commit{
				ChangedFiles:     []string{"readme", "license"},
				Refspec:          "branch-a:branch-b",
				PullRequestBody:  "Fixes #1",
				PullRequestDraft: true,
			},
		},
		Prev: Pipeline{
			Event: EventPull,
			Commit: Commit{
				Refspec: "branch-a:branch-b",
			},
		},
	}

	envs = m.Environ()

	_, ok := envs["CI_COMMIT_TAG"]
	assert.False(t, ok)
	assert.Equal(t, `["readme","license"]`, envs["CI_PIPELINE_FILES"])
	assert.Equal(t, "true", envs["CI_COMMIT_PULL_REQUEST_DRAFT"])
	assert.Equal(t, "Fixes #1", envs["CI_COMMIT_PULL_REQUEST_BODY"])

	m = Metadata{
		Sys:  System{Name: "wp"},
		Curr: Pipeline{Event: EventPullMetadata},
	}
	envs = m.Environ()
	_, ok = envs["CI_COMMIT_PULL_REQUEST_BODY"]
	assert.True(t, ok)
	assert.Empty(t, envs["CI_COMMIT_PULL_REQUEST_BODY"])

	m = Metadata{
		Sys: System{Name: "wp"},
		Curr: Pipeline{
			Event:  EventPush,
			Commit: Commit{PullRequestBody: "ignored for push"},
		},
	}
	envs = m.Environ()
	_, ok = envs["CI_COMMIT_PULL_REQUEST_BODY"]
	assert.False(t, ok)

	m = Metadata{
		Sys: System{Name: "wp"},
		Curr: Pipeline{
			Event: EventPull,
			Commit: Commit{
				Refspec: "branch-a:branch-b",
			},
		},
	}
	envs = m.Environ()
	_, ok = envs["CI_COMMIT_PULL_REQUEST_BODY"]
	assert.True(t, ok)
	assert.Empty(t, envs["CI_COMMIT_PULL_REQUEST_BODY"])
	assert.Equal(t, "false", envs["CI_COMMIT_PULL_REQUEST_DRAFT"])
}

func TestPullRequestBodyIsCappedOnARuneBoundary(t *testing.T) {
	// 3-byte runes, so the byte limit falls inside a rune.
	body := "Spec-impact: none\n" + strings.Repeat("€", maxPullRequestBodyBytes)
	m := Metadata{Curr: Pipeline{Event: EventPull, Commit: Commit{PullRequestBody: body}}}

	got := m.Environ()["CI_COMMIT_PULL_REQUEST_BODY"]
	assert.LessOrEqual(t, len(got), maxPullRequestBodyBytes)
	assert.Greater(t, len(got), maxPullRequestBodyBytes-utf8.UTFMax)
	assert.True(t, utf8.ValidString(got))
	assert.True(t, strings.HasPrefix(body, got))
	assert.True(t, strings.HasPrefix(got, "Spec-impact: none\n"))
}
