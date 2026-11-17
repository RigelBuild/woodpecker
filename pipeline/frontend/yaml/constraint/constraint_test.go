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

package constraint

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v4"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/metadata"
)

func TestConstraintStatusSuccessFailure(t *testing.T) {
	testdata := []struct {
		conf        string
		wantSuccess bool
		wantFail    bool
	}{
		{conf: "", wantSuccess: true, wantFail: false},
		{conf: "{status: [failure]}", wantSuccess: false, wantFail: true},
		{conf: "{status: [success]}", wantSuccess: true, wantFail: false},
		{conf: "{status: [failure, success]}", wantSuccess: true, wantFail: true},
		{conf: "{event: push, status: [failure, success]}", wantSuccess: false, wantFail: false},
		{conf: "{event: pull_request, status: [failure, success]}", wantSuccess: true, wantFail: true},
		{conf: "{event: push, status: failure}", wantSuccess: false, wantFail: false},
		{conf: "{event: pull_request, status: [failure]}", wantSuccess: false, wantFail: true},
		{conf: "{status: success}", wantSuccess: true, wantFail: false},
		{conf: "[{}]", wantSuccess: true, wantFail: false},
		{conf: "[{status: success}]", wantSuccess: true, wantFail: false},
		{conf: "[{},{status: failure}]", wantSuccess: true, wantFail: true},
		{conf: "[{event: push, status: success},{status: failure}]", wantSuccess: false, wantFail: true},
		{conf: "[{status: failure},{event: push, status: success}]", wantSuccess: false, wantFail: true},
	}
	for _, tt := range testdata {
		t.Run(tt.conf, func(t *testing.T) {
			c := parseConstraints(t, tt.conf)
			assert.Equalf(t,
				tt.wantSuccess,
				c.IncludesStatusSuccess(metadata.Metadata{Curr: metadata.Pipeline{Event: metadata.EventPull}}, true, make(map[string]string)),
				"include success is wrong for when: '%s'", tt.conf)
			assert.Equal(t,
				tt.wantFail,
				c.IncludesStatusFailure(metadata.Metadata{Curr: metadata.Pipeline{Event: metadata.EventPull}}, true, make(map[string]string)),
				"include fail is wrong for when: '%s'", tt.conf)
		})
	}
}

func TestConstraints(t *testing.T) {
	testdata := []struct {
		desc string
		conf string
		with metadata.Metadata
		env  map[string]string
		want bool
	}{
		{
			desc: "no constraints, must match on default events",
			conf: "",
			with: metadata.Metadata{
				Curr: metadata.Pipeline{
					Event: metadata.EventPush,
				},
			},
			want: true,
		},
		{
			desc: "global branch filter",
			conf: "{ branch: develop }",
			with: metadata.Metadata{Curr: metadata.Pipeline{Event: metadata.EventPush, Commit: metadata.Commit{Branch: "main"}}},
			want: false,
		},
		{
			desc: "global branch filter",
			conf: "{ branch: main }",
			with: metadata.Metadata{Curr: metadata.Pipeline{Event: metadata.EventPush, Commit: metadata.Commit{Branch: "main"}}},
			want: true,
		},
		{
			desc: "repo constraint",
			conf: "{ repo: owner/* }",
			with: metadata.Metadata{Curr: metadata.Pipeline{Event: metadata.EventPush}, Repo: metadata.Repo{Owner: "owner", Name: "repo"}},
			want: true,
		},
		{
			desc: "repo constraint",
			conf: "{ repo: octocat/* }",
			with: metadata.Metadata{Curr: metadata.Pipeline{Event: metadata.EventPush}, Repo: metadata.Repo{Owner: "owner", Name: "repo"}},
			want: false,
		},
		{
			desc: "ref constraint",
			conf: "{ ref: refs/tags/* }",
			with: metadata.Metadata{Curr: metadata.Pipeline{Commit: metadata.Commit{Ref: "refs/tags/v1.0.0"}, Event: metadata.EventPush}},
			want: true,
		},
		{
			desc: "ref constraint",
			conf: "{ ref: refs/tags/* }",
			with: metadata.Metadata{Curr: metadata.Pipeline{Commit: metadata.Commit{Ref: "refs/heads/main"}, Event: metadata.EventPush}},
			want: false,
		},
		{
			desc: "platform constraint",
			conf: "{ platform: linux/amd64 }",
			with: metadata.Metadata{Curr: metadata.Pipeline{Event: metadata.EventPush}, Sys: metadata.System{Platform: "linux/amd64"}},
			want: true,
		},
		{
			desc: "platform constraint",
			conf: "{ repo: linux/amd64 }",
			with: metadata.Metadata{Curr: metadata.Pipeline{Event: metadata.EventPush}, Sys: metadata.System{Platform: "windows/amd64"}},
			want: false,
		},
		{
			desc: "instance constraint",
			conf: "{ instance: agent.tld }",
			with: metadata.Metadata{Curr: metadata.Pipeline{Event: metadata.EventPush}, Sys: metadata.System{Host: "agent.tld"}},
			want: true,
		},
		{
			desc: "instance constraint",
			conf: "{ instance: agent.tld }",
			with: metadata.Metadata{Curr: metadata.Pipeline{Event: metadata.EventPush}, Sys: metadata.System{Host: "beta.agent.tld"}},
			want: false,
		},
		{
			desc: "filter cron by matching name",
			conf: "{ event: cron, cron: job1 }",
			with: metadata.Metadata{Curr: metadata.Pipeline{Event: metadata.EventCron, Cron: "job1"}},
			want: true,
		},
		{
			desc: "filter cron by name",
			conf: "{ event: cron, cron: job2 }",
			with: metadata.Metadata{Curr: metadata.Pipeline{Event: metadata.EventCron, Cron: "job1"}},
			want: false,
		},
		{
			desc: "filter with build-in env passes",
			conf: "{ branch: ${CI_REPO_DEFAULT_BRANCH} }",
			with: metadata.Metadata{
				Curr: metadata.Pipeline{Event: metadata.EventPush, Commit: metadata.Commit{Branch: "stable"}},
				Repo: metadata.Repo{Branch: "stable"},
			},
			want: true,
		},
		{
			desc: "filter by eval based on event",
			conf: `{ evaluate: 'CI_PIPELINE_EVENT == "push"' }`,
			with: metadata.Metadata{Curr: metadata.Pipeline{Event: metadata.EventPush}},
			want: true,
		},
		{
			desc: "filter by eval based on event and repo",
			conf: `{ evaluate: 'CI_PIPELINE_EVENT == "push" && CI_REPO == "owner/repo"' }`,
			with: metadata.Metadata{Curr: metadata.Pipeline{Event: metadata.EventPush}, Repo: metadata.Repo{Owner: "owner", Name: "repo"}},
			want: true,
		},
		{
			desc: "filter by eval based on custom variable",
			conf: `{ evaluate: 'TESTVAR == "testval"' }`,
			with: metadata.Metadata{Curr: metadata.Pipeline{Event: metadata.EventManual}},
			env:  map[string]string{"TESTVAR": "testval"},
			want: true,
		},
		{
			desc: "filter by eval based on custom variable",
			conf: `{ evaluate: 'TESTVAR == "testval"' }`,
			with: metadata.Metadata{Curr: metadata.Pipeline{Event: metadata.EventManual}},
			env:  map[string]string{"TESTVAR": "qwe"},
			want: false,
		},
		{
			desc: "filter by eval with escaped regex matches release tag",
			conf: `{ evaluate: 'CI_COMMIT_TAG matches "^v?[0-9]+\\\\.[0-9]+\\\\.[0-9]+$"' }`,
			with: metadata.Metadata{Curr: metadata.Pipeline{Event: metadata.EventTag, Commit: metadata.Commit{Ref: "refs/tags/v3.20.0"}}},
			want: true,
		},
		{
			desc: "filter by eval with escaped regex skips pre-release tag",
			conf: `{ evaluate: 'CI_COMMIT_TAG matches "^v?[0-9]+\\\\.[0-9]+\\\\.[0-9]+$"' }`,
			with: metadata.Metadata{Curr: metadata.Pipeline{Event: metadata.EventTag, Commit: metadata.Commit{Ref: "refs/tags/v3.20.0-rc.1"}}},
			want: false,
		},
		{
			desc: "filter by branch with escaped slash matches one slash",
			conf: `{ branch: '*\\/*' }`,
			with: metadata.Metadata{Curr: metadata.Pipeline{Event: metadata.EventPush, Commit: metadata.Commit{Branch: "feat/x"}}},
			want: true,
		},
		{
			desc: "filter by branch with escaped slash skips two slashes",
			conf: `{ branch: '*\\/*' }`,
			with: metadata.Metadata{Curr: metadata.Pipeline{Event: metadata.EventPush, Commit: metadata.Commit{Branch: "feat/x/y"}}},
			want: false,
		},
		{
			desc: "filter by branch with escaped slash and globstar matches two slashes",
			conf: `{ branch: '*\\/**' }`,
			with: metadata.Metadata{Curr: metadata.Pipeline{Event: metadata.EventPush, Commit: metadata.Commit{Branch: "feat/x/y"}}},
			want: true,
		},
	}

	for _, tt := range testdata {
		t.Run(tt.desc, func(t *testing.T) {
			conf, err := metadata.EnvVarSubst(tt.conf, tt.with.Environ())
			assert.NoError(t, err)
			c := parseConstraints(t, conf)
			got, err := c.Match(tt.with, false, tt.env)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

<<<<<<< conflict 1 of 1
+++++++ vsrqopkp 79d82d67 "Merge pull request #149 from RigelBuild/ci-pipelines/rig-8574-release-3-17-0-rigel-23"
func TestIncludesEvent(t *testing.T) {
	const metaEvent = "pull_request_metadata"
	testdata := []struct {
		desc string
		conf string
		want bool
	}{
		{desc: "empty When matches every event", conf: "", want: true},
		{desc: "event lists the queried event", conf: "{event: [pull_request_metadata]}", want: true},
		{desc: "event lists only a different event", conf: "{event: [pull_request]}", want: false},
		{desc: "constraint with no event filter matches every event", conf: "{branch: main}", want: true},
		{
			// IncludesEvent is structural: it judges the `event` clause ALONE, a
			// superset of would-Match. A path filter would make Match false for a
			// metadata event (no changed files), but the event IS listed, so
			// IncludesEvent is deliberately true.
			desc: "mixed sub-constraint with path still includes the event (superset direction)",
			conf: "{event: [pull_request, pull_request_metadata], path: ['docs/**']}",
			want: true,
		},
	}
	for _, test := range testdata {
		t.Run(test.desc, func(t *testing.T) {
			c := parseConstraints(t, test.conf)
			assert.Equal(t, test.want, c.IncludesEvent(metaEvent),
				"IncludesEvent is wrong for when: '%s'", test.conf)
		})
	}
}

%%%%%%% diff from: ouulyvmm f4d3d580 "Update pre-commit hook rbubley/mirrors-prettier to v3.9.10 (#7277)"
\\\\\\\        to: yyprtmsp 1a845577 "Update module gitlab.com/gitlab-org/api/client-go/v3 to v3.47.0 (#7541)"
+func TestConstraintsMatchIgnoringEvaluate(t *testing.T) {
+	testdata := []struct {
+		desc string
+		conf string
+		with metadata.Metadata
+		want bool
+	}{
+		{
+			desc: "eval on unknown variable is treated as matching",
+			conf: `{ event: manual, evaluate: 'TESTVAR == "testval"' }`,
+			with: metadata.Metadata{Curr: metadata.Pipeline{Event: metadata.EventManual}},
+			want: true,
+		},
+		{
+			desc: "other conditions still apply",
+			conf: `{ event: push, evaluate: 'TESTVAR == "testval"' }`,
+			with: metadata.Metadata{Curr: metadata.Pipeline{Event: metadata.EventManual}},
+			want: false,
+		},
+		{
+			desc: "branch still applies",
+			conf: `{ branch: main, evaluate: 'true' }`,
+			with: metadata.Metadata{Curr: metadata.Pipeline{Event: metadata.EventManual, Commit: metadata.Commit{Branch: "dev"}}},
+			want: false,
+		},
+	}
+
+	for _, tt := range testdata {
+		t.Run(tt.desc, func(t *testing.T) {
+			c := parseConstraints(t, tt.conf)
+			got, err := c.MatchIgnoringEvaluate(tt.with, true, nil)
+			assert.NoError(t, err)
+			assert.Equal(t, tt.want, got)
+		})
+	}
+}
+
>>>>>>> conflict 1 of 1 ends
func parseConstraints(t *testing.T, s string) *When {
	t.Helper()
	c := &When{}
	require.NoError(t, yaml.Unmarshal([]byte(s), c))
	return c
}
