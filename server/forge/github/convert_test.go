// Copyright 2022 Woodpecker Authors
// Copyright 2018 Drone.IO Inc.
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

package github

import (
	"testing"

	"github.com/google/go-github/v88/github"
	"github.com/stretchr/testify/assert"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

func Test_convertStatus(t *testing.T) {
	// Exhaustive over every model.StatusValue constant. The contract: only
	// terminal *bad* outcomes map to GitHub "failure"; success maps to
	// "success"; every not-yet-terminal or internal state maps to "pending".
	// convertStatus must NEVER return "error" for a defined status — the old
	// default turned the transient/internal StatusCreated into a spurious red
	// required check, which failed Graphite merge-queue batches.
	tests := []struct {
		name   string
		status model.StatusValue
		want   string
	}{
		{name: "success", status: model.StatusSuccess, want: statusSuccess},
		{name: "failure", status: model.StatusFailure, want: statusFailure},
		{name: "declined", status: model.StatusDeclined, want: statusFailure},
		{name: "killed", status: model.StatusKilled, want: statusFailure},
		{name: "error", status: model.StatusError, want: statusFailure},
		{name: "pending", status: model.StatusPending, want: statusPending},
		{name: "running", status: model.StatusRunning, want: statusPending},
		{name: "blocked", status: model.StatusBlocked, want: statusPending},
		{name: "skipped", status: model.StatusSkipped, want: statusPending},
		// A never-started supersede (StatusCanceled) is terminal → success, so it
		// can't wedge the required aggregate check / block the merge queue.
		{name: "canceled", status: model.StatusCanceled, want: statusSuccess},
		// Regression: StatusCreated ("internal use only") briefly surfaces
		// during a merge-queue re-run. It used to fall through to statusError.
		{name: "created", status: model.StatusCreated, want: statusPending},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := convertStatus(tc.status)
			assert.Equal(t, tc.want, got)
			// The fix removed the error-default: no defined status may
			// report a red GitHub "error" commit status.
			assert.NotEqual(t, statusError, got, "convertStatus must never return statusError for a defined status")
		})
	}
}

func Test_convertDesc(t *testing.T) {
	assert.Equal(t, descSuccess, convertDesc(model.StatusSuccess))
	assert.Equal(t, descPending, convertDesc(model.StatusPending))
	assert.Equal(t, descPending, convertDesc(model.StatusRunning))
	assert.Equal(t, descFailure, convertDesc(model.StatusFailure))
	assert.Equal(t, descBlocked, convertDesc(model.StatusBlocked))
	assert.Equal(t, descDeclined, convertDesc(model.StatusDeclined))
	assert.Equal(t, descError, convertDesc(model.StatusKilled))
	assert.Equal(t, descError, convertDesc(model.StatusError))
}

func Test_convertRepoList(t *testing.T) {
	from := []*github.Repository{
		{
			Private:  github.Ptr(false),
			FullName: github.Ptr("octocat/hello-world"),
			Name:     github.Ptr("hello-world"),
			Owner: &github.User{
				AvatarURL: github.Ptr("http://..."),
				Login:     github.Ptr("octocat"),
			},
			HTMLURL:  github.Ptr("https://github.com/octocat/hello-world"),
			CloneURL: github.Ptr("https://github.com/octocat/hello-world.git"),
			Permissions: &github.RepositoryPermissions{
				Admin: github.Ptr(true),
				Push:  github.Ptr(true),
				Pull:  github.Ptr(true),
			},
		},
	}

	to := convertRepoList(from)
	assert.Equal(t, "http://...", to[0].Avatar)
	assert.Equal(t, "octocat/hello-world", to[0].FullName)
	assert.Equal(t, "octocat", to[0].Owner)
	assert.Equal(t, "hello-world", to[0].Name)
}

func Test_convertRepo(t *testing.T) {
	from := github.Repository{
		FullName:      github.Ptr("octocat/hello-world"),
		Name:          github.Ptr("hello-world"),
		HTMLURL:       github.Ptr("https://github.com/octocat/hello-world"),
		CloneURL:      github.Ptr("https://github.com/octocat/hello-world.git"),
		DefaultBranch: github.Ptr("develop"),
		Private:       github.Ptr(true),
		Owner: &github.User{
			AvatarURL: github.Ptr("http://..."),
			Login:     github.Ptr("octocat"),
		},
		Permissions: &github.RepositoryPermissions{
			Admin: github.Ptr(true),
			Push:  github.Ptr(true),
			Pull:  github.Ptr(true),
		},
	}

	to := convertRepo(&from)
	assert.Equal(t, "http://...", to.Avatar)
	assert.Equal(t, "octocat/hello-world", to.FullName)
	assert.Equal(t, "octocat", to.Owner)
	assert.Equal(t, "hello-world", to.Name)
	assert.Equal(t, "develop", to.Branch)
	assert.True(t, to.IsSCMPrivate)
	assert.Equal(t, "https://github.com/octocat/hello-world.git", to.Clone)
	assert.Equal(t, "https://github.com/octocat/hello-world", to.ForgeURL)
}

func Test_convertPerm(t *testing.T) {
	from := &github.Repository{
		Permissions: &github.RepositoryPermissions{
			Admin: github.Ptr(true),
			Push:  github.Ptr(true),
			Pull:  github.Ptr(true),
		},
	}

	to := convertPerm(from.GetPermissions())
	assert.True(t, to.Push)
	assert.True(t, to.Pull)
	assert.True(t, to.Admin)
}

func Test_convertTeam(t *testing.T) {
	from := &github.Organization{
		Login:     github.Ptr("octocat"),
		AvatarURL: github.Ptr("http://..."),
	}
	to := convertTeam(from)
	assert.Equal(t, "octocat", to.Login)
	assert.Equal(t, "http://...", to.Avatar)
}

func Test_convertTeamList(t *testing.T) {
	from := []*github.Organization{
		{
			Login:     github.Ptr("octocat"),
			AvatarURL: github.Ptr("http://..."),
		},
	}
	to := convertTeamList(from)
	assert.Equal(t, "octocat", to[0].Login)
	assert.Equal(t, "http://...", to[0].Avatar)
}

func Test_convertRepoHook(t *testing.T) {
	t.Run("should convert a repository from webhook", func(t *testing.T) {
		from := &github.PushEventRepository{Owner: &github.User{}}
		from.Owner.Login = github.Ptr("octocat")
		from.Owner.Name = github.Ptr("octocat")
		from.Name = github.Ptr("hello-world")
		from.FullName = github.Ptr("octocat/hello-world")
		from.Private = github.Ptr(true)
		from.HTMLURL = github.Ptr("https://github.com/octocat/hello-world")
		from.CloneURL = github.Ptr("https://github.com/octocat/hello-world.git")
		from.DefaultBranch = github.Ptr("develop")

		repo := convertRepoHook(from)
		assert.Equal(t, *from.Owner.Login, repo.Owner)
		assert.Equal(t, *from.Name, repo.Name)
		assert.Equal(t, *from.FullName, repo.FullName)
		assert.Equal(t, *from.Private, repo.IsSCMPrivate)
		assert.Equal(t, *from.HTMLURL, repo.ForgeURL)
		assert.Equal(t, *from.CloneURL, repo.Clone)
		assert.Equal(t, *from.DefaultBranch, repo.Branch)
	})

	t.Run("should derive full name from owner and name when missing", func(t *testing.T) {
		from := &github.PushEventRepository{Owner: &github.User{}}
		from.Owner.Login = github.Ptr("octocat")
		from.Name = github.Ptr("hello-world")
		// FullName intentionally left empty to hit the fallback branch

		repo := convertRepoHook(from)
		assert.Equal(t, "octocat/hello-world", repo.FullName)
	})
}
