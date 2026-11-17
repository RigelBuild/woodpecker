// Copyright 2021 Woodpecker Authors
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

package model

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
)

var (
	ErrSecretNameInvalid  = errors.New("invalid secret name")
	ErrSecretImageInvalid = errors.New("invalid secret image")
	ErrSecretValueInvalid = errors.New("invalid secret value")
	ErrSecretEventInvalid = errors.New("invalid secret event")
)

// Secret represents a secret variable, such as a password or token.
type Secret struct {
	ID     int64          `json:"id"              xorm:"pk autoincr 'id'"`
	OrgID  int64          `json:"org_id"          xorm:"NOT NULL DEFAULT 0 UNIQUE(s) INDEX 'org_id'"`
	RepoID int64          `json:"repo_id"         xorm:"NOT NULL DEFAULT 0 UNIQUE(s) INDEX 'repo_id'"`
	Name   string         `json:"name"            xorm:"NOT NULL UNIQUE(s) INDEX 'name'"`
	Value  string         `json:"value,omitempty" xorm:"TEXT 'value'"`
	Images []string       `json:"images"          xorm:"json 'images'"`
	Events []WebhookEvent `json:"events"          xorm:"json 'events'"`
<<<<<<< conflict 1 of 2
+++++++ vsrqopkp 79d82d67 "Merge pull request #149 from RigelBuild/ci-pipelines/rig-8574-release-3-17-0-rigel-23"
	// A gated secret reaches a pipeline only after an approver's browser-session approval.
	RequireApproval bool `json:"require_approval" xorm:"require_approval"`
	// When set, non-pull_request runs get the secret only on these branches.
	Branches []string `json:"branches" xorm:"json 'branches'"`
	Note     string   `json:"note" xorm:"note"`
	// Version makes an update based on a stale read fail instead of reverting a
	// concurrent change, such as an approver adding a gate.
	Version int64 `json:"-" xorm:"version 'version'"`
%%%%%%% diff from: ouulyvmm f4d3d580 "Update pre-commit hook rbubley/mirrors-prettier to v3.9.10 (#7277)"
\\\\\\\        to: yyprtmsp 1a845577 "Update module gitlab.com/gitlab-org/api/client-go/v3 to v3.47.0 (#7541)"
-	Note   string         `json:"note" xorm:"note"`
+	Note   string         `json:"note"            xorm:"note"`
>>>>>>> conflict 1 of 2 ends
} //	@name	Secret

// TableName return database table name for xorm.
func (Secret) TableName() string {
	return "secrets"
}

// BeforeInsert will sort events before inserted into database.
func (s *Secret) BeforeInsert() {
	s.Events = sortEvents(s.Events)
}

// Global secret.
func (s Secret) IsGlobal() bool {
	return s.RepoID == 0 && s.OrgID == 0
}

// Organization secret.
func (s Secret) IsOrganization() bool {
	return s.RepoID == 0 && s.OrgID != 0
}

// Repository secret.
func (s Secret) IsRepository() bool {
	return s.RepoID != 0 && s.OrgID == 0
}

var validDockerImageString = regexp.MustCompile(
	`^(` +
		`[\w\d\-_\.]+` + // hostname
		`(:\d+)?` + // optional port
		`/)?` + // optional hostname + port
		`([\w\d\-_\.][\w\d\-_\.\/]*/)?` + // optional url prefix
		`([\w\d\-_\.]+)` + // image name
		`(:[\w\d\-_]+)?` + // optional image tag
		`(@sha256:[a-f\d]{64})?` + // optional sha256 digest
		`$`,
)

// Validate validates the required fields and formats.
func (s *Secret) Validate() error {
	for _, event := range s.Events {
		if err := event.Validate(); err != nil {
			return errors.Join(err, ErrSecretEventInvalid)
		}
	}
	if len(s.Events) == 0 {
		return fmt.Errorf("%w: no event specified", ErrSecretEventInvalid)
	}

	for _, image := range s.Images {
		if len(image) == 0 {
			return fmt.Errorf("%w: empty image in images", ErrSecretImageInvalid)
		}
		if !validDockerImageString.MatchString(image) {
			return fmt.Errorf("%w: image '%s' do not match regexp '%s'", ErrSecretImageInvalid, image, validDockerImageString.String())
		}
	}

	switch {
	case len(s.Name) == 0:
		return fmt.Errorf("%w: empty name", ErrSecretNameInvalid)
	case len(s.Value) == 0:
		return fmt.Errorf("%w: empty value", ErrSecretValueInvalid)
	default:
		return nil
	}
}

// Copy makes a copy of the secret without the value.
func (s *Secret) Copy() *Secret {
	return &Secret{
		ID:              s.ID,
		OrgID:           s.OrgID,
		RepoID:          s.RepoID,
		Name:            s.Name,
		Images:          s.Images,
		Events:          sortEvents(s.Events),
		RequireApproval: s.RequireApproval,
		Branches:        s.Branches,
		Note:            s.Note,
	}
}

func sortEvents(wel WebhookEventList) WebhookEventList {
	sort.Sort(wel)
	return wel
}

type SecretPatch struct {
<<<<<<< conflict 2 of 2
+++++++ vsrqopkp 79d82d67 "Merge pull request #149 from RigelBuild/ci-pipelines/rig-8574-release-3-17-0-rigel-23"
	Name            *string        `json:"name"            `
	Value           *string        `json:"value,omitempty" `
	Images          []string       `json:"images"          `
	Events          []WebhookEvent `json:"events"          `
	RequireApproval *bool          `json:"require_approval"`
	Branches        []string       `json:"branches"        `
	Note            *string        `json:"note" `
%%%%%%% diff from: ouulyvmm f4d3d580 "Update pre-commit hook rbubley/mirrors-prettier to v3.9.10 (#7277)"
\\\\\\\        to: yyprtmsp 1a845577 "Update module gitlab.com/gitlab-org/api/client-go/v3 to v3.47.0 (#7541)"
-	Name   *string        `json:"name"            `
-	Value  *string        `json:"value,omitempty" `
-	Images []string       `json:"images"          `
-	Events []WebhookEvent `json:"events"          `
-	Note   *string        `json:"note" `
+	Name   *string        `json:"name"`
+	Value  *string        `json:"value,omitempty"`
+	Images []string       `json:"images"`
+	Events []WebhookEvent `json:"events"`
+	Note   *string        `json:"note"`
>>>>>>> conflict 2 of 2 ends
} //	@name	SecretPatch
