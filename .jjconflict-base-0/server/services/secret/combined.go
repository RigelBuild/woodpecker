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

package secret

import (
	"context"

	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

type combined struct {
	base      Service
	extension *httpExtension
}

// NewCombined returns a secret service that combines a base service with an HTTP extension.
// The extension is called during SecretListPipeline to fetch additional secrets and
// the extension secrets taking priority.
func NewCombined(base Service, extension *httpExtension) Service {
	return &combined{base, extension}
}

func (c *combined) SecretListPipeline(ctx context.Context, repo *model.Repo, pipeline *model.Pipeline, netrc *model.Netrc) ([]*model.Secret, error) {
	// Get secrets from base service
	baseSecrets, err := c.base.SecretListPipeline(ctx, repo, pipeline, netrc)
	if err != nil {
		return nil, err
	}

	// Get secrets from HTTP extension
	extensionSecrets, err := c.extension.SecretListPipeline(ctx, repo, pipeline, netrc)
	if err != nil {
		// Stored values may be stale overrides the extension meant to replace, so
		// they wait for an approver instead of running without review.
		log.Warn().Err(err).Msg("failed to fetch secrets from extension, gating every stored secret")
		gated := make([]*model.Secret, 0, len(baseSecrets))
		for _, s := range baseSecrets {
			g := *s
			g.RequireApproval = true
			gated = append(gated, &g)
		}
		return gated, nil
	}

	if len(extensionSecrets) == 0 {
		return baseSecrets, nil
	}

	// Merge secrets, with extension secrets taking priority (no duplicates by name).
	// An extension can replace a stored row's value but never loosen its policy.
	// Names match case-insensitively, as the compiler lowercases them.
	stored := make(map[string]*model.Secret, len(baseSecrets))
	for _, s := range baseSecrets {
		stored[secretKey(s.Name)] = s
	}

	exists := make(map[string]struct{}, len(extensionSecrets))
	merged := make([]*model.Secret, 0, len(baseSecrets)+len(extensionSecrets))
	for _, s := range extensionSecrets {
		key := secretKey(s.Name)
		if _, ok := exists[key]; ok {
			continue
		}
		exists[key] = struct{}{}
		if base, ok := stored[key]; ok {
			narrowed, usable := withStrictestPolicy(s, base)
			if !usable {
				continue
			}
			s = narrowed
		}
		merged = append(merged, s)
	}

	for _, s := range baseSecrets {
		key := secretKey(s.Name)
		if _, ok := exists[key]; ok {
			continue
		}
		exists[key] = struct{}{}
		merged = append(merged, s)
	}

	return merged, nil
}

// All other methods delegate to the base service.

func (c *combined) SecretFind(repo *model.Repo, name string) (*model.Secret, error) {
	return c.base.SecretFind(repo, name)
}

func (c *combined) SecretList(repo *model.Repo, p *model.ListOptionsWithAll) ([]*model.Secret, error) {
	return c.base.SecretList(repo, p)
}

func (c *combined) SecretCreate(repo *model.Repo, secret *model.Secret) error {
	return c.base.SecretCreate(repo, secret)
}

func (c *combined) SecretUpdate(repo *model.Repo, secret *model.Secret) error {
	return c.base.SecretUpdate(repo, secret)
}

func (c *combined) SecretDelete(repo *model.Repo, secret *model.Secret) error {
	return c.base.SecretDelete(repo, secret)
}

func (c *combined) OrgSecretFind(orgID int64, name string) (*model.Secret, error) {
	return c.base.OrgSecretFind(orgID, name)
}

func (c *combined) OrgSecretList(orgID int64, p *model.ListOptionsWithAll) ([]*model.Secret, error) {
	return c.base.OrgSecretList(orgID, p)
}

func (c *combined) OrgSecretCreate(orgID int64, secret *model.Secret) error {
	return c.base.OrgSecretCreate(orgID, secret)
}

func (c *combined) OrgSecretUpdate(orgID int64, secret *model.Secret) error {
	return c.base.OrgSecretUpdate(orgID, secret)
}

func (c *combined) OrgSecretDelete(orgID int64, secret *model.Secret) error {
	return c.base.OrgSecretDelete(orgID, secret)
}

func (c *combined) GlobalSecretFind(name string) (*model.Secret, error) {
	return c.base.GlobalSecretFind(name)
}

func (c *combined) GlobalSecretList(p *model.ListOptionsWithAll) ([]*model.Secret, error) {
	return c.base.GlobalSecretList(p)
}

func (c *combined) GlobalSecretCreate(secret *model.Secret) error {
	return c.base.GlobalSecretCreate(secret)
}

func (c *combined) GlobalSecretUpdate(secret *model.Secret) error {
	return c.base.GlobalSecretUpdate(secret)
}

func (c *combined) GlobalSecretDelete(secret *model.Secret) error {
	return c.base.GlobalSecretDelete(secret)
}
