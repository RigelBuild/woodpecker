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

package github

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/go-github/v88/github"
)

// appToken is a cached installation access token with its expiry.
type appToken struct {
	token  string
	expiry time.Time
}

const (
	// Lifetime of the App JWT; GitHub allows up to 10 minutes.
	appJWTExpiry = 9 * time.Minute
	// Clock-skew tolerance; backdates the JWT issued-at claim.
	appJWTClockDrift = 30 * time.Second
)

// appConfigured reports whether GitHub App credentials are set, enabling the
// Checks API reporting path.
func (c *client) appConfigured() bool {
	return c.appID != 0 && c.appKey != nil
}

// appJWT returns a short-lived JSON Web Token authenticating as the GitHub App,
// signed with the configured RSA private key.
func (c *client) appJWT() (string, error) {
	now := time.Now()
	claims := jwt.RegisteredClaims{
		Issuer:    strconv.FormatInt(c.appID, 10),
		IssuedAt:  jwt.NewNumericDate(now.Add(-appJWTClockDrift)),
		ExpiresAt: jwt.NewNumericDate(now.Add(appJWTExpiry)),
	}
	return jwt.NewWithClaims(jwt.SigningMethodRS256, claims).SignedString(c.appKey)
}

// installationToken returns a cached or freshly minted installation access token
// for the App's installation on the given repository.
func (c *client) installationToken(ctx context.Context, owner, name string) (string, error) {
	cacheKey := owner + "/" + name

	c.appTokenMu.Lock()
	defer c.appTokenMu.Unlock()
	if c.appTokens == nil {
		c.appTokens = make(map[string]appToken)
	}
	if tok, ok := c.appTokens[cacheKey]; ok && time.Now().Before(tok.expiry.Add(-time.Minute)) {
		return tok.token, nil
	}

	jwtToken, err := c.appJWT()
	if err != nil {
		return "", err
	}
	appClient, err := c.newClientTokenKind(ctx, jwtToken, tokenKindApp)
	if err != nil {
		return "", err
	}
	installation, _, err := appClient.Apps.GetRepositoryInstallation(ctx, owner, name)
	if err != nil {
		return "", fmt.Errorf("could not find GitHub App installation for %s: %w", cacheKey, err)
	}
	token, _, err := appClient.Apps.CreateInstallationToken(ctx, installation.GetID(), nil)
	if err != nil {
		return "", fmt.Errorf("could not create installation token for %s: %w", cacheKey, err)
	}

	c.appTokens[cacheKey] = appToken{token: token.GetToken(), expiry: token.GetExpiresAt().Time}
	return token.GetToken(), nil
}

// installationClient returns a GitHub client authenticated as the App's
// installation on the given repository.
func (c *client) installationClient(ctx context.Context, owner, name string) (*github.Client, error) {
	token, err := c.installationToken(ctx, owner, name)
	if err != nil {
		return nil, err
	}
	return c.newClientTokenKind(ctx, token, tokenKindApp)
}
