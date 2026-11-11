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
	"crypto/rand"
	"crypto/rsa"
	"strconv"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppJWT(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	c := &client{appID: 4242, appKey: key}
	tokenStr, err := c.appJWT()
	require.NoError(t, err)

	parsed, err := jwt.Parse(tokenStr, func(_ *jwt.Token) (any, error) {
		return &key.PublicKey, nil
	}, jwt.WithValidMethods([]string{"RS256"}))
	require.NoError(t, err)
	assert.True(t, parsed.Valid)

	iss, err := parsed.Claims.GetIssuer()
	require.NoError(t, err)
	assert.Equal(t, strconv.FormatInt(4242, 10), iss)
}
