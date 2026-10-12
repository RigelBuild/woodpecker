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

package config_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
	forge_mocks "go.woodpecker-ci.org/woodpecker/v3/server/forge/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/services/config"
	"go.woodpecker-ci.org/woodpecker/v3/server/services/utils"
)

type readOnlyNetrcForge struct {
	forge.Forge
	netrc *model.Netrc
	err   error
}

func (f *readOnlyNetrcForge) ReadOnlyNetrc(_ context.Context, _ *model.Repo) (*model.Netrc, error) {
	return f.netrc, f.err
}

func TestFetchReadOnlyNetrc(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	for _, tt := range []struct {
		name          string
		mintErr       error
		supportsNetrc bool
		nilNetrc      bool
		wantNetrc     *model.Netrc
	}{
		{
			name:          "sends the minted netrc",
			supportsNetrc: true,
			wantNetrc: &model.Netrc{
				Machine:   "github.com",
				Login:     "x-access-token",
				Password:  "read-only-token",
				ExpiresAt: time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC),
				Type:      model.ForgeTypeGithub,
			},
		},
		{
			name:          "does not fetch config when minting fails",
			supportsNetrc: true,
			mintErr:       errors.New("token mint failed"),
		},
		{
			name:          "does not fetch config when netrc is nil",
			supportsNetrc: true,
			nilNetrc:      true,
		},
		{
			name: "does not fetch config without netrc capability",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var requests atomic.Int32
			var gotNetrc *model.Netrc
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				var body struct {
					Netrc *model.Netrc `json:"netrc"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				gotNetrc = body.Netrc
				_, _ = w.Write([]byte(`{"configs":[]}`))
			}))
			t.Cleanup(server.Close)

			client, err := utils.NewHTTPClient(privateKey, "loopback", time.Second)
			require.NoError(t, err)
			var forgeClient forge.Forge
			if tt.supportsNetrc {
				forgeClient = &readOnlyNetrcForge{netrc: tt.wantNetrc, err: tt.mintErr}
			} else {
				forgeClient = forge_mocks.NewMockForge(t)
			}
			service := config.NewHTTP(server.URL, client, true)
			files, err := service.Fetch(t.Context(), forgeClient, nil, &model.Repo{}, &model.Pipeline{}, nil, false)
			if tt.mintErr != nil || !tt.supportsNetrc || tt.nilNetrc {
				if tt.mintErr != nil {
					require.ErrorContains(t, err, tt.mintErr.Error())
				} else {
					require.Error(t, err)
				}
				assert.Empty(t, files)
				assert.Zero(t, requests.Load())
				return
			}
			require.NoError(t, err)
			assert.Empty(t, files)
			assert.EqualValues(t, 1, requests.Load())
			assert.Equal(t, tt.wantNetrc, gotNetrc)
		})
	}
}
