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

package config

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

type blockingReadOnlyNetrcForge struct {
	forge.Forge
}

func (f *blockingReadOnlyNetrcForge) ReadOnlyNetrc(ctx context.Context, _ *model.Repo) (*model.Netrc, error) {
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > readOnlyNetrcMintTimeout {
		return nil, context.Canceled
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestHTTPFetchReadOnlyNetrcMintTimeout(t *testing.T) {
	previousTimeout := readOnlyNetrcMintTimeout
	readOnlyNetrcMintTimeout = 10 * time.Millisecond
	t.Cleanup(func() { readOnlyNetrcMintTimeout = previousTimeout })

	service := &httpService{includeNetrc: true}
	_, err := service.Fetch(t.Context(), &blockingReadOnlyNetrcForge{}, nil, &model.Repo{}, &model.Pipeline{}, nil, false)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
