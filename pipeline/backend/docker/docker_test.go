// Copyright 2022 Woodpecker Authors
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

package docker

import (
	"context"
	"io"
	"iter"
	"strings"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/jsonstream"
	"github.com/moby/moby/api/types/system"
	"github.com/moby/moby/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	backend_types "go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types"
)

// fakePullResponse is a minimal client.ImagePullResponse whose Read yields a
// caller-supplied JSON message stream. StartStep consumes the response through
// the io.Reader path (jsonmessage.DisplayJSONMessagesStream) and Close, so
// JSONMessages/Wait are benign stubs that the path under test never calls.
type fakePullResponse struct {
	r      io.Reader
	closed bool
}

func (f *fakePullResponse) Read(p []byte) (int, error) { return f.r.Read(p) }

func (f *fakePullResponse) Close() error {
	f.closed = true
	return nil
}

func (f *fakePullResponse) JSONMessages(_ context.Context) iter.Seq2[jsonstream.Message, error] {
	return func(func(jsonstream.Message, error) bool) {}
}

func (f *fakePullResponse) Wait(_ context.Context) error { return nil }

// fakeClient embeds a nil client.APIClient (unimplemented methods panic if the
// code path reaches them) and overrides only the methods StartStep exercises:
// ImagePull, ContainerCreate, and ContainerStart.
type fakeClient struct {
	client.APIClient
	pullBody   func() client.ImagePullResponse
	createErrs []error // returned in sequence, one per ContainerCreate call
	createN    int
}

func (f *fakeClient) ImagePull(_ context.Context, _ string, _ client.ImagePullOptions) (client.ImagePullResponse, error) {
	// The failure is delivered INSIDE the stream body, not as this error: that
	// is the real-world shape being defended against.
	return f.pullBody(), nil
}

func (f *fakeClient) ContainerCreate(_ context.Context, _ client.ContainerCreateOptions) (client.ContainerCreateResult, error) {
	var err error
	if f.createN < len(f.createErrs) {
		err = f.createErrs[f.createN]
	}
	f.createN++
	return client.ContainerCreateResult{}, err
}

func (f *fakeClient) ContainerStart(_ context.Context, _ string, _ client.ContainerStartOptions) (client.ContainerStartResult, error) {
	return client.ContainerStartResult{}, nil
}

func newTestStep() *backend_types.Step {
	return &backend_types.Step{
		Name:  "test",
		UUID:  "test-uuid",
		Image: "ghcr.io/x/y:pr-1",
		Pull:  true,
	}
}

// TestStartStep_SurfacesPullStreamError defends the regression: when the initial
// ContainerCreate fails NotFound and the authoritative retry pull fails with an
// error carried inside the JSON message stream, StartStep must return that real
// cause (rate limit) rather than letting the retried ContainerCreate mask it as
// a downstream "No such image" NotFound.
func TestStartStep_SurfacesPullStreamError(t *testing.T) {
	const streamErr = "toomanyrequests: rate limit"
	// The error rides inside the pull stream; ImagePull itself returns nil.
	pullJSON := `{"errorDetail":{"message":"` + streamErr + `"},"error":"` + streamErr + `"}` + "\n"

	fake := &fakeClient{
		pullBody: func() client.ImagePullResponse {
			return &fakePullResponse{r: strings.NewReader(pullJSON)}
		},
		createErrs: []error{
			// First create: image not present -> forces the retry path.
			errdefs.ErrNotFound,
			// Second create (only reached if the fix is absent and the stream
			// error is swallowed): the misleading mask the fix must not surface.
			errdefs.ErrNotFound.WithMessage("No such image: ghcr.io/x/y:pr-1"),
		},
	}

	e := &docker{client: fake, info: system.Info{OSType: "linux"}, config: config{}}

	err := e.StartStep(context.Background(), newTestStep(), "task-uuid")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "rate limit", "StartStep must surface the real pull-stream error")
	assert.NotContains(t, err.Error(), "No such image", "StartStep must not mask the cause as the downstream NotFound")
}

// TestStartStep_RetryPathSucceedsWhenPullStreamClean is the success guard: it
// drives the same retry path (first ContainerCreate NotFound), but the retry
// pull stream is clean, so StartStep must complete nil. This proves the fix's
// return-on-stream-error only fires on a genuine stream error and did not turn
// StartStep into an always-error path.
func TestStartStep_RetryPathSucceedsWhenPullStreamClean(t *testing.T) {
	pullJSON := `{"status":"Pulling from x/y"}` + "\n" +
		`{"status":"Status: Downloaded newer image for ghcr.io/x/y:pr-1"}` + "\n"

	fake := &fakeClient{
		pullBody: func() client.ImagePullResponse {
			return &fakePullResponse{r: strings.NewReader(pullJSON)}
		},
		createErrs: []error{
			errdefs.ErrNotFound, // force the retry path
			nil,                 // re-create succeeds after a clean re-pull
		},
	}

	e := &docker{client: fake, info: system.Info{OSType: "linux"}, config: config{}}

	err := e.StartStep(context.Background(), newTestStep(), "task-uuid")

	assert.NoError(t, err)
}
