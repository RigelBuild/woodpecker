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
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/services/utils"
)

// refusedRetryHooks replaces the clock and wait seams; onWait runs at each retry gap.
func refusedRetryHooks(t *testing.T, onWait func(clock *time.Time) <-chan time.Time) {
	t.Helper()
	prevNow, prevWait, prevWindow := extensionRefusedNow, extensionRefusedWait, extensionRefusedWindow
	t.Cleanup(func() {
		extensionRefusedNow, extensionRefusedWait, extensionRefusedWindow = prevNow, prevWait, prevWindow
	})
	clock := time.Date(2026, time.November, 3, 0, 0, 0, 0, time.UTC)
	extensionRefusedNow = func() time.Time { return clock }
	extensionRefusedWait = func() <-chan time.Time { return onWait(&clock) }
}

func closedAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

func newTestService(t *testing.T, endpoint string) *httpService {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	client, err := utils.NewHTTPClient(key, "loopback", time.Second)
	require.NoError(t, err)
	return &httpService{endpoint: endpoint, client: client}
}

func fired() <-chan time.Time {
	ch := make(chan time.Time, 1)
	ch <- time.Time{}
	return ch
}

func TestFetchRetriesUntilExtensionAccepts(t *testing.T) {
	addr := closedAddr(t)
	var waits atomic.Int32
	refusedRetryHooks(t, func(*time.Time) <-chan time.Time {
		if waits.Add(1) == 1 {
			l, err := net.Listen("tcp", addr)
			require.NoError(t, err)
			srv := &httptest.Server{Listener: l, Config: &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if _, err := w.Write([]byte(`{"configs":[{"name":"a","data":"b"}]}`)); err != nil {
					t.Error(err)
				}
			})}}
			srv.Start()
			t.Cleanup(srv.Close)
		}
		return fired()
	})

	files, err := newTestService(t, "http://"+addr).Fetch(t.Context(), nil, nil, &model.Repo{}, &model.Pipeline{}, nil, false)
	require.NoError(t, err)
	require.Len(t, files, 1)
	assert.EqualValues(t, 1, waits.Load())
}

func TestFetchFailsLoudAfterRefusedWindow(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	var waits atomic.Int32
	refusedRetryHooks(t, func(clock *time.Time) <-chan time.Time {
		if waits.Add(1) > 1 {
			t.Error("retried past the refused window")
			cancel(errors.New("retried past the refused window"))
			return make(chan time.Time)
		}
		*clock = clock.Add(extensionRefusedWindow)
		return fired()
	})

	_, err := newTestService(t, "http://"+closedAddr(t)).Fetch(ctx, nil, nil, &model.Repo{}, &model.Pipeline{}, nil, false)
	require.ErrorIs(t, err, syscall.ECONNREFUSED)
	require.ErrorContains(t, err, "config extension refused connections for 1m0s")
	assert.EqualValues(t, 1, waits.Load())
}

func TestFetchDoesNotRetryServerErrors(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithCancelCause(t.Context())
	refusedRetryHooks(t, func(*time.Time) <-chan time.Time {
		t.Error("a 500 must not take the refused-connection retry")
		cancel(errors.New("a 500 took the refused retry"))
		return make(chan time.Time)
	})

	_, err := newTestService(t, srv.URL).Fetch(ctx, nil, nil, &model.Repo{}, &model.Pipeline{}, nil, false)
	require.Error(t, err)
	assert.EqualValues(t, 3, requests.Load(), "only Send's own attempts")
}

func TestFetchStopsRetryingWhenContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	refusedRetryHooks(t, func(*time.Time) <-chan time.Time {
		cancel(nil)
		return make(chan time.Time)
	})

	_, err := newTestService(t, "http://"+closedAddr(t)).Fetch(ctx, nil, nil, &model.Repo{}, &model.Pipeline{}, nil, false)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, syscall.ECONNREFUSED)
}
