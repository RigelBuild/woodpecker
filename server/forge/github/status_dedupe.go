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
	"errors"
	"sync"
	"time"

	"github.com/google/go-github/v92/github"
	"github.com/jellydator/ttlcache/v3"
)

const (
	deliveredStatusTTL      = time.Hour
	deliveredStatusCapacity = 10000
)

// errUnchangedStatus stops a write that would repeat the last delivered status.
var errUnchangedStatus = errors.New("status unchanged since the last delivered write")

// statusDedupes holds one dedupe per GitHub API URL. The server rebuilds its
// forge client every few minutes, and the dedupe must outlive each rebuild.
var (
	statusDedupesMu sync.Mutex
	statusDedupes   = map[string]*statusDedupe{}
)

// statusDedupe remembers the last pending commit status this server delivered
// per (repo, commit, context). The aggregate contexts are re-posted on every
// workflow transition, and most of those writes repeat the same pending state.
// It assumes this server is the only writer of those contexts. Terminal states
// are always posted, so a check that another writer changed still heals.
type statusDedupe struct {
	delivered *ttlcache.Cache[string, string]
	mu        sync.Mutex
	locks     map[string]*keyLock
}

// keyLock serializes reports for one key; refs counts holders and waiters.
type keyLock struct {
	sem  chan struct{}
	refs int
}

func newStatusDedupe() *statusDedupe {
	return &statusDedupe{
		delivered: ttlcache.New(
			ttlcache.WithTTL[string, string](deliveredStatusTTL),
			ttlcache.WithCapacity[string, string](deliveredStatusCapacity),
			ttlcache.WithDisableTouchOnHit[string, string](),
		),
		locks: map[string]*keyLock{},
	}
}

func statusDedupeFor(api string) *statusDedupe {
	statusDedupesMu.Lock()
	defer statusDedupesMu.Unlock()
	d := statusDedupes[api]
	if d == nil {
		d = newStatusDedupe()
		statusDedupes[api] = d
	}
	return d
}

// post runs one write attempt for key. The key lock covers choosing the status
// and posting it, so concurrent reports for one context land in the order they
// were decided and the cache matches what GitHub last accepted.
func (d *statusDedupe) post(ctx context.Context, key string, build func() (github.RepoStatus, error),
	send func(github.RepoStatus) (*github.Response, error),
) (*github.Response, error) {
	if d == nil {
		status, err := build()
		if err != nil {
			return nil, err
		}
		return send(status)
	}

	unlock, err := d.lock(ctx, key)
	if err != nil {
		return nil, err
	}
	defer unlock()

	status, err := build()
	if err != nil {
		return nil, err
	}
	value := status.GetState() + "\x00" + status.GetDescription() + "\x00" + status.GetTargetURL()
	if item := d.delivered.Get(key); status.GetState() == statusPending && item != nil && item.Value() == value {
		return nil, errUnchangedStatus
	}
	resp, err := send(status)
	if err != nil {
		// GitHub may or may not hold the write; never skip the next attempt.
		d.delivered.Delete(key)
		return resp, err
	}
	d.delivered.Set(key, value, ttlcache.DefaultTTL)
	return resp, nil
}

// lock waits for key's lock until ctx ends, so a queued report keeps its budget.
func (d *statusDedupe) lock(ctx context.Context, key string) (func(), error) {
	d.mu.Lock()
	l := d.locks[key]
	if l == nil {
		l = &keyLock{sem: make(chan struct{}, 1)}
		d.locks[key] = l
	}
	l.refs++
	d.mu.Unlock()

	release := func() {
		d.mu.Lock()
		if l.refs--; l.refs == 0 {
			delete(d.locks, key)
		}
		d.mu.Unlock()
	}
	select {
	case l.sem <- struct{}{}:
		return func() { <-l.sem; release() }, nil
	case <-ctx.Done():
		release()
		return nil, ctx.Err()
	}
}

func deliveredStatusKey(owner, name, commit, context string) string {
	return owner + "/" + name + "\x00" + commit + "\x00" + context
}
