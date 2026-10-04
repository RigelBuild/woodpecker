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
	"errors"
	"hash/maphash"
	"sync"
	"time"

	"github.com/google/go-github/v90/github"
	"github.com/jellydator/ttlcache/v3"
)

const (
	deliveredStatusTTL      = time.Hour
	deliveredStatusCapacity = 10000
	deliveredStatusStripes  = 64
)

// errUnchangedStatus stops a write that would repeat the last delivered status.
var errUnchangedStatus = errors.New("status unchanged since the last delivered write")

// statusDedupe remembers the last commit status this server delivered per
// (repo, commit, context). The aggregate contexts are re-posted on every
// workflow transition, and most of those writes repeat the same state.
type statusDedupe struct {
	delivered *ttlcache.Cache[string, string]
	seed      maphash.Seed
	stripes   [deliveredStatusStripes]sync.Mutex
}

func newStatusDedupe() *statusDedupe {
	return &statusDedupe{
		delivered: ttlcache.New(
			ttlcache.WithTTL[string, string](deliveredStatusTTL),
			ttlcache.WithCapacity[string, string](deliveredStatusCapacity),
			ttlcache.WithDisableTouchOnHit[string, string](),
		),
		seed: maphash.MakeSeed(),
	}
}

// post runs one write attempt for key. The stripe lock covers choosing the
// status and posting it, so concurrent reports for one context land in the
// order they were decided and the cache matches what GitHub last accepted.
func (d *statusDedupe) post(key string, build func() (github.RepoStatus, error),
	send func(github.RepoStatus) (*github.Response, error),
) (*github.Response, error) {
	if d == nil {
		status, err := build()
		if err != nil {
			return nil, err
		}
		return send(status)
	}

	mu := &d.stripes[maphash.String(d.seed, key)%deliveredStatusStripes]
	mu.Lock()
	defer mu.Unlock()

	status, err := build()
	if err != nil {
		return nil, err
	}
	value := status.GetState() + "\x00" + status.GetDescription() + "\x00" + status.GetTargetURL()
	if item := d.delivered.Get(key); item != nil && item.Value() == value {
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

func deliveredStatusKey(owner, name, commit, context string) string {
	return owner + "/" + name + "\x00" + commit + "\x00" + context
}
