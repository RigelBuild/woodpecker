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
	"slices"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// withStrictestPolicy returns a copy of value whose gate and filters are no looser
// than other's, so a same-name secret from another source can never widen access.
// It reports false when the filters share nothing, as the name then fits no run.
func withStrictestPolicy(value, other *model.Secret) (*model.Secret, bool) {
	merged := *value
	merged.RequireApproval = value.RequireApproval || other.RequireApproval

	var ok bool
	if merged.Events, ok = narrowest(value.Events, other.Events); !ok {
		return nil, false
	}
	if merged.Branches, ok = narrowest(value.Branches, other.Branches); !ok {
		return nil, false
	}
	if merged.Images, ok = narrowest(value.Images, other.Images); !ok {
		return nil, false
	}
	return &merged, true
}

// narrowest intersects two filters where an empty list means "everything". An
// empty intersection of two non-empty filters matches nothing, so it reports false.
func narrowest[T comparable](a, b []T) ([]T, bool) {
	switch {
	case len(b) == 0:
		return a, true
	case len(a) == 0:
		return b, true
	}
	both := make([]T, 0, min(len(a), len(b)))
	for _, item := range a {
		if slices.Contains(b, item) {
			both = append(both, item)
		}
	}
	return both, len(both) > 0
}
