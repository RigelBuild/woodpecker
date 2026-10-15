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
	"strings"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml/utils"
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
	if merged.Images, ok = narrowestImages(value.Images, other.Images); !ok {
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

// narrowestImages intersects two image filters with the matching the compiler
// applies at run time, where an untagged pattern allows every tag of its image.
// It keeps the narrower of each overlapping pair and drops patterns another kept
// pattern already covers. An empty list means any image.
func narrowestImages(a, b []string) ([]string, bool) {
	switch {
	case len(b) == 0:
		return a, true
	case len(a) == 0:
		return b, true
	}
	var both []string
	for _, p := range a {
		for _, q := range b {
			switch {
			case imageCovers(p, q):
				both = append(both, q)
			case imageCovers(q, p):
				both = append(both, p)
			}
		}
	}
	kept := make([]string, 0, len(both))
	for i, p := range both {
		redundant := slices.ContainsFunc(kept, func(k string) bool { return imageCovers(k, p) }) ||
			slices.ContainsFunc(both[i+1:], func(q string) bool { return imageCovers(q, p) && !imageCovers(p, q) })
		if !redundant {
			kept = append(kept, p)
		}
	}
	return kept, len(kept) > 0
}

// imageCovers reports whether pattern p allows every image pattern q allows. A
// tagged pattern cannot cover an untagged one, which allows every tag.
func imageCovers(p, q string) bool {
	return utils.MatchImageDynamic(q, p) && (imageHasTag(q) || !imageHasTag(p))
}

// imageHasTag mirrors the compiler's tag check, so policy and run time agree.
func imageHasTag(name string) bool {
	return strings.Contains(name, ":")
}
