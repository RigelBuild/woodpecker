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
	"testing"

	"github.com/stretchr/testify/assert"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

func TestWithStrictestPolicyImages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		a, b   []string
		want   []string
		usable bool
	}{
		{name: "empty means any image, so the other list wins", a: nil, b: []string{"plugins/deploy"}, want: []string{"plugins/deploy"}, usable: true},
		{name: "any on the other side", a: []string{"plugins/deploy:v1"}, b: []string{}, want: []string{"plugins/deploy:v1"}, usable: true},
		{name: "untagged overlaps tagged, tagged kept", a: []string{"plugins/deploy"}, b: []string{"plugins/deploy:v1"}, want: []string{"plugins/deploy:v1"}, usable: true},
		{name: "tagged overlaps untagged, either order", a: []string{"plugins/deploy:v1"}, b: []string{"plugins/deploy"}, want: []string{"plugins/deploy:v1"}, usable: true},
		{name: "equivalent spellings dedupe", a: []string{"plugins/deploy", "docker.io/plugins/deploy"}, b: []string{"plugins/deploy"}, want: []string{"plugins/deploy"}, usable: true},
		{name: "untagged covers its tagged form, which is dropped", a: []string{"plugins/deploy"}, b: []string{"plugins/deploy", "plugins/deploy:v1"}, want: []string{"plugins/deploy"}, usable: true},
		{name: "partial overlap keeps only the shared images", a: []string{"plugins/deploy", "plugins/s3"}, b: []string{"plugins/deploy:v1", "alpine"}, want: []string{"plugins/deploy:v1"}, usable: true},
		{name: "different tags are disjoint", a: []string{"plugins/deploy:v1"}, b: []string{"plugins/deploy:v2"}, usable: false},
		{name: "different images are disjoint", a: []string{"plugins/deploy"}, b: []string{"plugins/s3"}, usable: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, usable := withStrictestPolicy(&model.Secret{Name: "s", Images: tt.a}, &model.Secret{Name: "s", Images: tt.b})
			assert.Equal(t, tt.usable, usable)
			if tt.usable {
				assert.ElementsMatch(t, tt.want, got.Images)
			}
		})
	}
}
