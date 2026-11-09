// Copyright 2026 Woodpecker Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"reflect"
	"strings"
	"testing"
)

func TestParseStepOutputs(t *testing.T) {
	tests := []struct {
		name    string
		data    string
		want    map[string]string
		invalid bool
	}{
		{name: "empty", want: map[string]string{}},
		{name: "valid and duplicate", data: "deploy.state=started\ndeploy.state=completed\na=b=c\n", want: map[string]string{"deploy.state": "completed", "a": "b=c"}},
		{name: "malformed", data: "no-equals", invalid: true},
		{name: "bad key", data: "Bad=value", invalid: true},
		{name: "key starts with punctuation", data: ".a=value", invalid: true},
		{name: "long key", data: strings.Repeat("a", 65) + "=value", invalid: true},
		{name: "maximum key and value", data: strings.Repeat("a", 64) + "=" + strings.Repeat("x", 1024), want: map[string]string{strings.Repeat("a", 64): strings.Repeat("x", 1024)}},
		{name: "blank line", data: "a=b\n\nc=d", invalid: true},
		{name: "long value", data: "a=" + strings.Repeat("x", 1025), invalid: true},
		{name: "oversize", data: strings.Repeat("a", 4001), invalid: true},
		{name: "invalid utf8", data: "a=\xff", invalid: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseStepOutputs([]byte(tt.data))
			if (err != nil) != tt.invalid {
				t.Fatalf("error = %v, invalid = %v", err, tt.invalid)
			}
			if !tt.invalid && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}
