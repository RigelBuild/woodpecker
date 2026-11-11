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
	"bytes"
	"errors"
	"unicode/utf8"
)

const MaxStepOutputBytes = 4000

// ParseStepOutputs parses a complete step output file.
func ParseStepOutputs(data []byte) (map[string]string, error) {
	if len(data) > MaxStepOutputBytes || !utf8.Valid(data) {
		return nil, errors.New("invalid step output size or encoding")
	}
	outputs := make(map[string]string)
	if len(data) == 0 {
		return outputs, nil
	}
	lines := bytes.Split(data, []byte{'\n'})
	if len(lines[len(lines)-1]) == 0 {
		lines = lines[:len(lines)-1]
	}
	for _, line := range lines {
		key, value, found := bytes.Cut(line, []byte{'='})
		if !found || len(key) == 0 || len(key) > 64 || len(value) > 1024 {
			return nil, errors.New("invalid step output line")
		}
		for i, c := range key {
			if (c < 'a' || c > 'z') && (c < '0' || c > '9') && (i == 0 || (c != '_' && c != '.' && c != '-')) {
				return nil, errors.New("invalid step output key")
			}
		}
		outputs[string(key)] = string(value)
	}
	return outputs, nil
}
