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

package shared

import (
	"bytes"
	"cmp"
	"io"
	"slices"
	"strings"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline"
)

// Strings not longer than minSecretLength are not considered secrets.
// Do not sanitize them.
const minSecretLength = 3

var secretsMask = []byte("********")

type secretsReplaceWriter struct {
	dst              io.Writer
	secrets          [][]byte
	prefixTables     [][]int
	pending          []byte
	maskUntilNewline bool
}

type secretMatch struct {
	start int
	end   int
}

// NewSecretsReplaceWriter wraps dst so that the given secret values are
// replaced with asterisks before being written. Secrets are split on newlines
// and matched within each line. Possible secret prefixes are held until the
// next write or Close.
func NewSecretsReplaceWriter(dst io.Writer, secrets []string) io.WriteCloser {
	w := &secretsReplaceWriter{dst: dst}
	for _, secret := range secrets {
		for part := range strings.SplitSeq(strings.TrimSpace(secret), "\n") {
			if len(part) <= minSecretLength {
				continue
			}
			secretBytes := []byte(part)
			w.secrets = append(w.secrets, secretBytes)
			w.prefixTables = append(w.prefixTables, prefixTable(secretBytes))
		}
	}
	return w
}

func prefixTable(pattern []byte) []int {
	table := make([]int, len(pattern))
	for i, matched := 1, 0; i < len(pattern); i++ {
		for matched > 0 && pattern[i] != pattern[matched] {
			matched = table[matched-1]
		}
		if pattern[i] == pattern[matched] {
			matched++
			table[i] = matched
		}
	}
	return table
}

func (w *secretsReplaceWriter) Write(p []byte) (int, error) {
	if len(w.secrets) == 0 {
		if _, err := w.dst.Write(p); err != nil {
			return 0, err
		}
		return len(p), nil
	}
	if len(p) == 0 {
		return 0, nil
	}
	if w.maskUntilNewline {
		newline := bytes.IndexByte(p, '\n')
		if newline < 0 {
			return len(p), nil
		}
		if err := w.writeOutput(append(bytes.Clone(secretsMask), '\n')); err != nil {
			return 0, err
		}
		w.maskUntilNewline = false
		if newline+1 < len(p) {
			if _, err := w.Write(p[newline+1:]); err != nil {
				return 0, err
			}
		}
		return len(p), nil
	}

	data := p
	if len(w.pending) > 0 {
		data = make([]byte, len(w.pending)+len(p))
		copy(data, w.pending)
		copy(data[len(w.pending):], p)
	}
	w.pending = nil

	// Secret parts cannot cross newlines, so flush complete lines immediately.
	for {
		newline := bytes.IndexByte(data, '\n')
		if newline < 0 {
			break
		}
		lineEnd := newline + 1
		if err := w.writeMasked(data[:lineEnd]); err != nil {
			return 0, err
		}
		data = data[lineEnd:]
	}

	// KMP finds the longest suffix that is a proper prefix of any secret.
	holdStart := len(data)
	for i, secret := range w.secrets {
		matched := 0
		for _, b := range data {
			for matched > 0 && b != secret[matched] {
				matched = w.prefixTables[i][matched-1]
			}
			if b == secret[matched] {
				matched++
			}
			if matched == len(secret) {
				matched = w.prefixTables[i][matched-1]
			}
		}
		if matched > 0 {
			holdStart = min(holdStart, len(data)-matched)
		}
	}

	// Keep complete matches intact when a possible prefix overlaps them.
	for {
		previousHoldStart := holdStart
		for _, secret := range w.secrets {
			for offset := 0; offset <= len(data)-len(secret); {
				match := bytes.Index(data[offset:], secret)
				if match < 0 {
					break
				}
				matchStart := offset + match
				matchEnd := matchStart + len(secret)
				if matchStart < holdStart && holdStart < matchEnd {
					holdStart = matchStart
				}
				offset = matchStart + 1
			}
		}
		if holdStart == previousHoldStart {
			break
		}
	}

	if holdStart == len(data) {
		if err := w.writeMasked(data); err != nil {
			return 0, err
		}
		return len(p), nil
	}

	if len(data) > pipeline.MaxLogLineLength {
		if len(data)-holdStart > pipeline.MaxLogLineLength {
			// A candidate larger than the bound cannot be retained safely.
			w.maskUntilNewline = true
			return len(p), nil
		}
		if err := w.writeMasked(data[:holdStart]); err != nil {
			return 0, err
		}
		data = data[holdStart:]
	}

	// Hold the entire unterminated part to preserve the downstream write frame.
	w.pending = bytes.Clone(data)
	return len(p), nil
}

func (w *secretsReplaceWriter) writeMasked(p []byte) error {
	var matches []secretMatch
	for _, secret := range w.secrets {
		for offset := 0; offset <= len(p)-len(secret); {
			match := bytes.Index(p[offset:], secret)
			if match < 0 {
				break
			}
			start := offset + match
			matches = append(matches, secretMatch{start: start, end: start + len(secret)})
			offset = start + 1
		}
	}
	if len(matches) == 0 {
		return w.writeOutput(p)
	}
	slices.SortFunc(matches, func(a, b secretMatch) int {
		if a.start != b.start {
			return cmp.Compare(a.start, b.start)
		}
		return cmp.Compare(a.end, b.end)
	})

	out := make([]byte, 0, len(p))
	cursor := 0
	for i := 0; i < len(matches); {
		start, end := matches[i].start, matches[i].end
		i++
		for i < len(matches) && matches[i].start <= end {
			end = max(end, matches[i].end)
			i++
		}
		out = append(out, p[cursor:start]...)
		out = append(out, secretsMask...)
		cursor = end
	}
	out = append(out, p[cursor:]...)
	return w.writeOutput(out)
}

func (w *secretsReplaceWriter) writeOutput(p []byte) error {
	if len(p) == 0 {
		return nil
	}
	if len(p) <= pipeline.MaxLogLineLength {
		_, err := w.dst.Write(p)
		return err
	}
	for len(p) > pipeline.MaxLogLineLength {
		if _, err := w.dst.Write(p[:pipeline.MaxLogLineLength]); err != nil {
			return err
		}
		p = p[pipeline.MaxLogLineLength:]
	}
	if len(p) > 0 {
		_, err := w.dst.Write(p)
		return err
	}
	return nil
}

func (w *secretsReplaceWriter) Close() error {
	if w.maskUntilNewline {
		w.maskUntilNewline = false
		return w.writeOutput(secretsMask)
	}
	if len(w.pending) == 0 {
		return nil
	}
	if err := w.writeMasked(w.pending); err != nil {
		return err
	}
	w.pending = nil
	return nil
}
