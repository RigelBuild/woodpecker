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
	boundarySteps    *int // Enables deterministic complexity checks in tests.
	maxMatchRanges   *int // Enables deterministic range-count checks in tests.
}

func (w *secretsReplaceWriter) recordBoundaryStep() {
	w.recordBoundarySteps(1)
}

func (w *secretsReplaceWriter) recordBoundarySteps(steps int) {
	if w.boundarySteps != nil {
		*w.boundarySteps += steps
	}
}

func (w *secretsReplaceWriter) recordMatchRanges(count int) {
	if w.maxMatchRanges != nil {
		*w.maxMatchRanges = max(*w.maxMatchRanges, count)
	}
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
	return newSecretsReplaceWriter(dst, secrets)
}

func newSecretsReplaceWriter(dst io.Writer, secrets []string) *secretsReplaceWriter {
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
		return w.writePassThrough(p)
	}
	if len(p) == 0 {
		return 0, nil
	}
	if w.maskUntilNewline {
		return w.writeMaskedUntilNewline(p)
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

	holdStart := w.findBoundary(data)
	return w.holdPendingLine(p, data, holdStart)
}

func (w *secretsReplaceWriter) writePassThrough(p []byte) (int, error) {
	n, err := w.dst.Write(p)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func (w *secretsReplaceWriter) writeMaskedUntilNewline(p []byte) (int, error) {
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

func (w *secretsReplaceWriter) holdPendingLine(p, data []byte, holdStart int) (int, error) {
	if holdStart == len(data) {
		if err := w.writeMasked(data); err != nil {
			return 0, err
		}
		return len(p), nil
	}

	if len(data) > pipeline.MaxLogLineLength {
		if len(data)-holdStart > pipeline.MaxLogLineLength {
			// A candidate larger than the bound cannot be retained safely.
			if err := w.writeMasked(data[:holdStart]); err != nil {
				return 0, err
			}
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

func (w *secretsReplaceWriter) matchRanges(p []byte) ([]secretMatch, int) {
	holdStart := len(p)
	var matches []secretMatch
	for i, secret := range w.secrets {
		matched := 0
		var current secretMatch
		hasCurrent := false
		for offset, b := range p {
			w.recordBoundaryStep()
			for matched > 0 && b != secret[matched] {
				w.recordBoundaryStep()
				matched = w.prefixTables[i][matched-1]
			}
			if b == secret[matched] {
				matched++
			}
			if matched == len(secret) {
				start := offset + 1 - len(secret)
				end := offset + 1
				w.recordBoundaryStep()
				if hasCurrent && start <= current.end {
					current.end = max(current.end, end)
				} else {
					if hasCurrent {
						matches = append(matches, current)
					}
					current = secretMatch{start: start, end: end}
					hasCurrent = true
				}
				matched = w.prefixTables[i][matched-1]
			}
		}
		if matched > 0 {
			holdStart = min(holdStart, len(p)-matched)
		}
		if hasCurrent {
			matches = append(matches, current)
		}
	}

	slices.SortFunc(matches, func(a, b secretMatch) int {
		w.recordBoundaryStep()
		if a.start != b.start {
			return cmp.Compare(a.start, b.start)
		}
		return cmp.Compare(a.end, b.end)
	})
	merged := matches[:0]
	for _, match := range matches {
		w.recordBoundaryStep()
		if len(merged) > 0 && match.start <= merged[len(merged)-1].end {
			merged[len(merged)-1].end = max(merged[len(merged)-1].end, match.end)
			continue
		}
		merged = append(merged, match)
	}
	w.recordMatchRanges(len(merged))
	return merged, holdStart
}

func (w *secretsReplaceWriter) findBoundary(data []byte) int {
	matches, holdStart := w.matchRanges(data)
	for _, match := range matches {
		w.recordBoundaryStep()
		if match.start < holdStart && holdStart < match.end {
			return match.start
		}
	}
	return holdStart
}

func (w *secretsReplaceWriter) writeMasked(p []byte) error {
	matches, _ := w.matchRanges(p)
	if len(matches) == 0 {
		return w.writeOutput(p)
	}

	out := make([]byte, 0, len(p))
	cursor := 0
	for _, match := range matches {
		out = append(out, p[cursor:match.start]...)
		out = append(out, secretsMask...)
		cursor = match.end
	}
	out = append(out, p[cursor:]...)
	return w.writeOutput(out)
}

func (w *secretsReplaceWriter) writeOutput(p []byte) error {
	if len(p) == 0 {
		return nil
	}
	if len(p) <= pipeline.MaxLogLineLength {
		return writeFull(w.dst, p)
	}
	for len(p) > pipeline.MaxLogLineLength {
		if err := writeFull(w.dst, p[:pipeline.MaxLogLineLength]); err != nil {
			return err
		}
		p = p[pipeline.MaxLogLineLength:]
	}
	if len(p) > 0 {
		return writeFull(w.dst, p)
	}
	return nil
}

func writeFull(dst io.Writer, p []byte) error {
	n, err := dst.Write(p)
	if err == nil && n != len(p) {
		return io.ErrShortWrite
	}
	return err
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
