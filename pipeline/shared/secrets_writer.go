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
)

// Strings not longer than minSecretLength are not considered secrets.
// Do not sanitize them.
const minSecretLength = 3

var secretsMask = []byte("********")

type secretsReplaceWriter struct {
	dst          io.Writer
	secrets      [][]byte
	maxSecretLen int
	maxWriteSize int
	pending      []byte
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
			w.maxSecretLen = max(w.maxSecretLen, len(secretBytes))
		}
	}
	// Secrets are replaced one after another, so mask the longest first.
	// Else a secret that is part of a longer one would leak the rest of it.
	slices.SortStableFunc(w.secrets, func(a, b []byte) int {
		return cmp.Compare(len(b), len(a))
	})
	return w
}

func (w *secretsReplaceWriter) Write(p []byte) (int, error) {
	if len(w.secrets) == 0 {
		if _, err := w.dst.Write(p); err != nil {
			return 0, err
		}
		return len(p), nil
	}

	w.maxWriteSize = max(w.maxWriteSize, len(p))
	data := p
	if len(w.pending) > 0 {
		data = make([]byte, len(w.pending)+len(p))
		copy(data, w.pending)
		copy(data[len(w.pending):], p)
	}
	w.pending = nil

	// A secret part cannot cross a newline, so complete lines can be flushed.
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

	// Keep any suffix that could become a secret when the next write arrives.
	holdStart := len(data)
	for i := max(0, len(data)-(w.maxSecretLen-1)); i < len(data); i++ {
		suffix := data[i:]
		for _, secret := range w.secrets {
			if len(suffix) < len(secret) && bytes.HasPrefix(secret, suffix) {
				holdStart = min(holdStart, i)
			}
		}
	}
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
					holdStart = matchEnd
				}
				offset = matchStart + 1
			}
		}
		if holdStart == previousHoldStart {
			break
		}
	}

	if holdStart < len(data) {
		w.pending = bytes.Clone(data[holdStart:])
	}
	if holdStart > 0 {
		if err := w.writeMasked(data[:holdStart]); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

func (w *secretsReplaceWriter) writeMasked(p []byte) error {
	out := p
	for _, secret := range w.secrets {
		// Lines without a secret are passed on as is, without a copy.
		if bytes.Contains(out, secret) {
			out = bytes.ReplaceAll(out, secret, secretsMask)
		}
	}
	if len(p) <= w.maxWriteSize || len(out) <= w.maxWriteSize {
		_, err := w.dst.Write(out)
		return err
	}
	for len(out) > w.maxWriteSize {
		if _, err := w.dst.Write(out[:w.maxWriteSize]); err != nil {
			return err
		}
		out = out[w.maxWriteSize:]
	}
	if len(out) > 0 {
		_, err := w.dst.Write(out)
		return err
	}
	return nil
}

func (w *secretsReplaceWriter) Close() error {
	if len(w.pending) == 0 {
		return nil
	}
	if err := w.writeMasked(w.pending); err != nil {
		return err
	}
	w.pending = nil
	return nil
}
