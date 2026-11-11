// Copyright 2023 Woodpecker Authors
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
	"context"
	"sync/atomic"
)

type teardownContextKey struct{}

// WithTeardown returns a context that reports teardown while flag is set.
func WithTeardown(ctx context.Context, flag *atomic.Bool) context.Context {
	return context.WithValue(ctx, teardownContextKey{}, flag)
}

// InTeardown reports whether ctx carries a set teardown flag.
func InTeardown(ctx context.Context) bool {
	flag, ok := ctx.Value(teardownContextKey{}).(*atomic.Bool)
	return ok && flag != nil && flag.Load()
}

// Config defines the runtime configuration of a workflow.
type Config struct {
	Stages  []*Stage  `json:"pipeline"` // workflow stages
	Network string    `json:"network"`  // network definition
	Volume  string    `json:"volume"`   // volume definition
	Secrets []*Secret `json:"secrets"`  // secret definitions
}

// CliCommand is the context key to pass cli context to backends if needed.
var CliCommand contextKey

// contextKey is just an empty struct. It exists so CliCommand can be
// an immutable public variable with a unique type. It's immutable
// because nobody else can create a ContextKey, being unexported.
type contextKey struct{}
