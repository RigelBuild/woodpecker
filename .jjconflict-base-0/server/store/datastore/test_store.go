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

//go:build test

package datastore

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"xorm.io/xorm/contexts"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

// NewTestStore creates a fully-migrated in-memory sqlite store for use in
// tests of other packages (e.g. server/api).
//
// The returned store is automatically closed on test cleanup.
func NewTestStore(t *testing.T) store.Store {
	t.Helper()

	s, err := NewEngine(&store.Opts{
		Driver: "sqlite3",
		Config: ":memory:",
		// MaxOpenConns=1 and MaxIdleConns=1 are required for in-memory sqlite:
		// without them the pool drops idle connections, destroying the in-memory
		// schema between calls and breaking migrations.
		XORM: store.XORM{
			MaxOpenConns: 1,
			MaxIdleConns: 1,
		},
	})
	require.NoError(t, err, "create test store")
	require.NoError(t, s.Ping(), "ping test store")
	require.NoError(t, s.Migrate(t.Context(), true), "migrate test store")

	t.Cleanup(func() { _ = s.Close() })
	return s
}

// DispatchReportsForTest returns every dispatch report row of a store created
// by NewTestStore, including rows that are not due.
func DispatchReportsForTest(t *testing.T, s store.Store) []*model.PipelineDispatchReport {
	t.Helper()

	st, ok := s.(*storage)
	require.True(t, ok, "store must come from NewTestStore")
	reports := make([]*model.PipelineDispatchReport, 0)
	require.NoError(t, st.engine.Asc("id").Find(&reports))
	return reports
}

// QueryCounter counts every statement a test store sends to its database,
// including BEGIN and COMMIT: each one is a round trip.
type QueryCounter struct {
	n atomic.Int64
}

func (c *QueryCounter) BeforeProcess(h *contexts.ContextHook) (context.Context, error) {
	c.n.Add(1)
	return h.Ctx, nil
}

func (c *QueryCounter) AfterProcess(*contexts.ContextHook) error {
	return nil
}

// Count returns the number of statements fn sends.
func (c *QueryCounter) Count(fn func()) int64 {
	before := c.n.Load()
	fn()
	return c.n.Load() - before
}

// CountQueries hooks a counter into a store created by NewTestStore.
func CountQueries(t *testing.T, s store.Store) *QueryCounter {
	t.Helper()

	st, ok := s.(*storage)
	require.True(t, ok, "store must come from NewTestStore")
	c := new(QueryCounter)
	st.engine.AddHook(c)
	return c
}

// Query budget sizes: a hot path may send at most queryBudgetSlack more
// statements for queryBudgetLarge items than for queryBudgetSmall.
const (
	queryBudgetSmall = 10
	queryBudgetLarge = 100
	queryBudgetSlack = 2
)

// AssertQueryBudget runs measure for a small and a large n and fails if the
// larger run sends more than queryBudgetSlack statements beyond the smaller.
func AssertQueryBudget(t *testing.T, measure func(n int) int64) {
	t.Helper()

	small, large := measure(queryBudgetSmall), measure(queryBudgetLarge)
	t.Logf("queries: n=%d %d, n=%d %d", queryBudgetSmall, small, queryBudgetLarge, large)
	assert.LessOrEqual(t, large-small, int64(queryBudgetSlack), "query count must not grow with n")
}
