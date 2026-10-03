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

package rpc

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

// recordingPoster records each posted status in order. A post of a pipeline
// whose status is in `hold` blocks until release is closed.
type recordingPoster struct {
	mu      sync.Mutex
	posted  []model.StatusValue
	hold    model.StatusValue
	release chan struct{}
	held    chan struct{}
	// done, when set, receives each status as soon as it is recorded.
	done chan model.StatusValue
	once sync.Once
}

func (r *recordingPoster) post(_ context.Context, _ *model.Repo, p *model.Pipeline, _ *model.Workflow) {
	held := false
	if p.Status == r.hold {
		r.once.Do(func() { held = true })
	}
	if held {
		close(r.held)
		<-r.release
	}
	r.mu.Lock()
	r.posted = append(r.posted, p.Status)
	r.mu.Unlock()
	if r.done != nil {
		r.done <- p.Status
	}
}

func newReportRPC(t *testing.T, s *store_mocks.MockStore, poster *recordingPoster) (*RPC, *sync.WaitGroup) {
	t.Helper()
	wg := &sync.WaitGroup{}
	return &RPC{store: s, reportWG: wg, reports: newInflightReports(), postStatus: poster.post}, wg
}

// A running-snapshot report that stalls (rate-limit backoff) and lands after
// the pipeline's success must be followed by a re-posted success.
func TestReportForgeStatusReassertsTerminalAfterSlowerOlderReport(t *testing.T) {
	s := store_mocks.NewMockStore(t)
	// The stalled report re-reads the store before posting; the pipeline is still
	// running at that moment, so it posts the stale "running" verdict.
	s.On("GetPipeline", int64(20)).Return(defaultPipeline(model.StatusRunning), nil).Once()
	poster := &recordingPoster{
		hold: model.StatusRunning, release: make(chan struct{}), held: make(chan struct{}),
		done: make(chan model.StatusValue, 3),
	}
	r, wg := newReportRPC(t, s, poster)
	repo := defaultRepo()

	r.reportForgeStatusAsync(t.Context(), repo, defaultPipeline(model.StatusRunning), nil)
	<-poster.held
	r.reportForgeStatusAsync(t.Context(), repo, defaultPipeline(model.StatusSuccess), nil)
	assert.Equal(t, model.StatusSuccess, <-poster.done) // the terminal report lands first
	close(poster.release)
	wg.Wait()

	assert.Equal(t, []model.StatusValue{model.StatusSuccess, model.StatusRunning, model.StatusSuccess}, poster.posted)
}

// A report snapshotted mid-run that starts after the pipeline finished posts
// the stored terminal verdict, not its stale snapshot.
func TestReportForgeStatusPostsStoredTerminalOverRunningSnapshot(t *testing.T) {
	s := store_mocks.NewMockStore(t)
	s.On("GetPipeline", int64(20)).Return(defaultPipeline(model.StatusSuccess), nil)
	poster := &recordingPoster{}
	r, wg := newReportRPC(t, s, poster)

	r.reportForgeStatusAsync(t.Context(), defaultRepo(), defaultPipeline(model.StatusRunning), nil)
	wg.Wait()

	assert.Equal(t, []model.StatusValue{model.StatusSuccess}, poster.posted)
}

// With no overlapping report, a terminal report posts once: no extra forge write.
func TestReportForgeStatusPostsTerminalOnceWhenAlone(t *testing.T) {
	poster := &recordingPoster{}
	r, wg := newReportRPC(t, store_mocks.NewMockStore(t), poster)

	r.reportForgeStatusAsync(t.Context(), defaultRepo(), defaultPipeline(model.StatusSuccess), nil)
	wg.Wait()

	assert.Equal(t, []model.StatusValue{model.StatusSuccess}, poster.posted)
}
