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

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/builder"
	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

const (
	// The detection and delivery loops run every sweepInterval.
	sweepInterval = time.Minute
	// A pipeline in created for longer than orphanThreshold is flagged.
	orphanThreshold = 10 * time.Minute
	// One detection tick flags at most sweepBatch orphans.
	sweepBatch = 10
	// One delivery tick delivers at most deliverBatch reports.
	deliverBatch = 10
	// A delivery holds a report for reportLease, which exceeds
	// reportPostTimeout plus the store reads and writes.
	reportLease = 5 * time.Minute
	// One forge post is bounded by reportPostTimeout.
	reportPostTimeout = time.Minute
	// A terminal post is re-posted after reportSettle, which exceeds
	// reportLease plus reportPostTimeout.
	reportSettle = 10 * time.Minute
	// The backoff of a failed delivery starts at reportRetryBase, doubles up to
	// reportRetryMaxShift times, and is capped at reportRetryMax.
	reportRetryBase     = time.Minute
	reportRetryMax      = time.Hour
	reportRetryMaxShift = 6
	// The pipeline-level status uses the workflow name the builder reserves,
	// so no real workflow can share its status context.
	dispatchStatusWorkflowName = builder.DispatchStatusWorkflowName
)

// orphanMessage is the pipeline error stored on a flagged orphan.
const orphanMessage = "pipeline was created but never dispatched; restart it to run"

var (
	errPostInFlight = errors.New("previous post on this server has not returned")
	errPostTimeout  = errors.New("post timed out; the forge may have applied it")
)

// dispatchStatusWorkflow is the synthetic workflow the pipeline-level status
// is posted with. ID 0 and PID 0 never belong to a real workflow.
func dispatchStatusWorkflow(state model.StatusValue) *model.Workflow {
	return &model.Workflow{ID: 0, PID: 0, Name: dispatchStatusWorkflowName, State: state}
}

// dispatchPostState maps a mirrored pipeline status to the posted state.
// Forges map canceled and skipped to pending or to a failure; success is the
// only state that is neither red nor pending everywhere.
func dispatchPostState(s model.StatusValue) model.StatusValue {
	switch s {
	case model.StatusCanceled, model.StatusSkipped:
		return model.StatusSuccess
	default:
		return s
	}
}

// isTerminalStatus reports whether a pipeline in this status will not change.
func isTerminalStatus(s model.StatusValue) bool {
	switch s {
	case model.StatusSuccess, model.StatusFailure, model.StatusKilled, model.StatusError,
		model.StatusCanceled, model.StatusDeclined, model.StatusSkipped:
		return true
	default:
		return false
	}
}

// flagOrphanedPipeline claims a stale created pipeline and marks it as error
// with a dispatch report, in one conditional transaction. A lost claim or a
// creator that advanced the row first leaves it untouched. It makes no forge
// call and never enqueues the pipeline.
func flagOrphanedPipeline(_store store.Store, p *model.Pipeline, now int64) error {
	claimed := *p
	won, err := _store.PipelineDispatchClaim(&claimed, now)
	if err != nil {
		return fmt.Errorf("claim orphaned pipeline %d: %w", p.ID, err)
	}
	if !won {
		return nil
	}

	claimed.Errors = []*pipeline_errors.PipelineError{{
		Type:    pipeline_errors.PipelineErrorTypeGeneric,
		Message: orphanMessage,
	}}
	if _, err := _store.PipelineDispatchFlag(&claimed, now); err != nil {
		return fmt.Errorf("flag orphaned pipeline %d: %w", p.ID, err)
	}
	return nil
}

// runDispatchLivenessTick flags every orphan found at now. It makes no forge call.
func runDispatchLivenessTick(_ context.Context, _store store.Store, now time.Time) {
	orphans, err := _store.OrphanedDispatchPipelines(now.Add(-orphanThreshold).Unix(), sweepBatch)
	if err != nil {
		log.Error().Err(err).Int64("now", now.Unix()).Msg("dispatch liveness: list orphaned pipelines")
		return
	}

	for _, p := range orphans {
		if err := flagOrphanedPipeline(_store, p, now.Unix()); err != nil {
			log.Error().Err(err).Int64("pipeline", p.ID).Int64("repo", p.RepoID).
				Msg("dispatch liveness: flag orphaned pipeline")
		}
	}
}

// dispatchDelivery delivers dispatch reports. It keeps the set of reports
// whose post on this server has not returned, which outlives a tick.
type dispatchDelivery struct {
	store       store.Store
	clock       func() time.Time
	postTimeout time.Duration
	mu          sync.Mutex
	inFlight    map[int64]struct{} // report IDs whose post on this server has not returned
}

func newDispatchDelivery(_store store.Store) *dispatchDelivery {
	return &dispatchDelivery{
		store:       _store,
		clock:       time.Now,
		postTimeout: reportPostTimeout,
		inFlight:    make(map[int64]struct{}),
	}
}

// retry records a failed delivery attempt and reschedules the report with backoff.
func (d *dispatchDelivery) retry(report *model.PipelineDispatchReport, cause error) error {
	report.Attempts++
	return d.reschedule(report, cause)
}

// reschedule reschedules a report whose attempt was already counted, with backoff.
func (d *dispatchDelivery) reschedule(report *model.PipelineDispatchReport, cause error) error {
	backoff := min(reportRetryBase<<min(report.Attempts-1, reportRetryMaxShift), reportRetryMax)
	report.NextAttempt = d.clock().Add(backoff).Unix()
	report.LastError = cause.Error()
	return errors.Join(cause, d.write(report, d.store.DispatchReportUpdate))
}

// write applies a report write under its lease. A lost lease is logged and
// is not an error: another server holds the row.
func (d *dispatchDelivery) write(report *model.PipelineDispatchReport, fn func(*model.PipelineDispatchReport) error) error {
	err := fn(report)
	if errors.Is(err, types.ErrRecordNotExist) {
		log.Info().Int64("report", report.ID).Int64("pipeline", report.PipelineID).
			Msg("dispatch liveness: report lease lost")
		return nil
	}
	return err
}

func (d *dispatchDelivery) isInFlight(id int64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.inFlight[id]
	return ok
}

// deliverDispatchReport claims a report and posts the pipeline-level status
// it should show, then records the result.
func (d *dispatchDelivery) deliverDispatchReport(ctx context.Context, report *model.PipelineDispatchReport) error {
	leaseUntil := d.clock().Add(reportLease)
	won, err := d.store.DispatchReportClaim(report, leaseUntil.Unix())
	if err != nil {
		return fmt.Errorf("claim dispatch report %d: %w", report.ID, err)
	}
	if !won {
		return nil
	}

	if d.isInFlight(report.ID) {
		return d.retry(report, errPostInFlight)
	}

	var (
		base     *model.Pipeline
		desired  model.StatusValue
		retiring bool
	)
	p, err := d.store.GetPipeline(report.PipelineID)
	switch {
	case errors.Is(err, types.ErrRecordNotExist):
		if report.Delivered == "" && report.Attempts == 0 {
			return d.write(report, d.store.DispatchReportDelete)
		}
		// A red may have landed; clear it from the snapshot.
		base, desired, retiring = report.Snapshot, model.StatusSuccess, true
	case err != nil:
		return d.retry(report, fmt.Errorf("get pipeline: %w", err))
	default:
		base, desired = p, p.Status
		held := p.Status == model.StatusError && p.Updated == report.FlaggedAt
		hasSuccessor := false
		if held {
			successor, err := d.store.PipelineDispatchSuccessor(p)
			switch {
			case errors.Is(err, types.ErrRecordNotExist):
			case err != nil:
				return d.retry(report, fmt.Errorf("get later pipeline: %w", err))
			default:
				desired, hasSuccessor = successor.Status, true
			}
		}
		retiring = isTerminalStatus(desired) && (!held || hasSuccessor)
	}

	repo, err := d.store.GetRepo(report.RepoID)
	if errors.Is(err, types.ErrRecordNotExist) {
		return d.write(report, d.store.DispatchReportDelete)
	}
	if err != nil {
		return d.retry(report, fmt.Errorf("get repo: %w", err))
	}
	user, err := d.store.GetUser(repo.UserID)
	if err != nil {
		return d.retry(report, fmt.Errorf("get repo owner: %w", err))
	}
	_forge, err := server.Config.Services.Manager.ForgeFromRepo(repo)
	if err != nil {
		return d.retry(report, fmt.Errorf("get forge: %w", err))
	}

	state := dispatchPostState(desired)
	posted := *base
	posted.Status = state
	deadline := d.clock().Add(d.postTimeout)
	if leaseUntil.Before(deadline) {
		deadline = leaseUntil
	}
	postCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	// Record the attempt before posting. If the forge accepts the post but the
	// result is never recorded, attempts > 0 still tells a later delivery that
	// a red may have landed, so a deleted pipeline gets its success post.
	report.Attempts++
	if err := d.store.DispatchReportUpdate(report); err != nil {
		if errors.Is(err, types.ErrRecordNotExist) {
			log.Info().Int64("report", report.ID).Int64("pipeline", report.PipelineID).
				Msg("dispatch liveness: report lease lost")
			return nil
		}
		return fmt.Errorf("record post attempt: %w", err)
	}

	d.mu.Lock()
	d.inFlight[report.ID] = struct{}{}
	d.mu.Unlock()
	result := make(chan error, 1)
	go func() {
		forge.Refresh(postCtx, _forge, d.store, user)
		err := _forge.Status(postCtx, user, repo, &posted, dispatchStatusWorkflow(state))
		d.mu.Lock()
		delete(d.inFlight, report.ID)
		d.mu.Unlock()
		result <- err
	}()

	select {
	case err := <-result:
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				// The tick was canceled; no further write, the lease expires.
				return ctxErr
			}
			return d.reschedule(report, fmt.Errorf("post status: %w", err))
		}
	case <-postCtx.Done():
		// postCtx derives from ctx, so a canceled tick also lands here.
		if err := ctx.Err(); err != nil {
			// No further write: the lease expires and the row becomes due again.
			return err
		}
		return d.reschedule(report, errPostTimeout)
	}

	switch {
	case retiring && report.RetireAfter > 0 && state == dispatchPostState(report.Delivered):
		// This was the settle re-post.
		return d.write(report, d.store.DispatchReportDelete)
	case retiring:
		report.Delivered = desired
		report.Attempts = 0
		report.LastError = ""
		report.RetireAfter = d.clock().Add(reportSettle).Unix()
		report.NextAttempt = report.RetireAfter
	default:
		report.Delivered = desired
		report.Attempts = 0
		report.LastError = ""
		report.RetireAfter = 0
		report.NextAttempt = d.clock().Unix()
	}
	return d.write(report, d.store.DispatchReportUpdate)
}

// runDispatchDeliveryTick delivers every report due now.
func (d *dispatchDelivery) runDispatchDeliveryTick(ctx context.Context) {
	now := d.clock().Unix()
	reports, err := d.store.DispatchReportsDue(now, deliverBatch)
	if err != nil {
		log.Error().Err(err).Int64("now", now).Msg("dispatch liveness: list due dispatch reports")
		return
	}

	for _, report := range reports {
		if err := d.deliverDispatchReport(ctx, report); err != nil {
			log.Error().Err(err).Int64("report", report.ID).Int64("pipeline", report.PipelineID).
				Int64("repo", report.RepoID).Msg("dispatch liveness: deliver dispatch report")
		}
	}
}

// RunDispatchLivenessSweeper runs the orphan detection loop and the report
// delivery loop until ctx is canceled. Delivery runs on its own loop, so a
// hung forge call cannot stop detection.
func RunDispatchLivenessSweeper(ctx context.Context, _store store.Store) error {
	var g errgroup.Group
	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(sweepInterval):
				runDispatchLivenessTick(ctx, _store, time.Now())
			}
		}
	})
	g.Go(func() error {
		delivery := newDispatchDelivery(_store)
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(sweepInterval):
				delivery.runDispatchDeliveryTick(ctx)
			}
		}
	})
	return g.Wait()
}
