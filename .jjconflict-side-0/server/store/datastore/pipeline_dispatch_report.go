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

package datastore

import (
	"fmt"
	"strings"

	"xorm.io/builder"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

// dispatchReportsDueTemplate selects due reports that have something to
// deliver. The alias p is the flagged pipeline, and s is the newest later run
// on the same repo, commit and event. A held flag (p still error with updated
// equal to flagged_at) mirrors s, or error when there is no later run.
const dispatchReportsDueTemplate = `SELECT r.* FROM pipeline_dispatch_reports r
LEFT JOIN pipelines p ON p.id = r.pipeline_id
LEFT JOIN pipelines s ON s.id = (
	SELECT MAX(q.id) FROM pipelines q
	WHERE q.repo_id = p.repo_id AND q.%[1]s = p.%[1]s AND q.event = p.event AND q.id > p.id
)
WHERE r.next_attempt <= ? AND (
	r.retire_after > 0
	OR p.id IS NULL
	OR (NOT (p.status = ? AND p.updated = r.flagged_at)
		AND (p.status <> r.delivered OR p.status IN (%[2]s)))
	OR (p.status = ? AND p.updated = r.flagged_at
		AND (COALESCE(s.status, ?) <> r.delivered OR s.status IN (%[2]s)))
)
ORDER BY r.next_attempt, r.id
LIMIT ?`

// dispatchTerminalStatuses are the pipeline states a dispatch report retires on.
var dispatchTerminalStatuses = []model.StatusValue{
	model.StatusSuccess,
	model.StatusFailure,
	model.StatusKilled,
	model.StatusError,
	model.StatusCanceled,
	model.StatusDeclined,
	model.StatusSkipped,
}

// DispatchReportsDue returns up to limit reports due at now whose pipeline
// state has not been delivered yet, whose pipeline is gone, or that are settling.
func (s storage) DispatchReportsDue(now int64, limit int) ([]*model.PipelineDispatchReport, error) {
	placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(dispatchTerminalStatuses)), ", ")
	query := fmt.Sprintf(dispatchReportsDueTemplate, s.engine.Dialect().Quoter().Quote("commit"), placeholders)

	args := make([]any, 0, 5+2*len(dispatchTerminalStatuses))
	args = append(args, now, model.StatusError)
	for _, status := range dispatchTerminalStatuses {
		args = append(args, status)
	}
	args = append(args, model.StatusError, model.StatusError)
	for _, status := range dispatchTerminalStatuses {
		args = append(args, status)
	}
	args = append(args, limit)

	reports := make([]*model.PipelineDispatchReport, 0, limit)
	return reports, s.engine.SQL(query, args...).Find(&reports)
}

// PipelineDispatchSuccessor returns the newest pipeline after the given one
// with the same repo, commit and event.
func (s storage) PipelineDispatchSuccessor(pipeline *model.Pipeline) (*model.Pipeline, error) {
	successor := new(model.Pipeline)
	return successor, wrapGet(s.engine.
		Where(builder.Eq{
			"repo_id": pipeline.RepoID,
			s.engine.Dialect().Quoter().Quote("commit"): pipeline.Commit,
			"event": pipeline.Event,
		}.And(builder.Gt{"id": pipeline.ID})).
		Desc("id").
		Get(successor))
}

// DispatchReportClaim leases a report until leaseUntil while its next_attempt
// and claim still match the passed report, and increments the claim token.
func (s storage) DispatchReportClaim(report *model.PipelineDispatchReport, leaseUntil int64) (bool, error) {
	claim := report.Claim + 1
	cols, err := s.engine.ID(report.ID).
		Where(builder.Eq{"next_attempt": report.NextAttempt, "claim": report.Claim}).
		Cols("next_attempt", "claim").
		Update(&model.PipelineDispatchReport{NextAttempt: leaseUntil, Claim: claim})
	if err != nil {
		return false, err
	}
	if cols == 0 {
		return false, nil
	}
	report.NextAttempt = leaseUntil
	report.Claim = claim
	return true, nil
}

// DispatchReportUpdate writes the delivery state of a report held under its
// claim. It returns types.ErrRecordNotExist when the lease was lost.
func (s storage) DispatchReportUpdate(report *model.PipelineDispatchReport) error {
	cols, err := s.engine.ID(report.ID).
		Where(builder.Eq{"claim": report.Claim}).
		Cols("delivered", "attempts", "next_attempt", "last_error", "retire_after").
		Update(report)
	if err != nil {
		return err
	}
	if cols == 0 {
		return types.ErrRecordNotExist
	}
	return nil
}

// DispatchReportDelete deletes a report held under its claim. It returns
// types.ErrRecordNotExist when the lease was lost or the row is gone.
func (s storage) DispatchReportDelete(report *model.PipelineDispatchReport) error {
	return wrapDelete(s.engine.ID(report.ID).
		Where(builder.Eq{"claim": report.Claim}).
		Delete(new(model.PipelineDispatchReport)))
}
