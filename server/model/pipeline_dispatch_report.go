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

package model

// PipelineDispatchReport is the delivery state of the pipeline-level status
// posted for a pipeline the dispatch-liveness sweeper flagged as orphaned.
// It lives outside the pipelines table, because UpdatePipeline rewrites every
// column and DeletePipeline removes the row.
type PipelineDispatchReport struct {
	ID         int64 `xorm:"pk autoincr 'id'"`
	PipelineID int64 `xorm:"UNIQUE 'pipeline_id'"`
	RepoID     int64 `xorm:"INDEX 'repo_id'"`
	// FlaggedAt is the pipeline's updated value written by the flag.
	FlaggedAt int64 `xorm:"'flagged_at'"`
	// Delivered is the last pipeline status the forge accepted; empty before the first success.
	Delivered StatusValue `xorm:"'delivered'"`
	Attempts  int         `xorm:"'attempts'"`
	// NextAttempt is the due time, or the lease expiry while claimed.
	NextAttempt int64 `xorm:"INDEX 'next_attempt'"`
	// Claim is the lease token; it only grows.
	Claim     int64  `xorm:"'claim'"`
	LastError string `xorm:"TEXT 'last_error'"`
	// Snapshot is the flagged pipeline, so a post works after the row is deleted.
	Snapshot *Pipeline `xorm:"json 'snapshot'"`
	// RetireAfter is the end of the settle window; 0 when not settling.
	RetireAfter int64 `xorm:"'retire_after'"`
}

// TableName return database table name for xorm.
func (PipelineDispatchReport) TableName() string {
	return "pipeline_dispatch_reports"
}
