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
	"fmt"

	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

// orphanMessage is the pipeline error stored on a flagged orphan.
const orphanMessage = "pipeline was created but never dispatched; restart it to run"

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
