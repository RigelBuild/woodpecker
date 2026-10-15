// Copyright 2022 Woodpecker Authors
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
	"time"

	"github.com/rs/zerolog/log"

	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml/compiler"
	"go.woodpecker-ci.org/woodpecker/v3/server"
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

// Approve update the status to pending for a blocked pipeline so it can be executed.
func Approve(
	ctx context.Context,
	store store.Store,
	currentPipeline *model.Pipeline,
	user *model.User,
	repo *model.Repo,
	approval *Approval,
) (*model.Pipeline, error) {
	if currentPipeline.Status != model.StatusBlocked {
		return nil, ErrBadRequest{Msg: fmt.Sprintf("cannot approve a pipeline with status %s", currentPipeline.Status)}
	}

	forge, err := server.Config.Services.Manager.ForgeFromRepo(repo)
	if err != nil {
		msg := fmt.Sprintf("failure to load forge for repo '%s'", repo.FullName)
		log.Error().Err(err).Str("repo", repo.FullName).Msg(msg)
		return nil, errors.New(msg)
	}

	// fetch the pipeline file from the database
	configs, err := store.ConfigsForPipeline(currentPipeline.ID)
	if err != nil {
		msg := fmt.Sprintf("failure to get pipeline config for %s", repo.FullName)
		log.Error().Err(err).Msg(msg)
		return nil, ErrNotFound{Msg: msg}
	}
	var yamls []*forge_types.FileMeta
	for _, y := range configs {
		yamls = append(yamls, &forge_types.FileMeta{Data: y.Data, Name: y.Name})
	}

	// Release the gate before building workflows: saveWorkflowsFromPipelineBuilder
	// derives workflow and step state from the pipeline status, so the status
	// must already be pending when the new workflows are persisted.
	currentPipeline.Status = model.StatusPending
	// Drop the approval errors from the blocked parse; this parse records its own.
	currentPipeline.Errors = nil
	// Claim the pipeline only while it is still blocked, so an approval racing a
	// decline (or another approval) cannot queue it.
	if err := store.UpdatePipelineIfStatus(currentPipeline, model.StatusBlocked); err != nil {
		if errors.Is(err, types.ErrUpdateConflict) {
			return nil, ErrBadRequest{Msg: "cannot approve a pipeline that is no longer blocked"}
		}
		return nil, fmt.Errorf("error claiming blocked pipeline. %w", err)
	}
	claimed := currentPipeline

	currentPipeline, pipelineItems, parseErr, err := createPipelineItems(
		ctx,
		forge,
		store,
		currentPipeline,
		user,
		repo,
		yamls,
		nil,
		true,
		approval,
	)
	// An approval that cannot release a gated secret leaves the pipeline blocked.
	if compiler.OnlyApprovalErrors(parseErr) {
		claimed.Errors = pipeline_errors.GetPipelineErrors(parseErr)
		claimed.Status = model.StatusBlocked
		if err := releaseClaim(store, claimed); err != nil {
			return nil, fmt.Errorf("error re-blocking pipeline. %w", err)
		}
		publishPipeline(ctx, forge, store, claimed, repo, user)
		return claimed, nil
	}
	if handleParseErrors(claimed, parseErr) {
		claimed.Errors = pipeline_errors.GetPipelineErrors(parseErr)
		claimed.Status = model.StatusError
		claimed.Started = time.Now().Unix()
		claimed.Finished = claimed.Started
		if err := releaseClaim(store, claimed); err != nil {
			return nil, fmt.Errorf("error setting error status of pipeline for %s#%d after approval: %w", repo.FullName, claimed.Number, err)
		}
		publishPipeline(ctx, forge, store, claimed, repo, user)
		msg := fmt.Sprintf("failure to parse pipeline config for %s", repo.FullName)
		log.Error().Err(parseErr).Msg(msg)
		return nil, errors.New(msg)
	}
	if err != nil {
		log.Error().Err(err).Str("repo", repo.FullName).Msgf("error persisting new steps for %s#%d after approval", repo.FullName, claimed.Number)
		// Undo the claim so the pipeline can be approved again.
		claimed.Status = model.StatusBlocked
		if rbErr := releaseClaim(store, claimed); rbErr != nil {
			return nil, fmt.Errorf("error persisting workflows after approval: %w; re-blocking failed: %w", err, rbErr)
		}
		return nil, err
	}

	// Finalize only while the claim still holds; a cancel that landed meanwhile
	// wins, and nothing is queued for it.
	currentPipeline.Reviewer = user.Login
	currentPipeline.Reviewed = time.Now().Unix()
	if err := releaseClaim(store, currentPipeline); err != nil {
		return nil, fmt.Errorf("error updating pipeline. %w", err)
	}

	publishPipeline(ctx, forge, store, currentPipeline, repo, user)

	currentPipeline, err = start(ctx, forge, store, currentPipeline, user, repo, pipelineItems)
	if err != nil {
		msg := fmt.Sprintf("failure to start pipeline for %s: %v", repo.FullName, err)
		log.Error().Err(err).Msg(msg)
		return nil, errors.New(msg)
	}

	return currentPipeline, nil
}

// releaseClaim writes the pipeline only while it still holds Approve's pending
// claim. Losing means a concurrent cancel already moved it, and that result stands.
func releaseClaim(store store.Store, pipeline *model.Pipeline) error {
	err := store.UpdatePipelineIfStatus(pipeline, model.StatusPending)
	if errors.Is(err, types.ErrUpdateConflict) {
		return ErrBadRequest{Msg: "the pipeline was canceled during approval"}
	}
	return err
}
