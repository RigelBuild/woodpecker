// Copyright 2022 Woodpecker Authors
// Copyright 2019 mhmxs
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
	"bytes"
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types"
	"go.woodpecker-ci.org/woodpecker/v3/rpc"
	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

func CalcStepStatus(step model.Step, state rpc.StepState) (_ *model.Step, cancelPipelineFromStep bool, _ error) {
	log.Debug().Str("StepUUID", step.UUID).Msg("calculate step status")

	switch step.State {
	case model.StatusPending:
		if state.Skipped {
			step.State = model.StatusSkipped
			step.Started = state.Started
			step.Finished = state.Finished
			step.ExitCode = state.ExitCode
			step.Error = state.Error
			if state.Exited || state.Error != "" {
				storeStepOutputs(&step, state)
			}
			return &step, false, nil
		}

		// Transition from pending to running when started.
		if state.Finished == 0 {
			step.State = model.StatusRunning
		}
		step.Started = state.Started
		if step.Started == 0 {
			step.Started = time.Now().Unix()
		}

		// Handle direct transition to finished if step setup error happened.
		if state.Exited || state.Error != "" {
			step.Finished = state.Finished
			if step.Finished == 0 {
				step.Finished = time.Now().Unix()
			}
			step.ExitCode = state.ExitCode
			step.Error = state.Error

			switch {
			case state.ExitCode == 0 && state.Error == "":
				step.State = model.StatusSuccess
			default:
				step.State = model.StatusFailure
				if step.Failure == model.FailureCancel {
					cancelPipelineFromStep = true
				}
			}
		}

	case model.StatusRunning:
		// Already running, check if it finished.
		if state.Exited || state.Error != "" {
			step.Finished = state.Finished
			if step.Finished == 0 {
				step.Finished = time.Now().Unix()
			}
			step.ExitCode = state.ExitCode
			step.Error = state.Error

			switch {
			case state.Skipped:
				step.State = model.StatusSkipped
			case state.ExitCode == 0 && state.Error == "":
				step.State = model.StatusSuccess
			default:
				step.State = model.StatusFailure
				if step.Failure == model.FailureCancel {
					cancelPipelineFromStep = true
				}
			}
		}

	default:
		return nil, false, fmt.Errorf("step has state %s and does not expect rpc state updates", step.State)
	}

	// Handle cancellation across both cases.
	if state.Canceled && step.State != model.StatusKilled {
		step.State = model.StatusKilled
		if step.Finished == 0 {
			step.Finished = time.Now().Unix()
		}
	}

	if state.Exited || state.Error != "" {
		storeStepOutputs(&step, state)
	}

	return &step, cancelPipelineFromStep, nil
}

func storeStepOutputs(step *model.Step, state rpc.StepState) {
	if state.OutputsState == "" && state.Outputs == nil {
		return
	}
	step.Outputs = nil
	step.OutputsState = "invalid"
	if state.OutputsState == "invalid" {
		return
	}
	if state.OutputsState != "reported" {
		log.Warn().Str("step_uuid", step.UUID).Msg("invalid step output state")
		return
	}
	var data bytes.Buffer
	for key, value := range state.Outputs {
		separator := 0
		if data.Len() > 0 {
			separator = 1
		}
		if data.Len()+separator+len(key)+len(value)+1 > types.MaxStepOutputBytes {
			log.Warn().Str("step_uuid", step.UUID).Msg("invalid step outputs")
			return
		}
		if separator != 0 {
			data.WriteByte('\n')
		}
		data.WriteString(key)
		data.WriteByte('=')
		data.WriteString(value)
	}
	outputs, err := types.ParseStepOutputs(data.Bytes())
	if err != nil || !maps.Equal(outputs, state.Outputs) {
		log.Warn().Str("step_uuid", step.UUID).Msg("invalid step outputs")
		return
	}
	step.Outputs = outputs
	step.OutputsState = "reported"
}

// UpdateStepStatus updates step status based on agent reports via RPC.
func UpdateStepStatus(ctx context.Context, store store.Store, step *model.Step, state rpc.StepState) error {
	log.Debug().Str("StepUUID", step.UUID).Msg("update step status")

	updatedStep, shouldCancelPipelineFromStep, err := CalcStepStatus(*step, state)
	if err != nil {
		return err
	}
	*step = *updatedStep // update step for external callers

	if shouldCancelPipelineFromStep {
		if err := cancelPipelineFromStep(ctx, store, step); err != nil {
			return err
		}
	}
	return store.StepUpdate(step)
}

func cancelPipelineFromStep(ctx context.Context, store store.Store, step *model.Step) error {
	pipeline, err := store.GetPipeline(step.PipelineID)
	if err != nil {
		return err
	}

	repo, err := store.GetRepo(pipeline.RepoID)
	if err != nil {
		return err
	}

	repoUser, err := store.GetUser(repo.UserID)
	if err != nil {
		return err
	}

	_forge, err := server.Config.Services.Manager.ForgeFromRepo(repo)
	if err != nil {
		return err
	}
	return Cancel(ctx, _forge, store, repo, repoUser, pipeline, &model.CancelInfo{
		CanceledByStep: step.Name,
	})
}

func UpdateStepToStatusSkipped(store store.Store, step model.Step, finished int64, status model.StatusValue) (*model.Step, error) {
	step.State = status
	if step.Started != 0 {
		step.State = model.StatusSuccess // for daemons that are killed
		step.Finished = finished
	}
	return &step, store.StepUpdate(&step)
}
