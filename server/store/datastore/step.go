// Copyright 2021 Woodpecker Authors
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
	"xorm.io/builder"
	"xorm.io/xorm"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

func (s storage) StepLoad(pipelineID, stepID int64) (*model.Step, error) {
	step := new(model.Step)
	return step, wrapGet(s.engine.ID(stepID).Where(builder.Eq{"pipeline_id": pipelineID}).Get(step))
}

func (s storage) StepByUUID(uuid string) (*model.Step, error) {
	step := new(model.Step)
	return step, wrapGet(s.engine.Where(
		builder.Eq{"uuid": uuid},
	).Get(step))
}

func (s storage) StepList(pipelineID int64) ([]*model.Step, error) {
	stepList := make([]*model.Step, 0)
	return stepList, s.engine.
		Where("pipeline_id = ?", pipelineID).
		OrderBy("pid").
		Find(&stepList)
}

func (s storage) StepListFromWorkflowFind(workflow *model.Workflow) ([]*model.Step, error) {
	return s.stepListWorkflow(s.engine.NewSession(), workflow)
}

func (s storage) stepListWorkflow(sess *xorm.Session, workflow *model.Workflow) ([]*model.Step, error) {
	stepList := make([]*model.Step, 0)
	return stepList, sess.
		Where("pipeline_id = ?", workflow.PipelineID).
		Where("ppid = ?", workflow.PID).
		OrderBy("pid").
		Find(&stepList)
}

// stepListPipeline lists every step of a pipeline in the order stepListWorkflow uses.
func (s storage) stepListPipeline(sess *xorm.Session, pipelineID int64) ([]*model.Step, error) {
	stepList := make([]*model.Step, 0)
	return stepList, sess.
		Where("pipeline_id = ?", pipelineID).
		OrderBy("pid").
		Find(&stepList)
}

func (s storage) stepCreate(sess *xorm.Session, steps []*model.Step) error {
	size, err := s.batchSize(new(model.Step))
	if err != nil {
		return err
	}
	return insertWithIDs(sess, "steps", steps, size,
		func(step *model.Step) *int64 { return &step.ID },
		func(step *model.Step) (int64, int) { return step.PipelineID, step.PID },
	)
}

func (s storage) StepUpdate(step *model.Step) error {
	_, err := s.engine.ID(step.ID).AllCols().Update(step)
	return err
}

// StepUpdateIfState updates reaper-owned fields while the step and parent match.
func (s storage) StepUpdateIfState(step *model.Step, from model.StatusValue, workflow *model.Workflow) error {
	sess := s.engine.NewSession()
	defer sess.Close()
	return s.stepUpdateIfState(sess, step, from, workflow)
}

func (s storage) stepUpdateIfState(sess *xorm.Session, step *model.Step, from model.StatusValue, workflow *model.Workflow) error {
	affected, err := sess.ID(step.ID).Where("state = ?", from).
		Where("EXISTS (SELECT 1 FROM workflows WHERE workflows.id = ? AND workflows.pipeline_id = ? AND workflows.pid = ? AND workflows.state = ? AND workflows.agent_id = ? AND workflows.attempts = ?)",
			workflow.ID, step.PipelineID, step.PPID, workflow.State, workflow.AgentID, workflow.Attempts).
		Cols("state", "finished", "error").Update(step)
	if err != nil {
		return err
	}
	if affected == 0 {
		return types.ErrUpdateConflict
	}
	return nil
}

func deleteStep(sess *xorm.Session, stepID int64) error {
	if err := logDelete(sess, stepID); err != nil {
		return err
	}
	return wrapDelete(sess.ID(stepID).Delete(new(model.Step)))
}
