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

package datastore

import (
	"fmt"

	"xorm.io/xorm"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

func (s storage) WorkflowGetTree(pipeline *model.Pipeline) ([]*model.Workflow, error) {
	sess := s.engine.NewSession()
	wfList, err := s.workflowList(sess, pipeline)
	if err != nil {
		return nil, err
	}

	// one query for the whole pipeline; ordered by pid, as stepListWorkflow is
	steps, err := s.stepListPipeline(sess, pipeline.ID)
	if err != nil {
		return nil, err
	}
	children := make(map[int][]*model.Step, len(wfList))
	for _, step := range steps {
		children[step.PPID] = append(children[step.PPID], step)
	}
	for _, wf := range wfList {
		wf.Children = children[wf.PID]
		if wf.Children == nil {
			wf.Children = make([]*model.Step, 0)
		}
	}

	return wfList, sess.Commit()
}

func (s storage) WorkflowsCreate(workflows []*model.Workflow) error {
	sess := s.engine.NewSession()
	defer sess.Close()
	if err := sess.Begin(); err != nil {
		return err
	}

	if err := s.workflowsCreate(sess, workflows); err != nil {
		return err
	}

	return sess.Commit()
}

// workflowsCreate inserts all workflows, then all their steps, in multi-row
// statements and back-fills the IDs the per-row inserts used to set.
func (s storage) workflowsCreate(sess *xorm.Session, workflows []*model.Workflow) error {
	size, err := s.batchSize(new(model.Workflow))
	if err != nil {
		return err
	}
	if err := insertWithIDs(sess, "workflows", workflows, size,
		func(w *model.Workflow) *int64 { return &w.ID },
		func(w *model.Workflow) (int64, int) { return w.PipelineID, w.PID },
	); err != nil {
		return err
	}

	var steps []*model.Step
	for _, wf := range workflows {
		steps = append(steps, wf.Children...)
	}
	return s.stepCreate(sess, steps)
}

// insertWithIDs inserts rows in chunks and back-fills each auto-increment ID
// with one (pipeline_id, pid) lookup per pipeline, since slice inserts leave it 0.
func insertWithIDs[T any](sess *xorm.Session, table string, rows []*T, size int, id func(*T) *int64, key func(*T) (int64, int)) error {
	var batch []*T
	for _, row := range rows {
		if *id(row) != 0 {
			// a preset ID would change the column list of a multi-row insert
			if err := wrapInsert(sess.Insert(row)); err != nil {
				return err
			}
			continue
		}
		batch = append(batch, row)
	}
	if len(batch) == 0 {
		return nil
	}
	if err := insertBatched(sess, batch, size); err != nil {
		return err
	}

	idsByPipeline := make(map[int64]map[int]int64)
	for _, row := range batch {
		pipelineID, pid := key(row)
		ids, ok := idsByPipeline[pipelineID]
		if !ok {
			var err error
			if ids, err = idsByPID(sess, table, pipelineID); err != nil {
				return err
			}
			idsByPipeline[pipelineID] = ids
		}
		rowID, ok := ids[pid]
		if !ok {
			return fmt.Errorf("%s: inserted row pipeline %d pid %d not found", table, pipelineID, pid)
		}
		*id(row) = rowID
	}
	return nil
}

// WorkflowsReplace performs an atomic replacement of workflows and associated steps by deleting all existing workflows and steps and inserting the new ones.
func (s storage) WorkflowsReplace(pipeline *model.Pipeline, workflows []*model.Workflow) error {
	sess := s.engine.NewSession()
	defer sess.Close()
	if err := sess.Begin(); err != nil {
		return err
	}

	if err := s.workflowsDelete(sess, pipeline.ID); err != nil {
		return err
	}

	if err := s.workflowsCreate(sess, workflows); err != nil {
		return err
	}

	return sess.Commit()
}

func (s storage) workflowsDelete(sess *xorm.Session, pipelineID int64) error {
	// delete related steps
	for {
		stepIDs := make([]int64, 0, perPage)
		if err := sess.Limit(perPage).Table("steps").Cols("id").Where("pipeline_id = ?", pipelineID).Find(&stepIDs); err != nil {
			return err
		}
		if len(stepIDs) == 0 {
			break
		}

		for i := range stepIDs {
			if err := deleteStep(sess, stepIDs[i]); err != nil {
				return err
			}
		}
	}

	_, err := sess.Where("pipeline_id = ?", pipelineID).Delete(new(model.Workflow))
	return err
}

func (s storage) WorkflowList(pipeline *model.Pipeline) ([]*model.Workflow, error) {
	return s.workflowList(s.engine.NewSession(), pipeline)
}

// workflowList lists workflows without child steps.
func (s storage) workflowList(sess *xorm.Session, pipeline *model.Pipeline) ([]*model.Workflow, error) {
	var wfList []*model.Workflow
	err := sess.Where("pipeline_id = ?", pipeline.ID).
		OrderBy("pid").
		Find(&wfList)
	if err != nil {
		return nil, err
	}

	return wfList, nil
}

func (s storage) WorkflowLoad(id int64) (*model.Workflow, error) {
	workflow := new(model.Workflow)
	return workflow, wrapGet(s.engine.ID(id).Get(workflow))
}

// WorkflowByStep returns the workflow a step belongs to. A step's parent
// positional id (PPID) equals the workflow's PID within the same pipeline.
func (s storage) WorkflowByStep(step *model.Step) (*model.Workflow, error) {
	workflow := new(model.Workflow)
	return workflow, wrapGet(s.engine.
		Where("pipeline_id = ?", step.PipelineID).
		Where("pid = ?", step.PPID).
		Get(workflow))
}

func (s storage) WorkflowUpdate(workflow *model.Workflow) error {
	_, err := s.engine.ID(workflow.ID).AllCols().Update(workflow)
	return err
}

// WorkflowResetForRequeue compares and sets the workflow row in one transaction,
// so a concurrent finish or re-dispatch of the same row wins over the reset.
func (s storage) WorkflowResetForRequeue(workflow *model.Workflow, steps []*model.Step, agentID int64) (bool, error) {
	sess := s.engine.NewSession()
	defer sess.Close()
	if err := sess.Begin(); err != nil {
		return false, err
	}

	row := new(model.Workflow)
	if err := wrapGet(sess.ID(workflow.ID).Get(row)); err != nil {
		return false, err
	}

	switch {
	case row.State == model.StatusRunning && row.AgentID == agentID:
		reset := &model.Workflow{State: model.StatusPending, Attempts: row.Attempts + 1}
		n, err := sess.ID(row.ID).
			Where("state = ? AND agent_id = ? AND attempts = ?", row.State, agentID, row.Attempts).
			Cols("state", "started", "finished", "error", "agent_id", "attempts").
			Update(reset)
		if err != nil || n != 1 {
			return false, err
		}
		for _, step := range steps {
			if _, err := sess.ID(step.ID).
				Cols("state", "started", "finished", "exit_code", "error").
				Update(&model.Step{State: model.StatusPending}); err != nil {
				return false, err
			}
		}
		if err := sess.Commit(); err != nil {
			return false, err
		}
		workflow.State, workflow.Started, workflow.Finished, workflow.Error = model.StatusPending, 0, 0, ""
		workflow.AgentID, workflow.Attempts = 0, reset.Attempts
		for _, step := range steps {
			step.State, step.Started, step.Finished, step.ExitCode, step.Error = model.StatusPending, 0, 0, 0, ""
		}
		return true, nil

	case row.State == model.StatusPending && (row.AgentID == 0 || row.AgentID == agentID):
		// MySQL counts changed rows, not matched ones, so an agent already 0 needs no write.
		if row.AgentID != 0 {
			n, err := sess.ID(row.ID).
				Where("state = ? AND agent_id = ?", row.State, row.AgentID).
				Cols("agent_id").
				Update(&model.Workflow{})
			if err != nil || n != 1 {
				return false, err
			}
		}
		if err := sess.Commit(); err != nil {
			return false, err
		}
		workflow.AgentID = 0
		return true, nil

	default:
		return false, nil
	}
}
