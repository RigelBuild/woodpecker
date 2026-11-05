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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// overChunk is one row past the largest chunk any batched insert uses.
const overChunk = pgBatchSize + 1

func newBatchTestStore(t *testing.T) *storage {
	t.Helper()
	s, ok := NewTestStore(t).(*storage)
	require.True(t, ok)
	return s
}

func TestBatchSizeStaysUnderDriverLimits(t *testing.T) {
	s := newBatchTestStore(t)
	for _, bean := range []any{new(model.Workflow), new(model.Step), new(model.Task), new(model.Config), new(model.PipelineConfig)} {
		size, err := s.batchSize(bean)
		require.NoError(t, err)
		table, err := s.engine.TableInfo(bean)
		require.NoError(t, err)
		assert.LessOrEqual(t, size, pgBatchSize)
		assert.LessOrEqual(t, size*len(table.Columns()), maxBindParams)
		t.Logf("%T: %d columns, %d rows per statement", bean, len(table.Columns()), size)
	}
}

// testWorkflows builds n workflows with two steps each. Step PIDs follow the
// workflow PIDs, as workflowsFromPipelineBuilder numbers them.
func testWorkflows(pipelineID int64, n int) []*model.Workflow {
	workflows := make([]*model.Workflow, 0, n)
	pid := n
	for i := 1; i <= n; i++ {
		wf := &model.Workflow{PipelineID: pipelineID, PID: i, Name: fmt.Sprintf("wf-%d", i), State: model.StatusPending}
		for j := range 2 {
			pid++
			wf.Children = append(wf.Children, &model.Step{
				UUID:       fmt.Sprintf("%d-%d-%d", pipelineID, i, j),
				PipelineID: pipelineID,
				PID:        pid,
				PPID:       i,
				Name:       fmt.Sprintf("step-%d", j),
				State:      model.StatusPending,
				DependsOn:  []string{"x"},
			})
		}
		workflows = append(workflows, wf)
	}
	return workflows
}

func TestWorkflowsCreateBackfillsIDsAcrossChunks(t *testing.T) {
	s := newBatchTestStore(t)
	// a second pipeline with rows on both tables, so the back-fill must not mix pipelines
	require.NoError(t, s.WorkflowsCreate(testWorkflows(2, 3)))

	workflows := testWorkflows(1, overChunk)
	require.NoError(t, s.WorkflowsCreate(workflows))

	stored, err := s.WorkflowGetTree(&model.Pipeline{ID: 1})
	require.NoError(t, err)
	require.Len(t, stored, overChunk)
	for i, wf := range workflows {
		require.NotZero(t, wf.ID)
		assert.Equal(t, stored[i].ID, wf.ID, "workflow pid %d", wf.PID)
		require.Len(t, stored[i].Children, len(wf.Children))
		for j, step := range wf.Children {
			require.NotZero(t, step.ID)
			assert.Equal(t, stored[i].Children[j].ID, step.ID, "step pid %d", step.PID)
			assert.Equal(t, wf.PID, stored[i].Children[j].PPID)
		}
	}
}

func TestWorkflowsReplaceBackfillsIDs(t *testing.T) {
	s := newBatchTestStore(t)
	pipeline := &model.Pipeline{ID: 1}
	require.NoError(t, s.WorkflowsCreate(testWorkflows(1, 3)))

	workflows := testWorkflows(1, 4)
	require.NoError(t, s.WorkflowsReplace(pipeline, workflows))

	stored, err := s.WorkflowGetTree(pipeline)
	require.NoError(t, err)
	require.Len(t, stored, 4)
	for i, wf := range workflows {
		assert.Equal(t, stored[i].ID, wf.ID)
		for j, step := range wf.Children {
			assert.Equal(t, stored[i].Children[j].ID, step.ID)
		}
	}
}

// One call can mix rows with a preset ID and rows from two pipelines sharing PIDs.
func TestInsertWithIDsMixedBatch(t *testing.T) {
	s := newBatchTestStore(t)
	preset := &model.Workflow{ID: 5000, PipelineID: 1, PID: 1, Name: "preset", State: model.StatusPending}
	first := &model.Workflow{PipelineID: 1, PID: 2, Name: "p1-2", State: model.StatusPending}
	second := &model.Workflow{PipelineID: 2, PID: 1, Name: "p2-1", State: model.StatusPending}
	third := &model.Workflow{PipelineID: 2, PID: 2, Name: "p2-2", State: model.StatusPending}
	rows := []*model.Workflow{preset, first, second, third}
	require.NoError(t, s.WorkflowsCreate(rows))

	assert.EqualValues(t, 5000, preset.ID)
	for _, wf := range rows {
		require.NotZero(t, wf.ID)
		stored := new(model.Workflow)
		has, err := s.engine.ID(wf.ID).Get(stored)
		require.NoError(t, err)
		require.True(t, has, "workflow %s", wf.Name)
		assert.Equal(t, wf.Name, stored.Name)
		assert.Equal(t, wf.PipelineID, stored.PipelineID)
		assert.Equal(t, wf.PID, stored.PID)
	}
}

func TestWorkflowGetTreeMatchesPerWorkflowLoad(t *testing.T) {
	s := newBatchTestStore(t)
	pipeline := &model.Pipeline{ID: 1}
	workflows := testWorkflows(1, 3)
	// a workflow without steps and steps inserted out of pid order
	workflows = append(workflows, &model.Workflow{PipelineID: 1, PID: 20, Name: "empty"})
	workflows[0].Children[0], workflows[0].Children[1] = workflows[0].Children[1], workflows[0].Children[0]
	require.NoError(t, s.WorkflowsCreate(workflows))
	require.NoError(t, s.WorkflowsCreate(testWorkflows(2, 2)))

	got, err := s.WorkflowGetTree(pipeline)
	require.NoError(t, err)

	sess := s.engine.NewSession()
	defer sess.Close()
	want, err := s.workflowList(sess, pipeline)
	require.NoError(t, err)
	for _, wf := range want {
		wf.Children, err = s.stepListWorkflow(sess, wf)
		require.NoError(t, err)
	}
	assert.Equal(t, want, got)
	require.Len(t, got, 4)
	assert.Equal(t, []int{4, 5}, []int{got[0].Children[0].PID, got[0].Children[1].PID})
	assert.Empty(t, got[3].Children)
	assert.NotNil(t, got[3].Children)
}

func TestConfigsPersist(t *testing.T) {
	s := newBatchTestStore(t)
	existing, err := s.ConfigPersist(&model.Config{RepoID: 2, Name: "existing", Data: []byte("a")})
	require.NoError(t, err)
	// same data under another name and in another repo must not match
	otherName, err := s.ConfigPersist(&model.Config{RepoID: 2, Name: "renamed", Data: []byte("b")})
	require.NoError(t, err)
	_, err = s.ConfigPersist(&model.Config{RepoID: 3, Name: "new", Data: []byte("b")})
	require.NoError(t, err)

	input := []*model.Config{
		{Name: "new", Data: []byte("b")},
		{Name: "existing", Data: []byte("a")},
		{Name: "new", Data: []byte("b")},
	}
	for i := range overChunk {
		input = append(input, &model.Config{Name: fmt.Sprintf("bulk-%d", i), Data: fmt.Appendf(nil, "data-%d", i)})
	}

	got, err := s.ConfigsPersist(2, input)
	require.NoError(t, err)
	require.Len(t, got, len(input))
	assert.Equal(t, existing.ID, got[1].ID)
	assert.NotEqual(t, otherName.ID, got[0].ID)
	assert.Equal(t, got[0].ID, got[2].ID, "identical configs in one call share a row")
	for i, conf := range got {
		require.NotZero(t, conf.ID)
		assert.Equal(t, input[i].Name, conf.Name, "input order")
		assert.Equal(t, input[i].Data, conf.Data)
		assert.EqualValues(t, 2, conf.RepoID)
	}

	count, err := s.engine.Count(new(model.Config))
	require.NoError(t, err)
	assert.EqualValues(t, 3+1+overChunk, count)

	again, err := s.ConfigsPersist(2, input)
	require.NoError(t, err)
	assert.Equal(t, got, again, "a second call stores nothing new")
}

// A concurrent insert of the same config must resolve to one row on the batch path.
func TestConfigsPersistConcurrentDuplicate(t *testing.T) {
	s := newBatchTestStore(t)
	hook := &concurrentConfigInsertHook{
		store: s,
		conf:  model.Config{RepoID: 2, Data: data, Hash: hash, Name: name},
	}
	s.engine.AddHook(hook)

	got, err := s.ConfigsPersist(2, []*model.Config{
		{Name: "other", Data: []byte("other")},
		{Name: name, Data: data},
	})
	require.True(t, hook.fired)
	require.NoError(t, hook.err)
	require.NoError(t, err)
	require.Len(t, got, 2)

	concurrent, err := s.configFindIdentical(s.engine.NewSession(), 2, hash, name)
	require.NoError(t, err)
	assert.Equal(t, concurrent.ID, got[1].ID)
	assert.Equal(t, "other", got[0].Name)
	assert.NotZero(t, got[0].ID)

	count, err := s.engine.Count(new(model.Config))
	require.NoError(t, err)
	assert.EqualValues(t, 2, count)
}

func TestPipelineConfigsCreate(t *testing.T) {
	s := newBatchTestStore(t)
	links := make([]*model.PipelineConfig, 0, overChunk)
	for i := range overChunk {
		links = append(links, &model.PipelineConfig{PipelineID: 1, ConfigID: int64(i + 1)})
	}
	require.NoError(t, s.PipelineConfigsCreate(links))
	count, err := s.engine.Count(new(model.PipelineConfig))
	require.NoError(t, err)
	assert.EqualValues(t, overChunk, count)
}

func TestTaskInsertBatch(t *testing.T) {
	s := newBatchTestStore(t)
	tasks := make([]*model.Task, 0, overChunk)
	for i := range overChunk {
		tasks = append(tasks, &model.Task{
			ID:           fmt.Sprint(i),
			PID:          i,
			Name:         fmt.Sprintf("task-%d", i),
			Data:         []byte("payload"),
			Labels:       map[string]string{"repo": "r"},
			Dependencies: []string{"dep"},
			RunOn:        []string{"success"},
			DepStatus:    map[string]model.StatusValue{"dep": model.StatusPending},
			PipelineID:   7,
			RepoID:       3,
			Created:      42,
			BlockedCount: i % 5,
		})
	}
	require.NoError(t, s.TaskInsertBatch(tasks))

	stored, err := s.TaskList()
	require.NoError(t, err)
	require.Len(t, stored, overChunk)
	byID := make(map[string]*model.Task, len(stored))
	for _, task := range stored {
		byID[task.ID] = task
	}
	for _, task := range tasks {
		assert.Equal(t, task, byID[task.ID])
	}
}

func TestQueryBudgetWorkflowsCreate(t *testing.T) {
	s := NewTestStore(t)
	counter := CountQueries(t, s)
	pipelineID := int64(0)
	AssertQueryBudget(t, func(n int) int64 {
		pipelineID++
		workflows := testWorkflows(pipelineID, n)
		return counter.Count(func() { require.NoError(t, s.WorkflowsCreate(workflows)) })
	})
}

func TestQueryBudgetGetPipelineTree(t *testing.T) {
	s := NewTestStore(t)
	repo := &model.Repo{UserID: 1, FullName: "owner/repo", Owner: "owner", Name: "repo", ForgeRemoteID: "1"}
	require.NoError(t, s.CreateRepo(repo))
	counter := CountQueries(t, s)
	AssertQueryBudget(t, func(n int) int64 {
		pipeline := &model.Pipeline{RepoID: repo.ID, Status: model.StatusRunning}
		require.NoError(t, s.CreatePipeline(pipeline))
		require.NoError(t, s.WorkflowsCreate(testWorkflows(pipeline.ID, n)))
		return counter.Count(func() {
			pl, err := s.GetPipelineNumber(repo, pipeline.Number)
			require.NoError(t, err)
			tree, err := s.WorkflowGetTree(pl)
			require.NoError(t, err)
			require.Len(t, tree, n)
		})
	})
}

func TestQueryBudgetLogAppend(t *testing.T) {
	s := NewTestStore(t)
	counter := CountQueries(t, s)
	stepID := int64(0)
	AssertQueryBudget(t, func(n int) int64 {
		stepID++
		step := &model.Step{ID: stepID}
		entries := make([]*model.LogEntry, 0, n)
		for i := range n {
			entries = append(entries, &model.LogEntry{StepID: stepID, Line: i, Data: []byte("line")})
		}
		return counter.Count(func() { require.NoError(t, s.LogAppend(step, entries)) })
	})
}

func TestQueryBudgetConfigsPersist(t *testing.T) {
	s := NewTestStore(t)
	counter := CountQueries(t, s)
	run := 0
	AssertQueryBudget(t, func(n int) int64 {
		run++
		confs := make([]*model.Config, 0, n)
		for i := range n {
			confs = append(confs, &model.Config{Name: fmt.Sprintf("c-%d", i), Data: fmt.Appendf(nil, "%d-%d", run, i)})
		}
		return counter.Count(func() {
			stored, err := s.ConfigsPersist(1, confs)
			require.NoError(t, err)
			links := make([]*model.PipelineConfig, 0, n)
			for _, conf := range stored {
				links = append(links, &model.PipelineConfig{PipelineID: int64(run), ConfigID: conf.ID})
			}
			require.NoError(t, s.PipelineConfigsCreate(links))
		})
	})
}

func TestQueryBudgetTaskInsertBatch(t *testing.T) {
	s := NewTestStore(t)
	counter := CountQueries(t, s)
	run := 0
	AssertQueryBudget(t, func(n int) int64 {
		run++
		tasks := make([]*model.Task, 0, n)
		for i := range n {
			tasks = append(tasks, &model.Task{ID: fmt.Sprintf("%d-%d", run, i)})
		}
		return counter.Count(func() { require.NoError(t, s.TaskInsertBatch(tasks)) })
	})
}
