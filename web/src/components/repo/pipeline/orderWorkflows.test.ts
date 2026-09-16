import { describe, expect, it } from 'vitest';

import { orderWorkflows } from '~/components/repo/pipeline/orderWorkflows';
import type { OrderWorkflowsOptions } from '~/components/repo/pipeline/orderWorkflows';
import type { PipelineStep, PipelineWorkflow } from '~/lib/api/types';

function makeStep(pid: number): PipelineStep {
  return {
    id: pid,
    uuid: `uuid-${pid}`,
    pipeline_id: 1,
    pid,
    ppid: 1,
    name: `step-${pid}`,
    state: 'success',
    exit_code: 0,
  };
}

// The backend omits `children` (omitempty) or sends null, shapes the declared type forbids.
interface LooseWorkflow extends Omit<PipelineWorkflow, 'children'> {
  children?: PipelineStep[] | null;
}

function makeWorkflow(id: number, children: PipelineStep[] | null, state: PipelineWorkflow['state']): LooseWorkflow {
  return {
    id,
    pipeline_id: 1,
    pid: id,
    name: `workflow-${id}`,
    state,
    started: 1,
    finished: 2,
    children,
  };
}

function run(workflows: LooseWorkflow[], opts: OrderWorkflowsOptions): PipelineWorkflow[] {
  return orderWorkflows(workflows as unknown as PipelineWorkflow[], opts);
}

const states = (result: PipelineWorkflow[]) => result.map((workflow) => workflow.state);
const ids = (result: PipelineWorkflow[]) => result.map((workflow) => workflow.id);

describe('orderWorkflows', () => {
  it('orders failed before running before pending before success before skipped', () => {
    const workflows = [
      makeWorkflow(1, [], 'success'),
      makeWorkflow(2, [], 'skipped'),
      makeWorkflow(3, [], 'running'),
      makeWorkflow(4, [], 'failure'),
      makeWorkflow(5, [], 'pending'),
    ];

    const result = run(workflows, { showSkipped: true });

    expect(states(result)).toEqual(['failure', 'running', 'pending', 'success', 'skipped']);
  });

  it('ranks error and failure together, both ahead of running', () => {
    const workflows = [makeWorkflow(1, [], 'error'), makeWorkflow(2, [], 'running'), makeWorkflow(3, [], 'failure')];

    const result = run(workflows, { showSkipped: false });

    expect(states(result)).toEqual(['error', 'failure', 'running']);
  });

  it('ranks running and started together, both ahead of pending and blocked', () => {
    const workflows = [
      makeWorkflow(1, [], 'pending'),
      makeWorkflow(2, [], 'running'),
      makeWorkflow(3, [], 'blocked'),
      makeWorkflow(4, [], 'started'),
    ];

    const result = run(workflows, { showSkipped: false });

    expect(states(result)).toEqual(['running', 'started', 'pending', 'blocked']);
  });

  it('sinks canceled, killed and declined below success without filtering them', () => {
    const workflows = [
      makeWorkflow(1, [], 'canceled'),
      makeWorkflow(2, [], 'success'),
      makeWorkflow(3, [], 'killed'),
      makeWorkflow(4, [], 'failure'),
      makeWorkflow(5, [], 'declined'),
    ];

    const result = run(workflows, { showSkipped: false });

    expect(states(result)).toEqual(['failure', 'success', 'canceled', 'killed', 'declined']);
  });

  it('keeps equal-priority workflows in their input order (stable sort)', () => {
    const workflows = [makeWorkflow(1, [], 'failure'), makeWorkflow(2, [], 'success'), makeWorkflow(3, [], 'failure')];

    const result = run(workflows, { showSkipped: false });

    expect(ids(result)).toEqual([1, 3, 2]);
  });

  it('filters skipped workflows out when showSkipped is false', () => {
    const workflows = [makeWorkflow(1, [], 'success'), makeWorkflow(2, [], 'skipped'), makeWorkflow(3, [], 'failure')];

    const result = run(workflows, { showSkipped: false });

    expect(states(result)).toEqual(['failure', 'success']);
  });

  it('keeps the skipped workflow owning selectedStepId while hiding other skipped ones', () => {
    const owner = makeWorkflow(10, [makeStep(700)], 'skipped');
    const otherSkipped = makeWorkflow(11, [makeStep(701)], 'skipped');
    const failed = makeWorkflow(12, [], 'failure');

    const result = run([owner, otherSkipped, failed], { showSkipped: false, selectedStepId: 700 });

    expect(ids(result)).toEqual([12, 10]);
  });

  it('tolerates null and absent children while scanning for selectedStepId', () => {
    const nullChildren = makeWorkflow(2, null, 'skipped');
    const absentChildren: LooseWorkflow = {
      id: 3,
      pipeline_id: 1,
      pid: 3,
      name: 'workflow-3',
      state: 'skipped',
      started: 1,
      finished: 2,
    };
    const owner = makeWorkflow(4, [makeStep(500)], 'skipped');
    const failed = makeWorkflow(1, [], 'failure');
    const workflows = [nullChildren, absentChildren, owner, failed];

    expect(() => run(workflows, { showSkipped: false, selectedStepId: 500 })).not.toThrow();
    expect(ids(run(workflows, { showSkipped: false, selectedStepId: 500 }))).toEqual([1, 4]);
  });
});
