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

// `children` is typed `PipelineStep[]` (optional), but the backend serializes a
// skipped (stepless) workflow's children as `null` or omits the key entirely
// (`json:"children,omitempty"`) -- the exact mismatch SEA-1090 crashed on. Modeling
// both runtime shapes requires stepping outside the declared type, so we build a
// loose object and cast once at the boundary. `children?` covers the absent-key
// (omitempty) case; `| null` covers the skipped-workflow case.
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

// Single deliberate boundary cast: the fixtures intentionally carry `children: null`
// or omit `children` altogether, runtime shapes the declared `PipelineStep[]` type
// forbids -- precisely the SEA-1090 bug the selected-step scan must tolerate.
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

  it('ranks error and failure as top priority (rank 0), both ahead of running', () => {
    // error appears before failure in the input; equal rank -> input order preserved.
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

  it('filters skipped workflows out when showSkipped is false', () => {
    const workflows = [makeWorkflow(1, [], 'success'), makeWorkflow(2, [], 'skipped'), makeWorkflow(3, [], 'failure')];

    const result = run(workflows, { showSkipped: false });

    expect(states(result)).toEqual(['failure', 'success']);
    expect(states(result)).not.toContain('skipped');
  });

  it('keeps skipped workflows when showSkipped is true, sorted last', () => {
    const workflows = [makeWorkflow(1, [], 'skipped'), makeWorkflow(2, [], 'failure')];

    const result = run(workflows, { showSkipped: true });

    expect(states(result)).toEqual(['failure', 'skipped']);
  });

  it('keeps the skipped workflow owning selectedStepId while hiding other skipped ones', () => {
    const owner = makeWorkflow(10, [makeStep(700)], 'skipped');
    const otherSkipped = makeWorkflow(11, [makeStep(701)], 'skipped');
    const failed = makeWorkflow(12, [], 'failure');

    const result = run([owner, otherSkipped, failed], { showSkipped: false, selectedStepId: 700 });

    // The owning skipped workflow survives (sorted last), the other skipped one is gone.
    expect(ids(result)).toEqual([12, 10]);
    expect(ids(result)).not.toContain(11);
  });

  it('tolerates null and absent children while scanning for selectedStepId (no throw)', () => {
    const nullChildren = makeWorkflow(2, null, 'skipped');
    // Built without a `children` key at all -> `undefined`, mirroring the omitempty JSON.
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

    const result = run(workflows, { showSkipped: false, selectedStepId: 500 });
    // Only the failure and the selected-step owner survive; the stepless skipped ones are hidden.
    expect(ids(result)).toEqual([1, 4]);
  });

  it('sinks canceled, killed and declined below success without filtering them', () => {
    const workflows = [
      makeWorkflow(1, [], 'canceled'),
      makeWorkflow(2, [], 'success'),
      makeWorkflow(3, [], 'killed'),
      makeWorkflow(4, [], 'failure'),
      makeWorkflow(5, [], 'declined'),
    ];

    // Only `skipped` is filtered by showSkipped; the other terminal states always render.
    const result = run(workflows, { showSkipped: false });

    expect(states(result)).toEqual(['failure', 'success', 'canceled', 'killed', 'declined']);
  });

  it('keeps equal-priority workflows in their input order (stable sort)', () => {
    const workflows = [makeWorkflow(1, [], 'failure'), makeWorkflow(2, [], 'success'), makeWorkflow(3, [], 'failure')];

    const result = run(workflows, { showSkipped: false });

    // The two failures keep their relative input order (id 1 before id 3), success last.
    expect(ids(result)).toEqual([1, 3, 2]);
  });

  it('hides all skipped workflows when selectedStepId is null', () => {
    const workflows = [
      makeWorkflow(1, [makeStep(900)], 'skipped'),
      makeWorkflow(2, [], 'skipped'),
      makeWorkflow(3, [], 'failure'),
    ];

    const result = run(workflows, { showSkipped: false, selectedStepId: null });

    expect(ids(result)).toEqual([3]);
  });

  it('hides all skipped workflows when selectedStepId is undefined', () => {
    const workflows = [
      makeWorkflow(1, [makeStep(900)], 'skipped'),
      makeWorkflow(2, [], 'skipped'),
      makeWorkflow(3, [], 'failure'),
    ];

    const result = run(workflows, { showSkipped: false, selectedStepId: undefined });

    expect(ids(result)).toEqual([3]);
  });

  it('returns an empty array for empty input', () => {
    expect(run([], { showSkipped: false })).toEqual([]);
    expect(run([], { showSkipped: true })).toEqual([]);
  });

  it('does not mutate the input array', () => {
    const workflows = [makeWorkflow(1, [], 'success'), makeWorkflow(2, [], 'failure')];
    const originalOrder = [...workflows];

    run(workflows, { showSkipped: true });

    // Same references, same order -- the helper sorts a filtered copy.
    expect(workflows).toEqual(originalOrder);
    expect(workflows[0]).toBe(originalOrder[0]);
  });
});
