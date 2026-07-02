import { describe, expect, it } from 'vitest';

import { findStep } from '~/components/repo/pipeline/findStep';
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
// (`json:"children,omitempty"`) -- the exact mismatch SEA-1075 crashed on. Modeling
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
// forbids -- precisely the SEA-1075 bug under test. `findStep` must tolerate them.
function run(workflows: LooseWorkflow[], pid: number): PipelineStep | undefined {
  return findStep(workflows as unknown as PipelineWorkflow[], pid);
}

describe('findStep', () => {
  it('finds a step across a skipped (null children) workflow without throwing', () => {
    const workflows = [makeWorkflow(1, null, 'skipped'), makeWorkflow(2, [makeStep(77)], 'success')];

    expect(() => run(workflows, 77)).not.toThrow();

    const step = run(workflows, 77);
    expect(step?.pid).toBe(77);
  });

  it('tolerates a workflow with an absent children key (omitempty shape)', () => {
    // Built without a `children` key at all -> `undefined`, mirroring the omitempty JSON.
    const stepless: LooseWorkflow = {
      id: 1,
      pipeline_id: 1,
      pid: 1,
      name: 'workflow-1',
      state: 'skipped',
      started: 1,
      finished: 2,
    };
    const workflows = [stepless, makeWorkflow(2, [makeStep(88)], 'success')];

    expect(() => run(workflows, 88)).not.toThrow();

    const step = run(workflows, 88);
    expect(step?.pid).toBe(88);
  });

  it('finds the step when the skipped workflow appears after the target (order independent)', () => {
    const workflows = [makeWorkflow(1, [makeStep(42)], 'success'), makeWorkflow(2, null, 'skipped')];

    expect(() => run(workflows, 42)).not.toThrow();

    const step = run(workflows, 42);
    expect(step?.pid).toBe(42);
  });

  it('returns undefined when the pid is absent across a skipped and a real workflow', () => {
    const workflows = [makeWorkflow(1, null, 'skipped'), makeWorkflow(2, [makeStep(5)], 'success')];

    expect(() => run(workflows, 999)).not.toThrow();
    expect(run(workflows, 999)).toBeUndefined();
  });

  it('returns undefined when every workflow is skipped (all null children)', () => {
    const workflows = [makeWorkflow(1, null, 'skipped'), makeWorkflow(2, null, 'skipped')];

    expect(() => run(workflows, 1)).not.toThrow();
    expect(run(workflows, 1)).toBeUndefined();
  });

  it('finds the step in the correct workflow when several workflows have children', () => {
    const workflows = [
      makeWorkflow(1, [makeStep(10), makeStep(11)], 'success'),
      makeWorkflow(2, [makeStep(20), makeStep(21)], 'success'),
      makeWorkflow(3, [makeStep(30)], 'success'),
    ];

    const step = run(workflows, 21);
    expect(step?.pid).toBe(21);
    expect(step?.name).toBe('step-21');
  });
});
