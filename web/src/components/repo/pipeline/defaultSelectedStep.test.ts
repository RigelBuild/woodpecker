import { describe, expect, it } from 'vitest';

import { defaultSelectedStepPid } from '~/components/repo/pipeline/defaultSelectedStep';
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

// The backend omits `children` (`json:"children,omitempty"`) or sends null for a
// skipped/stepless workflow, shapes the declared `PipelineStep[]` type forbids.
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

function run(workflows: LooseWorkflow[]): number | null {
  return defaultSelectedStepPid(workflows as unknown as PipelineWorkflow[]);
}

describe('defaultSelectedStepPid', () => {
  it('returns null when every workflow is skipped, even if the first owns a step', () => {
    const workflows = [makeWorkflow(1, [makeStep(10)], 'skipped'), makeWorkflow(2, [makeStep(20)], 'skipped')];

    expect(run(workflows)).toBeNull();
  });

  it('returns null when every workflow is skipped with null or absent children, without throwing', () => {
    const nullChildren = makeWorkflow(1, null, 'skipped');
    const absentChildren: LooseWorkflow = {
      id: 2,
      pipeline_id: 1,
      pid: 2,
      name: 'workflow-2',
      state: 'skipped',
      started: 1,
      finished: 2,
    };

    expect(() => run([nullChildren, absentChildren])).not.toThrow();
    expect(run([nullChildren, absentChildren])).toBeNull();
  });

  it('skips a leading skipped workflow and returns the next non-skipped first step pid', () => {
    const workflows = [
      makeWorkflow(1, [makeStep(10)], 'skipped'),
      makeWorkflow(2, [makeStep(20), makeStep(21)], 'success'),
    ];

    expect(run(workflows)).toBe(20);
  });

  it('returns the first step pid of a leading non-skipped workflow', () => {
    const workflows = [
      makeWorkflow(1, [makeStep(10), makeStep(11)], 'success'),
      makeWorkflow(2, [makeStep(20)], 'success'),
    ];

    expect(run(workflows)).toBe(10);
  });

  it('skips a stepless non-skipped workflow and returns the next workflow first step pid', () => {
    const workflows = [makeWorkflow(1, [], 'success'), makeWorkflow(2, [makeStep(20)], 'failure')];

    expect(run(workflows)).toBe(20);
  });

  it('returns null for an empty workflows array', () => {
    expect(run([])).toBeNull();
  });

  it('returns null for undefined workflows', () => {
    expect(defaultSelectedStepPid(undefined)).toBeNull();
  });
});
