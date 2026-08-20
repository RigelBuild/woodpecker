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
// forbids -- precisely the SEA-1090 null/absent-children shape the scan must tolerate.
function run(workflows: LooseWorkflow[]): number | null {
  return defaultSelectedStepPid(workflows as unknown as PipelineWorkflow[]);
}

describe('defaultSelectedStepPid', () => {
  it('returns null when every workflow is skipped, even if the first has a child step (the user bug)', () => {
    // The exact reported shape: workflow[0] is skipped but owns a step. The old
    // blind `workflows[0].children[0].pid` auto-selected pid 10; the guard must not.
    const workflows = [makeWorkflow(1, [makeStep(10)], 'skipped'), makeWorkflow(2, [makeStep(20)], 'skipped')];

    expect(run(workflows)).toBeNull();
  });

  it('returns null when every workflow is skipped and none has children (null/absent, no throw)', () => {
    const nullChildren = makeWorkflow(1, null, 'skipped');
    // Built without a `children` key at all -> `undefined`, mirroring the omitempty JSON.
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

  it('skips a leading skipped workflow and returns the later success workflow first step pid', () => {
    const workflows = [
      makeWorkflow(1, [makeStep(10)], 'skipped'),
      makeWorkflow(2, [makeStep(20), makeStep(21)], 'success'),
    ];

    expect(run(workflows)).toBe(20);
  });

  it('returns the first step pid of the first workflow when it is non-skipped with steps (happy path)', () => {
    const workflows = [
      makeWorkflow(1, [makeStep(10), makeStep(11)], 'success'),
      makeWorkflow(2, [makeStep(20)], 'success'),
    ];

    expect(run(workflows)).toBe(10);
  });

  it('skips a stepless non-skipped workflow and returns the next non-skipped workflow first step pid', () => {
    const workflows = [makeWorkflow(1, [], 'success'), makeWorkflow(2, [makeStep(20)], 'failure')];

    expect(run(workflows)).toBe(20);
  });

  it('returns null for an empty workflows array', () => {
    expect(run([])).toBeNull();
  });

  it('returns null for undefined workflows', () => {
    expect(defaultSelectedStepPid(undefined)).toBeNull();
  });

  it('passes over a skipped-with-children workflow to return the success workflow step pid', () => {
    const workflows = [makeWorkflow(1, [makeStep(10)], 'skipped'), makeWorkflow(2, [makeStep(20)], 'success')];

    expect(run(workflows)).toBe(20);
  });
});
