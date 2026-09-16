import type { PipelineWorkflow } from '~/lib/api/types';

// A skipped/stepless workflow serializes `children` as absent (`omitempty`) or
// null, so the deref is guarded (`?? []`).
export function defaultSelectedStepPid(workflows: PipelineWorkflow[] | undefined): number | null {
  for (const workflow of workflows ?? []) {
    if (workflow.state === 'skipped') {
      continue;
    }
    const firstStep = (workflow.children ?? [])[0];
    if (firstStep !== undefined) {
      return firstStep.pid;
    }
  }
  return null;
}
