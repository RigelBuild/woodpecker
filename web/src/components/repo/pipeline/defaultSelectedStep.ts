import type { PipelineWorkflow } from '~/lib/api/types';

// defaultSelectedStepPid picks the step to auto-select when a pipeline is opened
// without an explicit step in the URL. It returns the pid of the first step of
// the first NON-skipped workflow that has one, so the auto-selection never lands
// on a skipped workflow that the step list hides by default -- otherwise the
// selected-step carve-out in orderWorkflows would surface that one skipped
// workflow while the rest stay hidden (inconsistent). When every workflow is
// skipped (or none has a step), there is nothing to show, so it returns null and
// no log pane opens.
//
// A skipped/stepless workflow serializes `children` as absent (`omitempty`) or
// null, so the deref is guarded (`?? []`) -- an unguarded access is the SEA-1090
// crash.
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
