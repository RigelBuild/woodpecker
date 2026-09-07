import type { PipelineStep, PipelineWorkflow } from '~/lib/api/types';

// findStep locates the step with the given pid across all workflows of a
// pipeline. A skipped/stepless workflow serializes `children` as absent
// (`omitempty`) or null, so the deref is guarded: an unguarded
// `workflow.children.reduce` throws "Cannot read properties of undefined
// (reading 'reduce')" and blanks the log view.
export function findStep(workflows: PipelineWorkflow[], pid: number): PipelineStep | undefined {
  return workflows.reduce(
    (prev, workflow) => {
      const result = (workflow.children ?? []).reduce(
        (prevChild, step) => {
          if (step.pid === pid) {
            return step;
          }

          return prevChild;
        },
        undefined as PipelineStep | undefined,
      );
      if (result) {
        return result;
      }

      return prev;
    },
    undefined as PipelineStep | undefined,
  );
}
