import type { PipelineStep, PipelineWorkflow } from '~/lib/api/types';

// A stepless workflow has no `children`, so the deref is guarded.
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
