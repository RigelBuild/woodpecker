import type { PipelineStatus, PipelineWorkflow } from '~/lib/api/types';

// Lower rank sorts first: attention-first, terminal states last.
const statusDisplayRank: Record<PipelineStatus, number> = {
  error: 0,
  failure: 0,
  running: 1,
  started: 1,
  pending: 2,
  blocked: 2,
  success: 3,
  skipped: 4,
  canceled: 4,
  killed: 4,
  declined: 4,
};

export interface OrderWorkflowsOptions {
  showSkipped: boolean;
  selectedStepId?: number | null;
}

export function orderWorkflows(
  workflows: PipelineWorkflow[],
  { showSkipped, selectedStepId }: OrderWorkflowsOptions,
): PipelineWorkflow[] {
  const visible = workflows.filter(
    (workflow) =>
      workflow.state !== 'skipped' ||
      showSkipped ||
      // A stepless workflow omits `children` (omitempty), so guard the deref.
      (selectedStepId != null && (workflow.children ?? []).some((step) => step.pid === selectedStepId)),
  );

  return visible.sort((a, b) => statusDisplayRank[a.state] - statusDisplayRank[b.state]);
}
