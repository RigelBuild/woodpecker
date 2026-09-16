import type { PipelineStatus, PipelineWorkflow } from '~/lib/api/types';

// Step-list display order: attention-first, terminal states last (lower rank sorts first).
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
  // The workflow owning this step is kept visible even when skipped and hidden.
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
      // A stepless workflow serializes `children` absent (omitempty), so guard the deref.
      (selectedStepId != null && (workflow.children ?? []).some((step) => step.pid === selectedStepId)),
  );

  // Array.prototype.sort is stable, so equal-rank workflows keep their input order.
  return visible.sort((a, b) => statusDisplayRank[a.state] - statusDisplayRank[b.state]);
}
