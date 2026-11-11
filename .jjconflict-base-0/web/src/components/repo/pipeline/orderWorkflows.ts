import type { PipelineStatus, PipelineWorkflow } from '~/lib/api/types';

// Display priority for the single-pipeline step list: most-attention-first, so
// failed/running workflows are never buried under a fan-out's grey `skipped`
// ones. Lower rank sorts earlier. This mirrors the *vocabulary* of the server's
// status roll-up (server/pipeline/status.go) but encodes a display order, not the
// merge precedence: here errors/failures come first and the terminal
// non-actionable states (skipped/canceled/killed/declined) sink to the bottom.
// Typed `Record<PipelineStatus, number>` so every status is ranked -- a new
// member fails the build here rather than silently sorting to 0.
const statusDisplayRank: Record<PipelineStatus, number> = {
  // needs attention: something is wrong
  error: 0,
  failure: 0,
  // in flight (`started` is the per-step running shape)
  running: 1,
  started: 1,
  // waiting to run
  pending: 2,
  blocked: 2,
  // finished cleanly
  success: 3,
  // terminal, not actionable -- grouped last
  skipped: 4,
  canceled: 4,
  killed: 4,
  declined: 4,
};

export interface OrderWorkflowsOptions {
  // When false, `skipped` workflows are filtered out (except the one owning the
  // selected step -- see selectedStepId).
  showSkipped: boolean;
  // The currently-selected step's pid, if any. The workflow that contains it is
  // always kept, even when it is skipped and showSkipped is false, so the list
  // never hides the thing the user is looking at (mirrors the collapse carve-out
  // in PipelineStepList.vue).
  selectedStepId?: number | null;
}

// orderWorkflows returns the workflows to render, in display order: skipped
// workflows are hidden unless `showSkipped` is set (or a skipped workflow owns
// the selected step), and the survivors are sorted by status priority
// (attention-first). The sort is stable, so two workflows with the same status
// keep their original relative order. The input array is not mutated.
//
// A skipped/stepless workflow serializes `children` as absent (`omitempty`) or
// null, so the selected-step scan guards the deref (`?? []`) -- an unguarded
// access is the SEA-1090 crash.
export function orderWorkflows(
  workflows: PipelineWorkflow[],
  { showSkipped, selectedStepId }: OrderWorkflowsOptions,
): PipelineWorkflow[] {
  const visible = workflows.filter(
    (workflow) =>
      workflow.state !== 'skipped' ||
      showSkipped ||
      // Always keep the workflow the user is currently looking at.
      (selectedStepId != null && (workflow.children ?? []).some((step) => step.pid === selectedStepId)),
  );

  // Array.prototype.sort is stable (ES2019+), so equal-rank workflows keep their
  // pipeline order -- two failures are never reshuffled relative to each other.
  return visible.sort((a, b) => statusDisplayRank[a.state] - statusDisplayRank[b.state]);
}
