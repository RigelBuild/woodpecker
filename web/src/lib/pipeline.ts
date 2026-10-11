import type { Pipeline, PipelineWorkflow } from '~/lib/api/types';

export function workflowsWithErrors(pipeline?: Pipeline): PipelineWorkflow[] {
  return pipeline?.workflows?.filter((workflow) => workflow.error !== undefined && workflow.error !== '') ?? [];
}

export function anyStepStarted(pipeline?: Pipeline): boolean {
  return (
    pipeline?.workflows?.some((workflow) =>
      workflow.children?.some((step) => step.started !== undefined && step.started > 0),
    ) ?? false
  );
}

export function pipelineHasNonWarningErrors(pipeline?: Pipeline): boolean {
  return pipeline?.errors?.some((e) => !e.is_warning) ?? false;
}

export function pipelineHasErrorsToShow(pipeline?: Pipeline): boolean {
  return pipelineHasNonWarningErrors(pipeline) || workflowsWithErrors(pipeline).length > 0;
}

// Cancel context can sit on a failed pipeline too: a failure outranks killed.
export function hasCancelInfo(pipeline?: Pipeline): boolean {
  const info = pipeline?.cancel_info;
  return (
    (info?.superseded_by ?? 0) > 0 || (info?.canceled_by_user ?? '') !== '' || (info?.canceled_by_step ?? '') !== ''
  );
}
