import { describe, expect, it } from 'vitest';

import type { CancelInfo, Pipeline } from '~/lib/api/types';
import { hasCancelInfo } from '~/lib/pipeline';

function withCancelInfo(status: Pipeline['status'], cancelInfo: Partial<CancelInfo>): Pipeline {
  return { status, cancel_info: cancelInfo } as unknown as Pipeline;
}

describe('hasCancelInfo', () => {
  it('shows a supersede on a failed pipeline', () => {
    expect(hasCancelInfo(withCancelInfo('failure', { superseded_by: 42 }))).toBe(true);
  });

  it('shows a user cancel on a killed pipeline', () => {
    expect(hasCancelInfo(withCancelInfo('killed', { canceled_by_user: 'matt' }))).toBe(true);
  });

  it('shows a step cancel', () => {
    expect(hasCancelInfo(withCancelInfo('failure', { canceled_by_step: 'lint' }))).toBe(true);
  });

  it('hides empty cancel info', () => {
    expect(hasCancelInfo(withCancelInfo('killed', {}))).toBe(false);
    expect(hasCancelInfo(undefined)).toBe(false);
  });
});
