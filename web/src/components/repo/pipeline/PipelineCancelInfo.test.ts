import { shallowMount } from '@vue/test-utils';
import { describe, expect, it } from 'vitest';
import { createI18n } from 'vue-i18n';

import PipelineCancelInfo from '~/components/repo/pipeline/PipelineCancelInfo.vue';
import type { CancelInfo, Pipeline } from '~/lib/api/types';

const i18n = createI18n({
  legacy: false,
  locale: 'en',
  missingWarn: false,
  fallbackWarn: false,
  messages: {
    en: {
      repo: {
        pipeline: {
          cancel_info: {
            superseded_by: 'Superseded by #{pipelineId}',
            canceled_by_user: 'Canceled by {user}',
            canceled_by_step: 'Canceled by step {user}',
          },
        },
      },
    },
  },
});

function mountFor(status: Pipeline['status'], cancelInfo: Partial<CancelInfo>) {
  const pipeline = { status, cancel_info: cancelInfo } as unknown as Pipeline;
  return shallowMount(PipelineCancelInfo, {
    props: { pipeline },
    global: { plugins: [i18n], stubs: { 'router-link': { template: '<a><slot /></a>' } } },
  });
}

describe('pipelineCancelInfo', () => {
  it('shows the supersede on a failed pipeline', () => {
    expect(mountFor('failure', { superseded_by: 42 }).text()).toContain('Superseded by #42');
  });

  it('shows a user cancel on a killed pipeline', () => {
    expect(mountFor('killed', { canceled_by_user: 'matt' }).text()).toContain('Canceled by matt');
  });

  it('renders nothing without cancel info', () => {
    expect(mountFor('killed', {}).html()).not.toContain('status-killed');
  });
});
