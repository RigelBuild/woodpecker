<template>
  <div v-if="hasCancelInfo(pipeline)" class="flex shrink-0 items-center gap-2">
    <Icon name="status-killed" />
    <span class="truncate">
      <router-link
        v-if="pipeline.cancel_info.superseded_by"
        :to="{ name: 'repo-pipeline', params: { pipelineId: pipeline.cancel_info.superseded_by } }"
        class="hover:underline"
      >
        {{ $t('repo.pipeline.cancel_info.superseded_by', { pipelineId: pipeline.cancel_info.superseded_by }) }}
      </router-link>
      <template v-else-if="pipeline.cancel_info.canceled_by_user">
        {{ $t('repo.pipeline.cancel_info.canceled_by_user', { user: pipeline.cancel_info.canceled_by_user }) }}
      </template>
      <template v-else-if="pipeline.cancel_info.canceled_by_step">
        {{ $t('repo.pipeline.cancel_info.canceled_by_step', { step: pipeline.cancel_info.canceled_by_step }) }}
      </template>
    </span>
  </div>
</template>

<script lang="ts" setup>
import Icon from '~/components/atomic/Icon.vue';
import type { Pipeline } from '~/lib/api/types';
import { hasCancelInfo } from '~/lib/pipeline';

defineProps<{
  pipeline: Pipeline;
}>();
</script>
