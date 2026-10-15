import type { WebhookEvents } from './webhook';

export interface Secret {
  id: string;
  repo_id: number;
  org_id: number;
  name: string;
  value: string;
  events: WebhookEvents[];
  images: string[];
  require_approval: boolean;
  branches: string[];
  note: string;
}
