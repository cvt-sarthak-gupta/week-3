import type { AlertRule } from '../schemas/alert.js';

export type { AlertRule };

export interface CreateAlertRuleInput {
  projectId: string;
  tenantId: string;
  rule: AlertRule;
}

export interface UpdateAlertRuleInput {
  projectId: string;
  updates: Partial<AlertRule>;
}
