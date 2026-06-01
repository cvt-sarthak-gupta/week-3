export interface AlertRule {
  name: string;
  conditionType: string;
  threshold: number | undefined;
  windowSeconds: number;
  notificationChannel: string;
  isEnabled: boolean;
  esQuery: Record<string, unknown>;
}

export interface CreateAlertRuleInput {
  name: string;
  conditionType: string;
  threshold?: number;
  windowSeconds: number;
  notificationChannel: string;
  isEnabled?: boolean;
  esQuery: Record<string, unknown>;
}

export interface UpdateAlertRuleInput {
  name?: string;
  conditionType?: string;
  threshold?: number;
  windowSeconds?: number;
  notificationChannel?: string;
  isEnabled?: boolean;
  esQuery?: Record<string, unknown>;
}

export interface AlertRuleRow {
  id: string;
  project_id: string;
  tenant_id: string;
  name: string;
  condition_type: string;
  threshold: number | null;
  window_seconds: number;
  notification_channel: string;
  is_enabled: boolean;
  es_query: string;
  created_at: Date;
  updated_at: Date;
  last_triggered_at: Date | null;
}

export interface AlertFireResult {
  fired: boolean;
}
