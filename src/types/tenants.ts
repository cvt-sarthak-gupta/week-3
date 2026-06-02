export interface OnboardTenantInput {
  tenantName: string;
  tenantSlug: string;
  planId: string;
  userId: string;
  projectName: string;
  projectSlug: string;
}

export interface OnboardTenantResult {
  tenantId: string;
  projectId: string;
  apiKey: string;
}

export interface TenantInfo {
  id: string;
  name: string;
  slug: string;
  plan_id: string | null;
  is_active: boolean;
  created_at: Date;
}

export interface TenantQuotaReport {
  tenantId: string;
  planName: string | null;
  eventQuotaPerMonth: number | null;
  eventsThisMonth: number;
  percentUsed: number;
}

export interface TenantQuotaReportRow {
  tenantId: string;
  tenantName: string;
  planName: string | null;
  eventQuotaPerMonth: number | null;
  eventsThisMonth: number;
  percentUsed: number;
  rank: number;
  exceeded80pct: boolean;
  momGrowthPct: number | null;
}
