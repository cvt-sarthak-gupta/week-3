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

export interface TenantDetail {
  id: string;
  name: string;
  slug: string;
  planId: string | null;
  planName: string | null;
  isActive: boolean;
  createdAt: Date;
  memberCount: number;
}

export interface TenantQuotaReport {
  tenantId: string;
  planName: string | null;
  eventQuotaPerMonth: number | null;
  eventsThisMonth: number;
  percentUsed: number;
}

export interface PatchTenantInput {
  name?: string;
  isActive?: boolean;
}
