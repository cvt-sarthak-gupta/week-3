export interface CreateDashboardInput {
  projectId: string;
  tenantId: string;
  userId: string;
  name: string;
  layout?: unknown[];
}

export interface DashboardResult {
  id: string;
  projectId: string;
  tenantId: string;
  name: string;
  layout: unknown[];
  createdAt: Date;
  updatedAt: Date;
}
