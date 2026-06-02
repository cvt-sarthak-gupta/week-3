/** Full project context returned by ProjectService (includes plan and retention data). */
export interface ProjectContext {
  id: string;
  tenantId: string;
  planId: string;
  apiKey: string;
  retentionDays: number;
}
