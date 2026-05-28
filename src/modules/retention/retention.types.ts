// ---------------------------------------------------------------------------
// Retention module types
// ---------------------------------------------------------------------------

export interface RetentionConfig {
  projectId: string;
  retentionDays: number;
}

export interface RetentionResult {
  projectsProcessed: number;
  documentsDeleted: number;
  errors: number;
  durationMs: number;
}
