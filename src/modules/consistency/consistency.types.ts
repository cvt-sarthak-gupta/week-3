// ---------------------------------------------------------------------------
// Consistency module types
// ---------------------------------------------------------------------------

export interface AuditMismatch {
  kind: 'missing_mongo_config' | 'missing_es_index' | 'count_drift' | 'orphan_mongo_config';
  projectId: string;
  details: string;
  suggestion: string;
}

export interface AuditResult {
  ranAt: Date;
  duration_ms: number;
  checked: number;
  inconsistencies: AuditMismatch[];
  summary: {
    total: number;
    byKind: Record<string, number>;
  };
}
