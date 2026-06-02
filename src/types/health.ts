export interface DatastoreChecks {
  postgres: boolean;
  mongo: boolean;
  elasticsearch: boolean;
  redis: boolean;
}

export interface HealthResponse {
  ok: boolean;
  checks: DatastoreChecks;
  timestamp: string;
}

export interface ReadyCheck {
  ok: boolean;
  latencyMs: number;
}
