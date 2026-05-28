export interface StackFrame {
  file?: string;
  line?: number;
  column?: number;
  function?: string;
  source?: string;
}

export interface EventDocument {
  _id: string; // eventId (uuidv7)
  projectId: string;
  type: 'error' | 'log' | 'metric' | 'custom';
  severity: 'debug' | 'info' | 'warn' | 'error' | 'fatal';
  message: string;
  stackTrace?: StackFrame[];
  tags?: Record<string, string>;
  userContext?: { userId?: string; email?: string; ip?: string };
  deviceContext?: { os?: string; browser?: string; version?: string };
  payload?: Record<string, unknown>;
  occurredAt: Date;
  ingestedAt: Date;
  fingerprint: string;
  traceId?: string;
}

export interface LogDocument {
  _id: string;
  projectId: string;
  level: string;
  message: string;
  timestamp: Date;
  meta?: Record<string, unknown>;
}

export interface DashboardDocument {
  _id: string;
  tenantId: string;
  name: string;
  layout: unknown[];
  createdAt: Date;
  updatedAt: Date;
}

export interface ProjectConfigDocument {
  _id: string; // projectId
  tenantId: string;
  name: string;
  retentionDays: number;
  alertsEnabled: boolean;
  createdAt: Date;
  updatedAt: Date;
  settings?: Record<string, unknown>;
}

export interface PipelineMetricsDocument {
  _id: string;
  projectId: string;
  pipelineId: string;
  stage: string;
  durationMs: number;
  status: 'success' | 'failure' | 'skipped';
  recordedAt: Date;
  meta?: Record<string, unknown>;
}
