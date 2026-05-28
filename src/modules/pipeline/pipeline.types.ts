export interface IngestPayload {
  eventId: string;
  traceId: string;
  projectId: string;
  tenantId: string;
  planId: string;
  raw: {
    type: 'error' | 'log' | 'metric' | 'custom';
    severity: 'debug' | 'info' | 'warn' | 'error' | 'fatal';
    message: string;
    stackTrace?: Array<{
      filename?: string;
      function?: string;
      line?: number;
      column?: number;
      context?: string;
    }>;
    tags?: Record<string, string>;
    userContext?: { userId?: string; email?: string; ip?: string };
    deviceContext?: { os?: string; browser?: string; version?: string };
    payload?: Record<string, unknown>;
    occurredAt?: string;
    fingerprint?: string;
  };
}
