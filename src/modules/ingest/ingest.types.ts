export interface EventBody {
  type: 'error' | 'log' | 'metric' | 'custom';
  severity: 'debug' | 'info' | 'warn' | 'error' | 'fatal';
  message: string;
  stackTrace?: Array<{ file?: string; line?: number; column?: number; function?: string }>;
  tags?: Record<string, string>;
  userContext?: { userId?: string; email?: string; ip?: string };
  deviceContext?: { os?: string; browser?: string; version?: string };
  payload?: Record<string, unknown>;
  occurredAt?: string;
  fingerprint?: string;
}

export interface IngestResult {
  eventId: string;
  traceId: string;
}

export interface BatchIngestResult {
  accepted: number;
  eventIds: string[];
}
