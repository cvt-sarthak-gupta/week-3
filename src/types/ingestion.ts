import type { EventIngest } from '../schemas/event.js';

export interface IngestPayload {
  eventId: string;  // uuidv7, generated at HTTP edge
  traceId: string;  // generated at HTTP edge
  projectId: string;
  tenantId: string;
  planId: string;
  raw: EventIngest;
}

export interface PipelineMetricEntry {
  stage: string;
  eventId: string;
  traceId: string;
  durationMs: number;
  success: boolean;
  error?: string;
}
