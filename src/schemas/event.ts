import { z } from 'zod';
import { createHash } from 'node:crypto';

// ---------------------------------------------------------------------------
// Sub-schemas
// ---------------------------------------------------------------------------

export const StackFrameSchema = z.object({
  filename: z.string().optional(),
  function: z.string().optional(),
  line: z.number().int().optional(),
  column: z.number().int().optional(),
  context: z.string().optional(),
});

// ---------------------------------------------------------------------------
// Main ingest schema
// ---------------------------------------------------------------------------

export const EventIngestSchema = z.object({
  type: z.enum(['error', 'log', 'metric', 'custom']),
  severity: z.enum(['debug', 'info', 'warn', 'error', 'fatal']),
  message: z.string().min(1).max(10_000),
  stackTrace: z.array(StackFrameSchema).optional(),
  tags: z.record(z.string(), z.string()).optional(),
  userContext: z
    .object({
      userId: z.string().optional(),
      email: z.string().email().optional(),
      ip: z.string().optional(),
    })
    .optional(),
  deviceContext: z
    .object({
      os: z.string().optional(),
      browser: z.string().optional(),
      version: z.string().optional(),
    })
    .optional(),
  payload: z.record(z.string(), z.unknown()).optional(),
  occurredAt: z.string().datetime().optional(), // ISO 8601, defaults to now if omitted
  fingerprint: z.string().optional(), // auto-generated if omitted
});

export type EventIngest = z.infer<typeof EventIngestSchema>;
export type StackFrame = z.infer<typeof StackFrameSchema>;

// ---------------------------------------------------------------------------
// Fingerprint helper
// ---------------------------------------------------------------------------

/**
 * Generates a deterministic fingerprint from type + message (first 100 chars).
 * Uses sha256(type + ':' + message.slice(0, 100)).slice(0, 16).
 */
export function generateFingerprint(type: string, message: string): string {
  const input = `${type}:${message.slice(0, 100)}`;
  return createHash('sha256').update(input).digest('hex').slice(0, 16);
}
