import { z } from 'zod';

export const AlertRuleSchema = z.object({
  name: z.string().min(1).max(200),
  conditionType: z.enum(['threshold', 'anomaly', 'keyword']),
  threshold: z.number().optional(),
  windowSeconds: z.number().int().min(60).max(86400).default(300),
  notificationChannel: z.string().url(),
  isEnabled: z.boolean().default(true),
  // The ES query that will be stored as a percolator document.
  // For keyword: { match: { message: keyword } }
  // For threshold: { bool: { filter: [{ term: { severity: 'error' } }] } }
  esQuery: z.record(z.string(), z.unknown()).default({}),
});

export type AlertRule = z.infer<typeof AlertRuleSchema>;
