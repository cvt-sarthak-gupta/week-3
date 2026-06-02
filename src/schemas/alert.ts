import { z } from 'zod';

// Blocks SSRF by rejecting non-https/http schemes and private/link-local hosts.
// fe80: = IPv6 link-local (fe80::/10) — missing from the original pattern
// ::ffff: prefixes cover IPv4-mapped IPv6 addresses (::ffff:127.0.0.1, etc.)
const PRIVATE_HOST_RE =
  /^(localhost|127\.|10\.|192\.168\.|169\.254\.|0\.0\.0\.0|::1|fc00:|fd[0-9a-f]{2}:|fe80:|::ffff:127\.|::ffff:10\.|::ffff:192\.168\.)/i;

export function safeWebhookUrl(raw: string): boolean {
  let parsed: URL;
  try {
    parsed = new URL(raw);
  } catch {
    return false;
  }
  if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') return false;
  if (PRIVATE_HOST_RE.test(parsed.hostname)) return false;
  return true;
}

export const AlertRuleSchema = z.object({
  name: z.string().min(1).max(200),
  conditionType: z.enum(['threshold', 'anomaly', 'keyword']),
  threshold: z.number().optional(),
  windowSeconds: z.number().int().min(60).max(86400).default(300),
  notificationChannel: z.string().url().refine(safeWebhookUrl, {
    message: 'notificationChannel must be a public http/https URL (private/internal addresses are not allowed)',
  }),
  isEnabled: z.boolean().default(true),
  // The ES query that will be stored as a percolator document.
  // For keyword: { match: { message: keyword } }
  // For threshold: { bool: { filter: [{ term: { severity: 'error' } }] } }
  esQuery: z.record(z.string(), z.unknown()).default({}),
});

export type AlertRule = z.infer<typeof AlertRuleSchema>;
