import { createHash } from 'node:crypto';

export function generateFingerprint(type: string, message: string): string {
  const input = `${type}:${message.slice(0, 100)}`;
  return createHash('sha256').update(input).digest('hex').slice(0, 16);
}
