import { ValidationError } from './errors.js';

export function encodeCursor(sortValues: unknown[]): string {
  return Buffer.from(JSON.stringify(sortValues)).toString('base64');
}

export function decodeCursor(cursor: string): unknown[] {
  try {
    return JSON.parse(Buffer.from(cursor, 'base64').toString('utf8')) as unknown[];
  } catch {
    throw new ValidationError('Invalid cursor token');
  }
}
