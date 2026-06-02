/**
 * Minimal PulseBoard SDK for demo use.
 * Drop this file into any Node.js project and call pb.capture() to send events.
 */

export interface PBEvent {
  type: 'error' | 'log' | 'metric' | 'custom';
  severity: 'debug' | 'info' | 'warn' | 'error' | 'fatal';
  message: string;
  tags?: Record<string, string>;
  userContext?: { userId?: string; email?: string; ip?: string };
  deviceContext?: { os?: string; browser?: string; version?: string };
  payload?: Record<string, unknown>;
  stackTrace?: { file?: string; function?: string; line?: number; column?: number }[];
  fingerprint?: string;
}

export interface PBConfig {
  apiKey: string;
  host?: string;   // default: http://localhost:3000
  silent?: boolean; // suppress SDK errors to stdout
}

export class PulseBoard {
  private readonly apiKey: string;
  private readonly ingestUrl: string;
  private readonly silent: boolean;
  private queue: PBEvent[] = [];
  private flushing = false;

  constructor(config: PBConfig) {
    this.apiKey   = config.apiKey;
    this.ingestUrl = `${config.host ?? 'http://localhost:3000'}/v1/ingest`;
    this.silent    = config.silent ?? false;
  }

  /** Capture a single event. Fire-and-forget: never throws. */
  capture(event: PBEvent): void {
    this.queue.push(event);
    void this.flush();
  }

  /** Capture an Error object directly. Extracts stack frames automatically. */
  captureError(err: Error, extras: Partial<PBEvent> = {}): void {
    const frames = (err.stack ?? '')
      .split('\n')
      .slice(1)
      .map((line) => {
        const m = line.trim().match(/^at (.+?) \((.+):(\d+):(\d+)\)$/) ??
                  line.trim().match(/^at (.+):(\d+):(\d+)$/);
        if (!m) return null;
        return m.length === 5
          ? { function: m[1], file: m[2], line: Number(m[3]), column: Number(m[4]) }
          : { file: m[1], line: Number(m[2]), column: Number(m[3]) };
      })
      .filter((f): f is NonNullable<typeof f> => f !== null)
      .slice(0, 10);

    this.capture({
      type: 'error',
      severity: 'error',
      message: `${err.name}: ${err.message}`,
      stackTrace: frames,
      fingerprint: `${err.name}:${err.message.slice(0, 80)}`,
      ...extras,
    });
  }

  private async flush(): Promise<void> {
    if (this.flushing || this.queue.length === 0) return;
    this.flushing = true;

    const batch = this.queue.splice(0, 25);
    try {
      const res = await fetch(this.ingestUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-PulseBoard-Key': this.apiKey,
        },
        body: JSON.stringify(batch.length === 1 ? batch[0] : batch),
      });
      if (!res.ok && !this.silent) {
        console.error(`[PulseBoard] ingest ${res.status}: ${await res.text()}`);
      }
    } catch (err) {
      if (!this.silent) console.error('[PulseBoard] send failed:', err);
    } finally {
      this.flushing = false;
      if (this.queue.length > 0) void this.flush();
    }
  }
}
