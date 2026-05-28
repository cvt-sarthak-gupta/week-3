import type { BreakerState, BreakerConfig, BreakerMetrics } from './types.js';

class CircuitBreakerOpenError extends Error {
  constructor(name: string) {
    super(`Circuit breaker '${name}' is open`);
    this.name = 'CircuitBreakerOpenError';
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class CircuitBreaker {
  private state: BreakerState = 'closed';
  private failures = 0;
  private successes = 0;
  private openedAt: number | null = null;
  private totalCalls = 0;
  private totalFailures = 0;

  private readonly failureThreshold: number;
  private readonly successThreshold: number;
  private readonly timeoutMs: number;

  constructor(private readonly name: string, opts?: BreakerConfig) {
    this.failureThreshold = opts?.failureThreshold ?? 5;
    this.successThreshold = opts?.successThreshold ?? 2;
    this.timeoutMs = opts?.timeout ?? 10_000;
  }

  async run<T>(fn: () => Promise<T>): Promise<T> {
    this.totalCalls++;

    if (this.state === 'open') {
      const elapsed = Date.now() - (this.openedAt ?? 0);
      if (elapsed >= this.timeoutMs) {
        this.state = 'half-open';
        this.successes = 0;
        this.failures = 0;
      } else {
        throw new CircuitBreakerOpenError(this.name);
      }
    }

    try {
      const result = await fn();

      // Success path
      if (this.state === 'half-open') {
        this.successes++;
        if (this.successes >= this.successThreshold) {
          this.close();
        }
      } else {
        // In closed state, a success resets the failure counter
        this.failures = 0;
      }

      return result;
    } catch (err) {
      this.totalFailures++;
      this.failures++;

      if (this.state === 'half-open') {
        // Any failure in half-open re-opens immediately
        this.open();
      } else if (this.failures >= this.failureThreshold) {
        this.open();
      }

      throw err;
    }
  }

  getState(): BreakerState {
    if (this.state === 'open' && this.openedAt !== null && Date.now() - this.openedAt >= this.timeoutMs) {
      this.state = 'half-open';
      this.successes = 0;
      this.failures = 0;
    }
    return this.state;
  }

  getMetrics(): BreakerMetrics {
    return {
      state: this.state,
      failures: this.failures,
      successes: this.successes,
      openedAt: this.openedAt,
      totalCalls: this.totalCalls,
      totalFailures: this.totalFailures,
    };
  }

  private open(): void {
    this.state = 'open';
    this.openedAt = Date.now();
    this.failures = 0;
    this.successes = 0;
  }

  private close(): void {
    this.state = 'closed';
    this.failures = 0;
    this.successes = 0;
    this.openedAt = null;
  }
}
