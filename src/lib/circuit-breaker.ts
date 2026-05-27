// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type BreakerState = 'closed' | 'open' | 'half-open';

export interface BreakerConfig {
  /** Number of consecutive failures before opening the circuit. Default: 5 */
  failureThreshold?: number;
  /** Number of consecutive successes in half-open state before closing. Default: 2 */
  successThreshold?: number;
  /** Milliseconds to wait in open state before trying half-open. Default: 10000 */
  timeout?: number;
}

export interface BreakerMetrics {
  state: BreakerState;
  failures: number;
  successes: number;
  openedAt: number | null;
  totalCalls: number;
  totalFailures: number;
}

export interface Breaker {
  run<T>(fn: () => Promise<T>): Promise<T>;
  getState(): BreakerState;
  getMetrics(): BreakerMetrics;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

class CircuitBreakerOpenError extends Error {
  constructor(name: string) {
    super(`Circuit breaker '${name}' is open`);
    this.name = 'CircuitBreakerOpenError';
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

function createBreaker(name: string, opts?: BreakerConfig): Breaker {
  const failureThreshold = opts?.failureThreshold ?? 5;
  const successThreshold = opts?.successThreshold ?? 2;
  const timeoutMs = opts?.timeout ?? 10_000;

  let state: BreakerState = 'closed';
  let failures = 0;
  let successes = 0;
  let openedAt: number | null = null;
  let totalCalls = 0;
  let totalFailures = 0;

  function open(): void {
    state = 'open';
    openedAt = Date.now();
    failures = 0;
    successes = 0;
  }

  function close(): void {
    state = 'closed';
    failures = 0;
    successes = 0;
    openedAt = null;
  }

  async function run<T>(fn: () => Promise<T>): Promise<T> {
    totalCalls++;

    if (state === 'open') {
      const elapsed = Date.now() - (openedAt ?? 0);
      if (elapsed >= timeoutMs) {
        state = 'half-open';
        successes = 0;
        failures = 0;
      } else {
        throw new CircuitBreakerOpenError(name);
      }
    }

    try {
      const result = await fn();

      // Success path
      if (state === 'half-open') {
        successes++;
        if (successes >= successThreshold) {
          close();
        }
      } else {
        // In closed state, a success resets the failure counter
        failures = 0;
      }

      return result;
    } catch (err) {
      totalFailures++;
      failures++;

      if (state === 'half-open') {
        // Any failure in half-open re-opens immediately
        open();
      } else if (failures >= failureThreshold) {
        open();
      }

      throw err;
    }
  }

  function getState(): BreakerState {
    if (state === 'open' && openedAt !== null && Date.now() - openedAt >= timeoutMs) {
      state = 'half-open';
      successes = 0;
      failures = 0;
    }
    return state;
  }

  function getMetrics(): BreakerMetrics {
    return {
      state,
      failures,
      successes,
      openedAt,
      totalCalls,
      totalFailures,
    };
  }

  return { run, getState, getMetrics };
}

// ---------------------------------------------------------------------------
// Module-level named breaker instances
// ---------------------------------------------------------------------------

export const breakers: Readonly<Record<'postgres' | 'mongo' | 'elasticsearch' | 'redis', Breaker>> =
  Object.freeze({
    postgres: createBreaker('postgres'),
    mongo: createBreaker('mongo'),
    elasticsearch: createBreaker('elasticsearch'),
    redis: createBreaker('redis'),
  });

export { createBreaker };
