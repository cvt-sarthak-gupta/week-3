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
