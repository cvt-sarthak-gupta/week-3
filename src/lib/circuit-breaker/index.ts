import { CircuitBreaker } from './circuit-breaker.js';
export { CircuitBreaker } from './circuit-breaker.js';
export type { BreakerState, BreakerConfig, BreakerMetrics } from './types.js';

export interface CircuitBreakers {
  postgres: CircuitBreaker;
  mongo: CircuitBreaker;
  elasticsearch: CircuitBreaker;
  redis: CircuitBreaker;
}

export function createBreakers(): CircuitBreakers {
  return {
    postgres: new CircuitBreaker('postgres'),
    mongo: new CircuitBreaker('mongo'),
    elasticsearch: new CircuitBreaker('elasticsearch'),
    redis: new CircuitBreaker('redis'),
  };
}
