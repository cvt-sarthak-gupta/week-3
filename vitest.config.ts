import { defineConfig } from 'vitest/config';
import { resolve } from 'path';
import { loadEnv } from 'vite';

export default defineConfig(({ mode }) => {
  // Load .env.test when running tests
  const env = loadEnv(mode ?? 'test', process.cwd(), '');
  return {
  resolve: {
    alias: {
      '@': resolve(__dirname, './src'),
    },
  },
  test: {
    environment: 'node',
    globals: true,
    env,                              // inject .env.test variables
    include: ['tests/integration/**/*.test.ts'],
    setupFiles: ['tests/helpers/setup.ts'],
    testTimeout: 60_000,
    hookTimeout: 60_000,
    // Run integration test files serially to avoid connection pool exhaustion
    // across Postgres, Mongo, Elasticsearch and Redis simultaneously.
    pool: 'forks',
    poolOptions: {
      forks: {
        singleFork: true,
      },
    },
    reporters: ['verbose'],
  },
  };
});
