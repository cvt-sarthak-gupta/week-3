import pino, { type Logger } from 'pino';
import { config } from '../config.js';

const redactPaths = [
  'req.headers.authorization',
  'req.body.password',
  'req.body.apiKey',
  '*.password',
  '*.passwordHash',
];

function buildTransport(): pino.TransportSingleOptions | undefined {
  if (config.node !== 'production') {
    return {
      target: 'pino-pretty',
      options: {
        colorize: true,
        translateTime: 'SYS:standard',
        ignore: 'pid,hostname',
      },
    };
  }
  return undefined;
}

const transport = buildTransport();

export const logger: Logger = pino({
  level: config.log,
  redact: {
    paths: redactPaths,
    censor: '[REDACTED]',
  },
  ...(transport !== undefined ? { transport } : {}),
});

export function createRequestLogger(requestId: string): Logger {
  return logger.child({ requestId });
}

export function createWorkerLogger(
  workerName: string,
  extra?: Record<string, unknown>,
): Logger {
  return logger.child({ worker: workerName, ...extra });
}
