import fp from 'fastify-plugin';
import type { FastifyPluginAsync, FastifyReply } from 'fastify';
import type { ApiKeyService } from '../../lib/auth/index.js';
import type { IngestService } from './ingest.service.js';
import type { EventBody } from './ingest.types.js';

export function createIngestRoutes(
  ingestService: IngestService,
  apiKeyService: ApiKeyService,
): FastifyPluginAsync {
  return fp(async (fastify) => {
    const apiKeyPreHandler = apiKeyService.createPreHandler();

    fastify.post<{ Body: EventBody | EventBody[] }>(
      '/ingest',
      {
        preHandler: [apiKeyPreHandler],
        schema: {
          tags: ['ingest'],
          body: {
            oneOf: [
              {
                type: 'object',
                required: ['type', 'severity', 'message'],
                properties: {
                  type: { type: 'string', enum: ['error', 'log', 'metric', 'custom'] },
                  severity: { type: 'string', enum: ['debug', 'info', 'warn', 'error', 'fatal'] },
                  message: { type: 'string', minLength: 1 },
                  stackTrace: { type: 'array' },
                  tags: { type: 'object' },
                  userContext: { type: 'object' },
                  deviceContext: { type: 'object' },
                  payload: { type: 'object' },
                  occurredAt: { type: 'string' },
                  fingerprint: { type: 'string' },
                },
              },
              {
                type: 'array',
                items: {
                  type: 'object',
                  required: ['type', 'severity', 'message'],
                  properties: {
                    type: { type: 'string', enum: ['error', 'log', 'metric', 'custom'] },
                    severity: { type: 'string', enum: ['debug', 'info', 'warn', 'error', 'fatal'] },
                    message: { type: 'string', minLength: 1 },
                    stackTrace: { type: 'array' },
                    tags: { type: 'object' },
                    userContext: { type: 'object' },
                    deviceContext: { type: 'object' },
                    payload: { type: 'object' },
                    occurredAt: { type: 'string' },
                    fingerprint: { type: 'string' },
                  },
                },
              },
            ],
          },
        },
      },
      async (request, reply: FastifyReply): Promise<void> => {
        const { id: projectId, tenantId, apiKey } = request.project!;
        const rl = await ingestService.checkRateLimit(apiKey, tenantId);
        void reply
          .header('X-RateLimit-Remaining', String(rl.remaining))
          .header('X-RateLimit-Reset', String(rl.resetAt));

        if (Array.isArray(request.body)) {
          const result = await ingestService.ingestBatch(
            request.body as EventBody[],
            projectId,
            tenantId,
          );
          void reply.status(202).send(result);
        } else {
          const result = await ingestService.ingestOne(
            request.body as EventBody,
            projectId,
            tenantId,
          );
          void reply.status(202).send(result);
        }
      },
    );
  }, { name: 'ingest-routes', fastify: '4.x' });
}
