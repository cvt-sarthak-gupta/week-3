import fp from 'fastify-plugin';
import { nanoid } from 'nanoid';
import type { FastifyInstance, FastifyPluginAsync } from 'fastify';

export function generateRequestId(): string {
  return nanoid(16);
}

const REQUEST_ID_HEADER = 'x-request-id';

const correlationPluginHandler: FastifyPluginAsync = async (fastify: FastifyInstance) => {
  fastify.addHook('onRequest', async (request, reply) => {
    const incoming = request.headers[REQUEST_ID_HEADER];
    const requestId = typeof incoming === 'string' && incoming.length > 0
      ? incoming
      : generateRequestId();

    // Fastify exposes request.id — reassign via the internal id field
    // The cast is necessary because FastifyRequest.id is typed as string
    // but the generated id field is set before the hook runs when using
    // a custom genReqId option; overwriting here ensures correlation.
    (request as { id: string }).id = requestId;

    void reply.header(REQUEST_ID_HEADER, requestId);
  });
};

// Wrap with fastify-plugin so the hook is not scoped to a child context
export const correlationPlugin = fp(correlationPluginHandler, {
  name: 'correlation',
  fastify: '4.x',
});
