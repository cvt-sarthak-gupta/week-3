import type { FastifyError, FastifyReply, FastifyRequest } from 'fastify';
import { logger } from './logger.js';

export class AppError extends Error {
  public readonly statusCode: number;
  public readonly code: string;
  public readonly details?: unknown;

  constructor(
    message: string,
    statusCode: number,
    code: string,
    details?: unknown,
  ) {
    super(message);
    this.name = this.constructor.name;
    this.statusCode = statusCode;
    this.code = code;
    if (details !== undefined) {
      this.details = details;
    }
    // Restore correct prototype chain when targeting ES5 output
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class ValidationError extends AppError {
  constructor(message: string, details?: unknown) {
    super(message, 400, 'VALIDATION_ERROR', details);
  }
}

export class NotFoundError extends AppError {
  constructor(message: string, details?: unknown) {
    super(message, 404, 'NOT_FOUND', details);
  }
}

export class AuthError extends AppError {
  constructor(message: string, details?: unknown) {
    super(message, 401, 'UNAUTHORIZED', details);
  }
}

export class ForbiddenError extends AppError {
  constructor(message: string, details?: unknown) {
    super(message, 403, 'FORBIDDEN', details);
  }
}

export class RateLimitError extends AppError {
  public readonly retryAfter: number;

  constructor(message: string, retryAfter: number, details?: unknown) {
    super(message, 429, 'RATE_LIMITED', details);
    this.retryAfter = retryAfter;
  }
}

export class ConflictError extends AppError {
  constructor(message: string, details?: unknown) {
    super(message, 409, 'CONFLICT', details);
  }
}

export class UpstreamError extends AppError {
  public readonly upstream: string;

  constructor(message: string, upstream: string, details?: unknown) {
    super(message, 502, 'UPSTREAM_ERROR', details);
    this.upstream = upstream;
  }
}

export class ServiceUnavailableError extends AppError {
  public readonly retryAfter?: number;

  constructor(message: string, retryAfter?: number, details?: unknown) {
    super(message, 503, 'SERVICE_UNAVAILABLE', details);
    if (retryAfter !== undefined) {
      this.retryAfter = retryAfter;
    }
  }
}

interface ErrorResponseBody {
  error: string;
  message: string;
  details?: unknown;
}

export function fastifyErrorHandler(
  error: FastifyError | AppError | Error,
  request: FastifyRequest,
  reply: FastifyReply,
): void {
  if (error instanceof AppError) {
    const body: ErrorResponseBody = {
      error: error.code,
      message: error.message,
    };
    if (error.details !== undefined) {
      body.details = error.details;
    }
    void reply.status(error.statusCode).send(body);
    return;
  }

  // Fastify native errors carry a statusCode
  const fastifyErr = error as FastifyError;
  if (typeof fastifyErr.statusCode === 'number' && fastifyErr.statusCode < 500) {
    void reply.status(fastifyErr.statusCode).send({
      error: fastifyErr.code ?? 'FASTIFY_ERROR',
      message: fastifyErr.message,
    });
    return;
  }

  // Unexpected internal error — log full stack, return opaque response
  logger.error(
    { err: error, requestId: request.id },
    'Unhandled internal error',
  );

  void reply.status(500).send({
    error: 'INTERNAL_ERROR',
    message: 'Internal server error',
  });
}
