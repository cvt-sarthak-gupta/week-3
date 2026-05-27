import type { FastifyInstance, FastifyPluginAsync, FastifyReply } from 'fastify';
import fp from 'fastify-plugin';
import { pool } from '../db/postgres.js';
import { authenticateUser } from '../lib/auth.js';
import { getOrFill } from '../lib/cache.js';
import { getErrorIntelligence, getTenantQuotaReport } from '../domain/reports.js';
import { esClient, aliasName } from '../db/elastic.js';
import { ForbiddenError } from '../errors.js';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const REPORT_CACHE_TTL_SECONDS = 300; // 5 minutes

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface ProjectParams {
  tenantId: string;
  projectId: string;
}

interface ErrorIntelligenceQuerystring {
  days?: number;
}

interface MemberRow {
  user_id: string;
  role: string;
}

// ---------------------------------------------------------------------------
// Membership guard
// ---------------------------------------------------------------------------

async function assertMember(userId: string, tenantId: string): Promise<MemberRow> {
  const result = await pool.query<MemberRow>(
    'SELECT user_id, role FROM tenant_members WHERE tenant_id = $1 AND user_id = $2 LIMIT 1',
    [tenantId, userId],
  );
  const member = result.rows[0];
  if (member === undefined) {
    throw new ForbiddenError('Not a member of this tenant');
  }
  return member;
}

async function assertAdmin(userId: string): Promise<void> {
  const result = await pool.query<{ role: string }>(
    `SELECT role FROM users WHERE id = $1 AND role = 'admin' LIMIT 1`,
    [userId],
  );
  if (result.rows[0] === undefined) {
    throw new ForbiddenError('Admin access required');
  }
}

// ---------------------------------------------------------------------------
// Dashboard ES aggregation
// ---------------------------------------------------------------------------

async function fetchDashboardReport(projectId: string): Promise<Record<string, unknown>> {
  const alias = aliasName(projectId);
  const fromDate = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();

  const response = await esClient.search({
    index: alias,
    size: 0,
    query: {
      bool: {
        filter: [
          { term: { projectId } },
          { range: { occurredAt: { gte: fromDate } } },
        ],
      },
    },
    aggs: {
      timeseries: {
        date_histogram: {
          field: 'occurredAt',
          fixed_interval: '1h',
          min_doc_count: 0,
          extended_bounds: {
            min: fromDate,
            max: new Date().toISOString(),
          },
        },
      },
      severity_breakdown: {
        terms: { field: 'severity', size: 10 },
      },
      response_time_percentiles: {
        percentiles: {
          field: 'payload.responseTimeMs',
          percents: [50, 95, 99],
        },
      },
      unique_errors: {
        cardinality: { field: 'fingerprint' },
      },
      anomalous_terms: {
        significant_terms: {
          field: 'message',
          size: 10,
        },
      },
    },
  });

  type TimeseriesBucket = { key_as_string: string; doc_count: number };
  type SeverityBucket = { key: string; doc_count: number };
  type SignificantTermsBucket = { key: string };

  const aggs = response.aggregations as {
    timeseries?: { buckets: TimeseriesBucket[] };
    severity_breakdown?: { buckets: SeverityBucket[] };
    response_time_percentiles?: { values: { '50.0'?: number; '95.0'?: number; '99.0'?: number } };
    unique_errors?: { value: number };
    anomalous_terms?: { buckets: SignificantTermsBucket[] };
  };

  const timeseries = (aggs?.timeseries?.buckets ?? []).map((b) => ({
    timestamp: b.key_as_string,
    count: b.doc_count,
  }));

  const severityBreakdown = (aggs?.severity_breakdown?.buckets ?? []).map((b) => ({
    severity: b.key,
    count: b.doc_count,
  }));

  const pctValues = aggs?.response_time_percentiles?.values ?? {};

  return {
    projectId,
    timeseries,
    severityBreakdown,
    percentiles: {
      p50: pctValues['50.0'] ?? 0,
      p95: pctValues['95.0'] ?? 0,
      p99: pctValues['99.0'] ?? 0,
    },
    uniqueErrors: aggs?.unique_errors?.value ?? 0,
    anomalousTerms: (aggs?.anomalous_terms?.buckets ?? []).map((b) => b.key),
    generatedAt: new Date().toISOString(),
  };
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

const reportPluginHandler: FastifyPluginAsync = async (fastify: FastifyInstance) => {
  fastify.get<{ Params: ProjectParams; Querystring: ErrorIntelligenceQuerystring }>(
    '/tenants/:tenantId/projects/:projectId/reports/error-intelligence',
    {
      preHandler: [authenticateUser],
      schema: {
        tags: ['reports'],
        params: {
          type: 'object',
          properties: {
            tenantId: { type: 'string' },
            projectId: { type: 'string' },
          },
        },
        querystring: {
          type: 'object',
          properties: {
            days: { type: 'integer', minimum: 1, maximum: 90, default: 7 },
          },
        },
      },
    },
    async (request, reply: FastifyReply): Promise<void> => {
      const { tenantId, projectId } = request.params;
      const userId = request.user.userId;
      const days = request.query.days ?? 7;

      await assertMember(userId, tenantId);

      const cacheKey = `report:${projectId}:error-intel:${days}`;

      const report = await getOrFill(
        cacheKey,
        REPORT_CACHE_TTL_SECONDS,
        () => getErrorIntelligence(projectId, days),
        projectId,
      );

      void reply.status(200).send(report);
    },
  );

  fastify.get<{ Params: ProjectParams }>(
    '/tenants/:tenantId/projects/:projectId/reports/dashboard',
    {
      preHandler: [authenticateUser],
      schema: {
        tags: ['reports'],
        params: {
          type: 'object',
          properties: {
            tenantId: { type: 'string' },
            projectId: { type: 'string' },
          },
        },
      },
    },
    async (request, reply: FastifyReply): Promise<void> => {
      const { tenantId, projectId } = request.params;
      const userId = request.user.userId;

      await assertMember(userId, tenantId);

      const cacheKey = `report:${projectId}:dashboard`;

      const report = await getOrFill(
        cacheKey,
        REPORT_CACHE_TTL_SECONDS,
        () => fetchDashboardReport(projectId),
        projectId,
      );

      void reply.status(200).send(report);
    },
  );

  // Admin-only: full multi-tenant quota report
  fastify.get(
    '/admin/reports/quota',
    {
      preHandler: [authenticateUser],
      schema: {
        tags: ['admin', 'reports'],
      },
    },
    async (request, reply: FastifyReply): Promise<void> => {
      const userId = request.user.userId;

      await assertAdmin(userId);

      const report = await getTenantQuotaReport();
      void reply.status(200).send({ report });
    },
  );
};

export const reportRoutes = fp(reportPluginHandler, {
  name: 'report-routes',
  fastify: '4.x',
});
