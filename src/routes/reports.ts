import type { FastifyInstance, FastifyPluginAsync, FastifyReply } from 'fastify';
import fp from 'fastify-plugin';
import { pool } from '../db/postgres.js';
import { authenticateUser } from '../lib/auth.js';
import { getOrFill } from '../lib/cache.js';
import { getErrorIntelligence, getTenantQuotaReport } from '../domain/reports.js';
import { esClient, aliasName } from '../db/elastic.js';
import { ForbiddenError } from '../errors.js';

const REPORT_CACHE_TTL_SECONDS = 300; // 5 minutes

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

async function fetchDashboardReport(projectId: string): Promise<Record<string, unknown>> {
  const alias = aliasName(projectId);
  const now = new Date();
  // E4 spec: date histogram covers last 7 days; significant_terms compares last 1h foreground vs 7d baseline
  const sevenDaysAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000).toISOString();
  const oneHourAgo   = new Date(now.getTime() -      60 * 60 * 1000).toISOString();
  const nowIso = now.toISOString();

  const response = await esClient.search({
    index: alias,
    size: 0,
    query: {
      bool: {
        filter: [
          { term: { projectId } },
          { range: { occurredAt: { gte: sevenDaysAgo } } },
        ],
      },
    },
    aggs: {
      // E4: hourly histogram for last 7 days with UTC timezone support
      timeseries: {
        date_histogram: {
          field: 'occurredAt',
          fixed_interval: '1h',
          time_zone: 'UTC',
          min_doc_count: 0,
          extended_bounds: {
            min: sevenDaysAgo,
            max: nowIso,
          },
        },
      },
      // E4: terms agg on severity with top_hits showing 3 most recent events per bucket
      severity_breakdown: {
        terms: { field: 'severity', size: 10 },
        aggs: {
          recent_events: {
            top_hits: {
              size: 3,
              sort: [{ occurredAt: { order: 'desc' } }],
              _source: ['message', 'occurredAt', 'fingerprint', 'severity'],
            },
          },
        },
      },
      // E4: percentiles on payload.responseTimeMs (only where field exists)
      response_time_percentiles: {
        percentiles: {
          field: 'payload.responseTimeMs',
          percents: [50, 95, 99],
        },
      },
      // E4: cardinality for approximate unique error count
      unique_errors: {
        cardinality: { field: 'fingerprint' },
      },
      // E4: significant_terms — foreground = last 1h, background = full 7-day query context.
      // This surfaces terms that appear anomalously often in the last hour compared to the
      // 7-day baseline, making it effective for detecting sudden error spikes or new failure modes.
      last_hour_anomalies: {
        filter: { range: { occurredAt: { gte: oneHourAgo } } },
        aggs: {
          anomalous_terms: {
            significant_terms: {
              field: 'message',
              size: 10,
            },
          },
        },
      },
    },
  });

  type TimeseriesBucket = { key_as_string: string; doc_count: number };
  type TopHit = { _source: { message?: string; occurredAt?: string; fingerprint?: string; severity?: string } };
  type SeverityBucket = { key: string; doc_count: number; recent_events?: { hits: { hits: TopHit[] } } };
  type SignificantTermsBucket = { key: string; score: number; doc_count: number };
  type LastHourAgg = { doc_count: number; anomalous_terms?: { buckets: SignificantTermsBucket[] } };

  const aggs = response.aggregations as {
    timeseries?: { buckets: TimeseriesBucket[] };
    severity_breakdown?: { buckets: SeverityBucket[] };
    response_time_percentiles?: { values: { '50.0'?: number; '95.0'?: number; '99.0'?: number } };
    unique_errors?: { value: number };
    last_hour_anomalies?: LastHourAgg;
  };

  const timeseries = (aggs?.timeseries?.buckets ?? []).map((b) => ({
    timestamp: b.key_as_string,
    count: b.doc_count,
  }));

  const severityBreakdown = (aggs?.severity_breakdown?.buckets ?? []).map((b) => ({
    severity: b.key,
    count: b.doc_count,
    recentEvents: (b.recent_events?.hits.hits ?? []).map((h) => h._source),
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
    anomalousTerms: (aggs?.last_hour_anomalies?.anomalous_terms?.buckets ?? []).map((b) => ({
      term: b.key,
      score: b.score,
      count: b.doc_count,
    })),
    generatedAt: nowIso,
  };
}

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
