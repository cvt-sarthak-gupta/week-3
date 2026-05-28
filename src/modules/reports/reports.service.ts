import type { PostgresPool } from '../../db/postgres/index.js';
import type { MongoDatabase } from '../../db/mongo/index.js';
import { logger } from '../../utils/logger.js';
import type { ErrorIntelligenceResult, QuotaReportRow } from './reports.types.js';

export class ReportsService {
  constructor(
    private readonly pool: PostgresPool,
    private readonly mongo: MongoDatabase,
  ) {}

  // ---------------------------------------------------------------------------
  // getErrorIntelligence — single $facet aggregation over MongoDB events
  // ---------------------------------------------------------------------------

  async getErrorIntelligence(
    projectId: string,
    days = 7,
  ): Promise<ErrorIntelligenceResult> {
    const now = new Date();
    const windowStart = new Date(now.getTime() - days * 24 * 60 * 60 * 1000);
    const last24h = new Date(now.getTime() - 24 * 60 * 60 * 1000);

    const pipeline = [
      // Stage 1: filter to project + time window
      {
        $match: {
          projectId,
          occurredAt: { $gte: windowStart },
        },
      },
      // Stage 2: $facet with 4 sub-pipelines
      {
        $facet: {
          // a. Top errors: group by fingerprint
          topErrors: [
            {
              $group: {
                _id: '$fingerprint',
                message: { $first: '$message' },
                count: { $sum: 1 },
                firstSeen: { $min: '$occurredAt' },
                lastSeen: { $max: '$occurredAt' },
                affectedUsers: {
                  $addToSet: '$userContext.userId',
                },
              },
            },
            { $sort: { count: -1 } },
            { $limit: 10 },
            {
              $project: {
                _id: 0,
                fingerprint: '$_id',
                message: 1,
                count: 1,
                firstSeen: 1,
                lastSeen: 1,
                affectedUsers: {
                  $filter: {
                    input: '$affectedUsers',
                    as: 'uid',
                    cond: { $ne: ['$$uid', null] },
                  },
                },
              },
            },
          ],

          // b. Hourly histogram: group by hour bucket
          hourlyHistogram: [
            {
              $group: {
                _id: {
                  $dateToString: {
                    format: '%Y-%m-%dT%H:00:00Z',
                    date: '$occurredAt',
                    timezone: 'UTC',
                  },
                },
                count: { $sum: 1 },
              },
            },
            { $sort: { _id: 1 } },
            {
              $project: {
                _id: 0,
                hour: '$_id',
                count: 1,
              },
            },
          ],

          // c. Severity + browser breakdown
          severityBrowser: [
            {
              $group: {
                _id: {
                  severity: '$severity',
                  browser: '$deviceContext.browser',
                },
                count: { $sum: 1 },
              },
            },
            { $sort: { count: -1 } },
            {
              $project: {
                _id: 0,
                severity: '$_id.severity',
                browser: { $ifNull: ['$_id.browser', 'unknown'] },
                count: 1,
              },
            },
          ],

          // d. New fingerprints: first seen within last 24h only
          newFingerprints: [
            {
              $group: {
                _id: '$fingerprint',
                firstSeen: { $min: '$occurredAt' },
              },
            },
            {
              $match: {
                firstSeen: { $gte: last24h },
              },
            },
            {
              $project: {
                _id: 0,
                fingerprint: '$_id',
              },
            },
          ],
        },
      },
    ];

    const results = await this.mongo.events().aggregate(pipeline).toArray();
    const facet = results[0] as {
      topErrors: ErrorIntelligenceResult['topErrors'];
      hourlyHistogram: ErrorIntelligenceResult['hourlyHistogram'];
      severityBrowser: Array<{ severity: string; browser: string; count: number }>;
      newFingerprints: Array<{ fingerprint: string }>;
    } | undefined;

    if (facet === undefined) {
      logger.warn({ projectId, days }, 'getErrorIntelligence: no facet result');
      return {
        topErrors: [],
        hourlyHistogram: [],
        severityBrowserBreakdown: [],
        newFingerprints: [],
      };
    }

    return {
      topErrors: facet.topErrors,
      hourlyHistogram: facet.hourlyHistogram,
      severityBrowserBreakdown: facet.severityBrowser,
      newFingerprints: facet.newFingerprints.map((r) => r.fingerprint),
    };
  }

  // ---------------------------------------------------------------------------
  // getQuotaReport — RANK() window function over monthly_usage in Postgres
  // ---------------------------------------------------------------------------

  async getQuotaReport(): Promise<QuotaReportRow[]> {
    return this.pool.withClient(async (client) => {
      const now = new Date();
      const thisYear = now.getUTCFullYear();
      const thisMonth = now.getUTCMonth() + 1;

      const prevMonth = thisMonth === 1 ? 12 : thisMonth - 1;
      const prevYear = thisMonth === 1 ? thisYear - 1 : thisYear;

      const result = await client.query<{
        tenant_id: string;
        tenant_name: string;
        this_month_events: string;
        prev_month_events: string;
        growth_rate: string | null;
        rank: string;
      }>(
        `
        WITH month_agg AS (
          SELECT
            mu.tenant_id,
            SUM(mu.event_count) AS this_month
          FROM monthly_usage mu
          WHERE mu.year = $1 AND mu.month = $2
          GROUP BY mu.tenant_id
        ),
        prev_month_agg AS (
          SELECT
            mu.tenant_id,
            SUM(mu.event_count) AS prev_month
          FROM monthly_usage mu
          WHERE mu.year = $3 AND mu.month = $4
          GROUP BY mu.tenant_id
        ),
        ranked AS (
          SELECT
            t.id                                              AS tenant_id,
            t.name                                            AS tenant_name,
            COALESCE(m.this_month, 0)                         AS this_month_events,
            COALESCE(p.prev_month, 0)                         AS prev_month_events,
            CASE
              WHEN COALESCE(p.prev_month, 0) = 0 THEN NULL
              ELSE (COALESCE(m.this_month, 0) - COALESCE(p.prev_month, 0))::NUMERIC
                   / NULLIF(p.prev_month, 0)
            END                                               AS growth_rate,
            RANK() OVER (ORDER BY COALESCE(m.this_month, 0) DESC) AS rank
          FROM tenants t
          LEFT JOIN month_agg      m ON m.tenant_id = t.id
          LEFT JOIN prev_month_agg p ON p.tenant_id = t.id
          WHERE t.is_active = true
        )
        SELECT
          tenant_id,
          tenant_name,
          this_month_events,
          prev_month_events,
          growth_rate,
          rank
        FROM ranked
        ORDER BY rank ASC, tenant_name ASC
        `,
        [thisYear, thisMonth, prevYear, prevMonth],
      );

      return result.rows.map((row) => ({
        tenantId: row.tenant_id,
        tenantName: row.tenant_name,
        thisMonthEvents: parseInt(row.this_month_events, 10),
        prevMonthEvents: parseInt(row.prev_month_events, 10),
        growthRate: row.growth_rate !== null ? parseFloat(row.growth_rate) : null,
        rank: parseInt(row.rank, 10),
      }));
    });
  }
}
