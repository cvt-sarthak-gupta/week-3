// Error intelligence and dashboard reports for PulseBoard.

import { MongoDatabase } from '../db/mongo.js';
import { ElasticsearchDatabase } from '../db/elastic.js';
import { RedisDatabase } from '../db/redis.js';
import { CacheService } from '../lib/cache.js';
import { logger } from '../logger.js';

export interface ErrorIntelligenceReport {
  topErrors: Array<{
    fingerprint: string;
    message: string;
    count: number;
    firstSeen: Date;
    lastSeen: Date;
    affectedUsers: string[];
  }>;
  hourlyHistogram: Array<{ hour: string; count: number }>;
  severityBrowserBreakdown: Array<{
    severity: string;
    browser: string;
    count: number;
  }>;
  newFingerprints: string[]; // first seen in last 24h
}

export interface TenantQuotaRow {
  tenantId: string;
  tenantName: string;
  thisMonthEvents: number;
  prevMonthEvents: number;
  growthRate: number | null; // null when prev_month is 0
  rank: number;
}

export class ReportService {
  private readonly mongo: MongoDatabase;
  private readonly es: ElasticsearchDatabase;
  private readonly redis: RedisDatabase;
  private readonly cache: CacheService;

  constructor(
    mongo: MongoDatabase,
    es: ElasticsearchDatabase,
    redis: RedisDatabase,
    cache: CacheService,
  ) {
    this.mongo = mongo;
    this.es = es;
    this.redis = redis;
    this.cache = cache;
  }

  async getErrorIntelligenceReport(
    projectId: string,
    days = 7,
  ): Promise<unknown> {
    const cacheKey = `report:error-intelligence:${projectId}:${days}`;
    const ttlSeconds = 300; // 5 minutes

    return this.cache.getOrFill<ErrorIntelligenceReport>(
      cacheKey,
      ttlSeconds,
      async () => {
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
                    // Filter out null/undefined user IDs
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
              // Two-stage approach: group by fingerprint, keep only those whose
              // firstSeen falls within the last 24h window.
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
        const facet = results[0] as
          | {
              topErrors: ErrorIntelligenceReport['topErrors'];
              hourlyHistogram: ErrorIntelligenceReport['hourlyHistogram'];
              severityBrowser: Array<{ severity: string; browser: string; count: number }>;
              newFingerprints: Array<{ fingerprint: string }>;
            }
          | undefined;

        if (facet === undefined) {
          logger.warn({ projectId, days }, 'getErrorIntelligenceReport: no facet result');
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
      },
    );
  }

  async getDashboardReport(projectId: string): Promise<unknown> {
    const cacheKey = `report:dashboard:${projectId}`;
    const ttlSeconds = 120; // 2 minutes

    return this.cache.getOrFill(cacheKey, ttlSeconds, async () => {
      const now = new Date();
      const windowStart = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
      const alias = this.es.aliasName(projectId);

      const response = await this.es.client.search({
        index: alias,
        size: 0,
        query: {
          bool: {
            filter: [
              { term: { projectId } },
              {
                range: {
                  occurredAt: {
                    gte: windowStart.toISOString(),
                    lte: now.toISOString(),
                  },
                },
              },
            ],
          },
        },
        aggs: {
          // Events over time: one bucket per hour for the 7-day window
          events_over_time: {
            date_histogram: {
              field: 'occurredAt',
              calendar_interval: 'hour',
              min_doc_count: 0,
              extended_bounds: {
                min: windowStart.toISOString(),
                max: now.toISOString(),
              },
            },
          },

          // Severity breakdown
          by_severity: {
            terms: {
              field: 'severity',
              size: 10,
            },
          },

          // Top affected users with latest event sample via top_hits
          top_affected_users: {
            terms: {
              field: 'userContext.userId',
              size: 10,
              order: { _count: 'desc' },
              min_doc_count: 1,
            },
            aggs: {
              latest_event: {
                top_hits: {
                  size: 1,
                  sort: [{ occurredAt: { order: 'desc' } }],
                  _source: {
                    includes: ['message', 'severity', 'occurredAt', 'fingerprint'],
                  },
                },
              },
            },
          },

          // Significant error terms relative to background corpus
          significant_error_terms: {
            significant_terms: {
              field: 'message.keyword',
              size: 10,
              background_filter: {
                term: { projectId },
              },
            },
          },

          // Overall error rate vs warning rate
          error_rate: {
            filter: {
              terms: { severity: ['error', 'fatal'] },
            },
          },
        },
      });

      const aggs = response.aggregations as Record<string, unknown> | undefined;

      if (!aggs) {
        logger.warn({ projectId }, 'getDashboardReport: no aggregations returned');
        return {
          eventsOverTime: [],
          bySeverity: [],
          topAffectedUsers: [],
          significantErrorTerms: [],
          errorCount: 0,
          totalCount: response.hits.total,
        };
      }

      return {
        eventsOverTime: (aggs['events_over_time'] as { buckets: unknown[] })?.buckets ?? [],
        bySeverity: (aggs['by_severity'] as { buckets: unknown[] })?.buckets ?? [],
        topAffectedUsers: (aggs['top_affected_users'] as { buckets: unknown[] })?.buckets ?? [],
        significantErrorTerms:
          (aggs['significant_error_terms'] as { buckets: unknown[] })?.buckets ?? [],
        errorCount: (aggs['error_rate'] as { doc_count: number })?.doc_count ?? 0,
        totalCount: response.hits.total,
      };
    });
  }

  async invalidateProjectReports(projectId: string): Promise<void> {
    await this.cache.invalidatePattern(`report:*:${projectId}*`);
  }
}
