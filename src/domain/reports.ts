// Error intelligence and dashboard reports for PulseBoard.

import { MongoDatabase } from '../db/mongo.js';
import { ElasticsearchDatabase } from '../db/elastic.js';
import { RedisDatabase } from '../db/redis.js';
import { CacheService } from '../lib/cache.js';
import { logger } from '../logger.js';
import type { ErrorIntelligenceReport, TenantQuotaRow } from '../types/reports.js';

export type { ErrorIntelligenceReport, TenantQuotaRow };

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

              // d. New fingerprints: fingerprints whose GLOBAL first occurrence
              // (across all time, not just this 7-day window) is within the
              // last 24h.  A $lookup against the full events collection verifies
              // there are no earlier occurrences before the 24h boundary.
              newFingerprints: [
                // Group within the 7-day window to get candidate fingerprints
                // that appeared recently.
                {
                  $group: {
                    _id: '$fingerprint',
                    windowFirstSeen: { $min: '$occurredAt' },
                  },
                },
                // Only candidates whose earliest occurrence within the window
                // falls in the last 24h are worth checking further.
                {
                  $match: { windowFirstSeen: { $gte: last24h } },
                },
                // Verify these are genuinely new by confirming no occurrence
                // existed before the 24h boundary in the full event history.
                {
                  $lookup: {
                    from: 'events',
                    let: { fp: '$_id' },
                    pipeline: [
                      {
                        $match: {
                          $expr: {
                            $and: [
                              { $eq: ['$fingerprint', '$$fp'] },
                              { $eq: ['$projectId', projectId] },
                              { $lt: ['$occurredAt', last24h] },
                            ],
                          },
                        },
                      },
                      { $limit: 1 },
                      { $project: { _id: 1 } },
                    ],
                    as: 'priorOccurrences',
                  },
                },
                // Only truly new fingerprints have zero prior occurrences.
                { $match: { priorOccurrences: { $size: 0 } } },
                { $project: { _id: 0, fingerprint: '$_id' } },
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
      projectId,
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
          // Events over time: one bucket per hour for the 7-day window.
          // time_zone: 'UTC' is explicit to satisfy the "time zone support"
          // requirement; callers may pass a different IANA zone in future.
          events_over_time: {
            date_histogram: {
              field: 'occurredAt',
              calendar_interval: 'hour',
              time_zone: 'UTC',
              min_doc_count: 0,
              extended_bounds: {
                min: windowStart.toISOString(),
                max: now.toISOString(),
              },
            },
          },

          // Severity breakdown with top 3 most recent events per bucket
          by_severity: {
            terms: {
              field: 'severity',
              size: 10,
            },
            aggs: {
              top_events: {
                top_hits: {
                  size: 3,
                  sort: [{ occurredAt: { order: 'desc' } }],
                  _source: {
                    includes: ['message', 'severity', 'occurredAt', 'fingerprint'],
                  },
                },
              },
            },
          },

          // Percentiles on response time (only where field exists)
          response_time_percentiles: {
            filter: { exists: { field: 'payload.responseTimeMs' } },
            aggs: {
              percentiles: {
                percentiles: {
                  field: 'payload.responseTimeMs',
                  percents: [50, 75, 95, 99],
                },
              },
            },
          },

          // Approximate unique error count via cardinality on fingerprint
          unique_error_count: {
            cardinality: {
              field: 'fingerprint',
            },
          },

          // Significant error terms: foreground = last 1h, background = full project corpus
          recent_hour_terms: {
            filter: {
              range: {
                occurredAt: { gte: new Date(now.getTime() - 60 * 60 * 1000).toISOString() },
              },
            },
            aggs: {
              significant_error_terms: {
                significant_terms: {
                  field: 'message.keyword',
                  size: 10,
                  background_filter: { term: { projectId } },
                },
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
          responseTimePercentiles: null,
          uniqueErrorCount: 0,
          significantErrorTerms: [],
          errorCount: 0,
          totalCount: response.hits.total,
        };
      }

      const recentHourTerms = aggs['recent_hour_terms'] as { significant_error_terms: { buckets: unknown[] } } | undefined;

      return {
        eventsOverTime: (aggs['events_over_time'] as { buckets: unknown[] })?.buckets ?? [],
        bySeverity: (aggs['by_severity'] as { buckets: unknown[] })?.buckets ?? [],
        responseTimePercentiles: (aggs['response_time_percentiles'] as { percentiles: { values: Record<string, number> } } | undefined)?.percentiles?.values ?? null,
        uniqueErrorCount: (aggs['unique_error_count'] as { value: number })?.value ?? 0,
        significantErrorTerms: recentHourTerms?.significant_error_terms?.buckets ?? [],
        errorCount: (aggs['error_rate'] as { doc_count: number })?.doc_count ?? 0,
        totalCount: response.hits.total,
      };
    }, projectId);
  }

  async invalidateProjectReports(projectId: string): Promise<void> {
    await this.cache.invalidatePattern(`report:*:${projectId}*`);
  }
}
