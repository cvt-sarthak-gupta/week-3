import { PostgresDatabase } from '../db/postgres.js';
import { ElasticsearchDatabase } from '../db/elastic.js';
import { logger } from '../logger.js';

// IlmApplyJob — called daily at 1am
// Reconcile project retention_days → ES ILM tier for all active projects.

export class IlmApplyJob {
  constructor(
    private readonly pg: PostgresDatabase,
    private readonly es: ElasticsearchDatabase,
  ) {}

  async run(): Promise<void> {
    logger.info('ILM apply job starting');

    const result = await this.pg.query<{ id: string; retention_days: number }>(
      `SELECT p.id, pl.retention_days
       FROM projects p
       JOIN tenants t   ON p.tenant_id = t.id
       JOIN plans pl    ON t.plan_id = pl.id
       WHERE p.is_archived = false
         AND t.is_active = true`,
    );

    const projects = result.rows;
    let applied = 0;
    let skipped = 0;
    let errors = 0;

    for (const { id: projectId, retention_days: retentionDays } of projects) {
      try {
        await this.es.applyPolicyForProject(projectId, retentionDays);
        applied++;
      } catch (err) {
        // If the project's index simply doesn't exist yet, log as skipped
        const isNotFound =
          err instanceof Error &&
          (err.message.includes('index_not_found') || err.message.includes('404'));

        if (isNotFound) {
          logger.debug({ projectId }, 'ILM apply: index not found, skipping');
          skipped++;
        } else {
          logger.error({ err, projectId, retentionDays }, 'ILM apply: failed for project');
          errors++;
        }
      }
    }

    logger.info(
      { applied, skipped, errors, total: projects.length },
      'ILM apply job complete',
    );
  }
}
