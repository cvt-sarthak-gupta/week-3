import { runRetentionJob } from '../domain/retention.js';
import { logger } from '../logger.js';

export async function retentionJob(): Promise<void> {
  logger.info('Retention job starting');
  const result = await runRetentionJob();
  logger.info({ result }, 'Retention job complete');
}
