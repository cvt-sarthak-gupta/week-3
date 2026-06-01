import { RetentionService } from '../domain/retention.js';
import { logger } from '../logger.js';

export class RetentionJob {
  constructor(private readonly retention: RetentionService) {}

  async run(): Promise<void> {
    logger.info('Retention job starting');
    await this.retention.runRetentionJob();
    logger.info('Retention job complete');
  }
}
