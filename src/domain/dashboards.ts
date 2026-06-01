import { randomUUID } from 'node:crypto';
import type { MongoDatabase, DashboardDocument } from '../db/mongo.js';
import { logger } from '../logger.js';

export interface CreateDashboardInput {
  projectId: string;
  tenantId: string;
  userId: string;
  name: string;
  layout?: unknown[];
}

export interface DashboardResult {
  id: string;
  projectId: string;
  tenantId: string;
  name: string;
  layout: unknown[];
  createdAt: Date;
  updatedAt: Date;
}

export class DashboardService {
  constructor(private readonly mongo: MongoDatabase) {}

  /**
   * Atomically saves a new dashboard using a MongoDB multi-document transaction:
   *   1. Insert the dashboard document.
   *   2. Push the dashboard ID to project_configs.settings.dashboardIds.
   *   3. Insert an audit event recording who created the dashboard.
   *
   * If any step fails, the transaction rolls back all three writes.
   */
  async createDashboard(input: CreateDashboardInput): Promise<DashboardResult> {
    const { projectId, tenantId, userId, name, layout = [] } = input;
    const dashboardId = randomUUID();
    const now = new Date();

    const client = this.mongo.db().client;
    const session = client.startSession();

    try {
      await session.withTransaction(async () => {
        // Step 1: Insert dashboard document.
        const dashDoc: DashboardDocument = {
          _id: dashboardId,
          tenantId,
          name,
          layout,
          createdAt: now,
          updatedAt: now,
        };
        await this.mongo.dashboards().insertOne(dashDoc, { session });

        // Step 2: Add dashboardId to project_configs.settings.dashboardIds.
        const updateResult = await this.mongo.projectConfigs().updateOne(
          { _id: projectId },
          {
            $push: { 'settings.dashboardIds': dashboardId } as Record<string, unknown>,
            $set: { updatedAt: now },
          },
          { session },
        );

        if (updateResult.matchedCount === 0) {
          // project_configs must exist; fail the transaction if not.
          throw new Error(`project_configs document not found for projectId=${projectId}`);
        }

        // Step 3: Insert audit event.
        await this.mongo.events().insertOne(
          {
            _id: randomUUID(),
            projectId,
            type: 'custom',
            severity: 'info',
            message: `Dashboard "${name}" created by user ${userId}`,
            occurredAt: now,
            ingestedAt: now,
            fingerprint: `audit:dashboard_created:${dashboardId}`,
            payload: { action: 'dashboard_created', dashboardId, userId },
          },
          { session },
        );
      });
    } finally {
      await session.endSession();
    }

    logger.info({ dashboardId, projectId, tenantId, userId }, 'Dashboard created');

    return {
      id: dashboardId,
      projectId,
      tenantId,
      name,
      layout,
      createdAt: now,
      updatedAt: now,
    };
  }

  async listDashboards(projectId: string): Promise<DashboardResult[]> {
    // Fetch the dashboardIds registered for this project from project_configs.
    // This avoids scanning the entire dashboards collection.
    const config = await this.mongo.projectConfigs().findOne({ _id: projectId });
    const dashboardIds = (config?.settings?.['dashboardIds'] as string[] | undefined) ?? [];

    if (dashboardIds.length === 0) return [];

    // Fetch only the relevant dashboard documents by ID.
    const docs = await this.mongo
      .dashboards()
      .find({ _id: { $in: dashboardIds } })
      .sort({ createdAt: -1 })
      .toArray();

    return docs.map((d) => ({
      id: d._id,
      projectId,
      tenantId: d.tenantId,
      name: d.name,
      layout: d.layout,
      createdAt: d.createdAt,
      updatedAt: d.updatedAt,
    }));
  }
}
