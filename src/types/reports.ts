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
  newFingerprints: string[];
}

export interface TenantQuotaRow {
  tenantId: string;
  tenantName: string;
  thisMonthEvents: number;
  prevMonthEvents: number;
  growthRate: number | null; // null when prev_month is 0
  rank: number;
}
