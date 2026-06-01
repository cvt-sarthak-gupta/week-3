export interface ErrorIntelligenceResult {
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

export interface QuotaReportRow {
  tenantId: string;
  tenantName: string;
  thisMonthEvents: number;
  prevMonthEvents: number;
  growthRate: number | null;
  rank: number;
}
