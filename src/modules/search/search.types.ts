// ---------------------------------------------------------------------------
// Search module types
// ---------------------------------------------------------------------------

export interface SearchQuerystring {
  q?: string;
  severity?: string;
  from?: string;
  to?: string;
  cursor?: string;
  limit?: number;
}

export interface SearchHit {
  id: string;
  highlight: Record<string, string[]>;
  [key: string]: unknown;
}

export interface SearchResult {
  hits: SearchHit[];
  total: number;
  nextCursor: string | null;
  took: number;
  cacheHit: boolean;
  stale?: boolean;
}
