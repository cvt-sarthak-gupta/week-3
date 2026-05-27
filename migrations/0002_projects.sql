CREATE TABLE IF NOT EXISTS projects (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  slug TEXT NOT NULL,
  api_key UUID NOT NULL DEFAULT gen_random_uuid(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  is_archived BOOLEAN NOT NULL DEFAULT false,
  CONSTRAINT projects_api_key_unique UNIQUE (api_key),
  CONSTRAINT projects_tenant_slug_unique UNIQUE (tenant_id, slug)
);

CREATE TABLE IF NOT EXISTS monthly_usage (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  year INT NOT NULL,
  month INT NOT NULL,
  event_count BIGINT NOT NULL DEFAULT 0,
  alert_fires INT NOT NULL DEFAULT 0,
  PRIMARY KEY (tenant_id, year, month)
);

-- Dedup table to prevent double-counting events (idempotency gate)
CREATE TABLE IF NOT EXISTS usage_dedup (
  event_id TEXT NOT NULL,
  tenant_id UUID NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (event_id)
);

-- Auto-purge usage_dedup entries older than 7 days via a scheduled job
CREATE INDEX IF NOT EXISTS usage_dedup_created_at_idx ON usage_dedup(created_at);
