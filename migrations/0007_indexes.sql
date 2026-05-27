-- Read-path performance indexes

-- Tenants
CREATE INDEX IF NOT EXISTS tenants_plan_id_idx ON tenants(plan_id);
CREATE INDEX IF NOT EXISTS tenants_is_active_idx ON tenants(is_active) WHERE is_active = true;

-- Projects
CREATE INDEX IF NOT EXISTS projects_tenant_id_idx ON projects(tenant_id);
CREATE INDEX IF NOT EXISTS projects_is_archived_idx ON projects(tenant_id, is_archived) WHERE is_archived = false;

-- Monthly usage (PK is already composite; add for reporting query)
CREATE INDEX IF NOT EXISTS monthly_usage_year_month_idx ON monthly_usage(year, month, event_count DESC);

-- Alert rules
CREATE INDEX IF NOT EXISTS alert_rules_project_id_enabled_idx ON alert_rules(project_id, is_enabled) WHERE is_enabled = true;

-- Billing events: GIN on JSONB metadata for containment queries (P5)
-- Use jsonb_path_ops for smaller index; only supports @> operator (all we need)
CREATE INDEX IF NOT EXISTS billing_events_metadata_gin_idx ON billing_events USING GIN (metadata jsonb_path_ops);
CREATE INDEX IF NOT EXISTS billing_events_tenant_occurred_idx ON billing_events(tenant_id, occurred_at DESC);

-- Audit log
CREATE INDEX IF NOT EXISTS audit_log_occurred_at_idx ON audit_log(occurred_at DESC);
