-- Add tenant_id and updated_at columns to alert_rules.
-- tenant_id is a denormalised FK (project → tenant) that lets the domain layer
-- scope withTenant() without an extra join, and lets RLS policies be expressed
-- directly on the table.
-- updated_at tracks the last time a rule was modified.

ALTER TABLE alert_rules
  ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) ON DELETE CASCADE,
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

-- Back-fill tenant_id from the projects table for existing rows.
UPDATE alert_rules ar
SET tenant_id = p.tenant_id
FROM projects p
WHERE p.id = ar.project_id
  AND ar.tenant_id IS NULL;

-- Make tenant_id NOT NULL now that the back-fill is done.
ALTER TABLE alert_rules
  ALTER COLUMN tenant_id SET NOT NULL;

-- Index to support tenant-scoped queries (e.g. list all rules for a tenant).
CREATE INDEX IF NOT EXISTS alert_rules_tenant_id_idx ON alert_rules(tenant_id);
