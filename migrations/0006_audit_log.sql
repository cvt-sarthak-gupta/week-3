CREATE TABLE IF NOT EXISTS audit_log (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  table_name TEXT NOT NULL,
  operation TEXT NOT NULL CHECK (operation IN ('UPDATE', 'DELETE')),
  old_row JSONB,
  new_row JSONB,
  tenant_id UUID,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS audit_log_tenant_id_idx ON audit_log(tenant_id);
CREATE INDEX IF NOT EXISTS audit_log_table_name_idx ON audit_log(table_name, occurred_at DESC);

-- Generic audit trigger function
CREATE OR REPLACE FUNCTION audit_trigger_fn()
RETURNS TRIGGER AS $$
DECLARE
  tenant_id_val UUID;
BEGIN
  -- Gracefully handle missing session var (e.g., during migrations)
  BEGIN
    tenant_id_val := current_setting('app.current_tenant_id', true)::UUID;
  EXCEPTION WHEN OTHERS THEN
    tenant_id_val := NULL;
  END;

  IF TG_OP = 'DELETE' THEN
    INSERT INTO audit_log(table_name, operation, old_row, new_row, tenant_id)
    VALUES (TG_TABLE_NAME, TG_OP, row_to_json(OLD)::JSONB, NULL, tenant_id_val);
    RETURN OLD;
  ELSIF TG_OP = 'UPDATE' THEN
    INSERT INTO audit_log(table_name, operation, old_row, new_row, tenant_id)
    VALUES (TG_TABLE_NAME, TG_OP, row_to_json(OLD)::JSONB, row_to_json(NEW)::JSONB, tenant_id_val);
    RETURN NEW;
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Attach to tenants, users, projects
DROP TRIGGER IF EXISTS tenants_audit ON tenants;
CREATE TRIGGER tenants_audit
  AFTER UPDATE OR DELETE ON tenants
  FOR EACH ROW EXECUTE FUNCTION audit_trigger_fn();

DROP TRIGGER IF EXISTS users_audit ON users;
CREATE TRIGGER users_audit
  AFTER UPDATE OR DELETE ON users
  FOR EACH ROW EXECUTE FUNCTION audit_trigger_fn();

DROP TRIGGER IF EXISTS projects_audit ON projects;
CREATE TRIGGER projects_audit
  AFTER UPDATE OR DELETE ON projects
  FOR EACH ROW EXECUTE FUNCTION audit_trigger_fn();
