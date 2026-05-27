-- Create the app_user role (IF NOT EXISTS guard)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_user') THEN
    CREATE ROLE app_user LOGIN PASSWORD 'app_user_password';
  END IF;
END;
$$;

-- Grant schema access
GRANT USAGE ON SCHEMA public TO app_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO app_user;

-- Enable RLS on tenant-scoped tables
ALTER TABLE projects ENABLE ROW LEVEL SECURITY;
ALTER TABLE alert_rules ENABLE ROW LEVEL SECURITY;
ALTER TABLE billing_events ENABLE ROW LEVEL SECURITY;

-- Force RLS even for table owners
ALTER TABLE projects FORCE ROW LEVEL SECURITY;
ALTER TABLE alert_rules FORCE ROW LEVEL SECURITY;
ALTER TABLE billing_events FORCE ROW LEVEL SECURITY;

-- projects: allow access only to rows belonging to current tenant
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'projects' AND policyname = 'projects_tenant_isolation') THEN
    CREATE POLICY projects_tenant_isolation ON projects
      USING (tenant_id = current_setting('app.current_tenant_id', true)::UUID);
  END IF;
END $$;

-- alert_rules: join through projects
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'alert_rules' AND policyname = 'alert_rules_tenant_isolation') THEN
    CREATE POLICY alert_rules_tenant_isolation ON alert_rules
      USING (
        project_id IN (
          SELECT id FROM projects
          WHERE tenant_id = current_setting('app.current_tenant_id', true)::UUID
        )
      );
  END IF;
END $$;

-- billing_events
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'billing_events' AND policyname = 'billing_events_tenant_isolation') THEN
    CREATE POLICY billing_events_tenant_isolation ON billing_events
      USING (tenant_id = current_setting('app.current_tenant_id', true)::UUID);
  END IF;
END $$;

-- Superuser bypass for migrations (postgres user)
ALTER TABLE projects OWNER TO postgres;
ALTER TABLE alert_rules OWNER TO postgres;
ALTER TABLE billing_events OWNER TO postgres;
