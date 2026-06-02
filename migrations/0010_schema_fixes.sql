-- Add updated_at to projects (referenced by archive, key-rotation, and list routes but missing from schema)
ALTER TABLE projects ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

-- Add is_platform_admin to users for platform-level admin endpoint authorization.
-- Tenant owners/admins must NOT have access to cross-tenant admin routes.
ALTER TABLE users ADD COLUMN IF NOT EXISTS is_platform_admin BOOLEAN NOT NULL DEFAULT false;

-- app_user is only ever activated via SET LOCAL ROLE inside a transaction; it never
-- opens a direct connection. Removing login capability neutralises the hardcoded
-- password committed in migration 0005 without affecting any application behaviour.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_user') THEN
    ALTER ROLE app_user NOLOGIN;
  END IF;
END;
$$;
