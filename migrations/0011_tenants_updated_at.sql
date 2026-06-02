-- tenants.updated_at was missing from the original schema but referenced by PATCH /tenants.
-- Every PATCH would fail with "column updated_at does not exist" in production.
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
