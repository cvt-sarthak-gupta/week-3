-- Update the alert_rules RLS policy to use the denormalized tenant_id column
-- added in migration 0008 instead of the project_id subquery.
--
-- Before: USING (project_id IN (SELECT id FROM projects WHERE tenant_id = ...))
-- After:  USING (tenant_id = current_setting(...)::UUID)
--
-- The new form avoids a correlated subquery on every row evaluation, which
-- matters at scale when queries scan many alert_rules rows.

DROP POLICY IF EXISTS alert_rules_tenant_isolation ON alert_rules;

CREATE POLICY alert_rules_tenant_isolation ON alert_rules
  USING (tenant_id = (current_setting('app.current_tenant_id', true))::UUID);
