CREATE TABLE IF NOT EXISTS alert_rules (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  condition_type TEXT NOT NULL CHECK (condition_type IN ('threshold', 'anomaly', 'keyword')),
  threshold NUMERIC,
  window_seconds INT NOT NULL DEFAULT 300,
  notification_channel TEXT NOT NULL,
  is_enabled BOOLEAN NOT NULL DEFAULT true,
  last_triggered_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  -- Stores the Elasticsearch query for percolation
  es_query JSONB NOT NULL DEFAULT '{}',
  CONSTRAINT alert_rules_project_name_unique UNIQUE (project_id, name)
);
