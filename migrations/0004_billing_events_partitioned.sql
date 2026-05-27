CREATE TABLE IF NOT EXISTS billing_events (
  id UUID NOT NULL DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL,
  event_type TEXT NOT NULL,
  amount_cents INT NOT NULL DEFAULT 0,
  metadata JSONB NOT NULL DEFAULT '{}',
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT billing_events_pkey PRIMARY KEY (id, occurred_at)
) PARTITION BY RANGE (occurred_at);

-- Create or replace the helper function
CREATE OR REPLACE FUNCTION ensure_billing_partition(p_year INT, p_month INT)
RETURNS VOID AS $$
DECLARE
  partition_name TEXT;
  start_date DATE;
  end_date DATE;
BEGIN
  partition_name := 'billing_events_' || LPAD(p_year::TEXT, 4, '0') || '_' || LPAD(p_month::TEXT, 2, '0');
  start_date := DATE(p_year || '-' || LPAD(p_month::TEXT, 2, '0') || '-01');
  -- End date: first day of next month
  end_date := start_date + INTERVAL '1 month';

  -- Only create if not exists
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = partition_name AND n.nspname = 'public'
  ) THEN
    EXECUTE format(
      'CREATE TABLE %I PARTITION OF billing_events FOR VALUES FROM (%L) TO (%L)',
      partition_name,
      start_date,
      end_date
    );
    RAISE NOTICE 'Created partition %', partition_name;
  END IF;
END;
$$ LANGUAGE plpgsql;

-- Create initial partitions (current month + next 3)
DO $$
DECLARE
  d DATE := DATE_TRUNC('month', NOW());
  i INT;
BEGIN
  FOR i IN 0..3 LOOP
    PERFORM ensure_billing_partition(
      EXTRACT(YEAR FROM d + (i || ' months')::INTERVAL)::INT,
      EXTRACT(MONTH FROM d + (i || ' months')::INTERVAL)::INT
    );
  END LOOP;
END;
$$;
