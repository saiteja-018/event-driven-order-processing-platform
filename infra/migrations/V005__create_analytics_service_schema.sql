CREATE SCHEMA IF NOT EXISTS analytics_service;
CREATE ROLE IF NOT EXISTS analytics_service_role NOLOGIN;
GRANT USAGE ON SCHEMA analytics_service TO analytics_service_role;

CREATE TABLE IF NOT EXISTS analytics_service.order_events_log (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_type VARCHAR(100) NOT NULL,
  order_id UUID,
  user_id UUID,
  payload JSONB NOT NULL,
  kafka_offset BIGINT,
  kafka_partition INTEGER,
  kafka_topic VARCHAR(255),
  processed_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS analytics_service.hourly_order_metrics (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  hour_bucket TIMESTAMPTZ NOT NULL,
  total_orders INTEGER NOT NULL DEFAULT 0,
  total_revenue NUMERIC(14,2) NOT NULL DEFAULT 0,
  successful_orders INTEGER NOT NULL DEFAULT 0,
  failed_orders INTEGER NOT NULL DEFAULT 0,
  avg_order_value NUMERIC(12,2),
  computed_at TIMESTAMPTZ DEFAULT now()
);

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA analytics_service TO analytics_service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics_service GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO analytics_service_role;
