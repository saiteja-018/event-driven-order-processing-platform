-- Add unique constraint on hour_bucket for upsert support in hourly_order_metrics
ALTER TABLE analytics_service.hourly_order_metrics
  ADD CONSTRAINT IF NOT EXISTS uq_hourly_order_metrics_hour_bucket UNIQUE (hour_bucket);

-- Ensure pgcrypto is available in all schemas (needed for gen_random_uuid)
CREATE EXTENSION IF NOT EXISTS pgcrypto;
