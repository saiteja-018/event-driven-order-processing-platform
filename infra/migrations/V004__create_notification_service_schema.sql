CREATE SCHEMA IF NOT EXISTS notification_service;
CREATE ROLE IF NOT EXISTS notification_service_role NOLOGIN;
GRANT USAGE ON SCHEMA notification_service TO notification_service_role;

CREATE TABLE IF NOT EXISTS notification_service.notification_logs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL,
  order_id UUID,
  channel VARCHAR(50) NOT NULL,
  template_key VARCHAR(100) NOT NULL,
  payload JSONB NOT NULL,
  status VARCHAR(50) NOT NULL DEFAULT 'QUEUED',
  sent_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now()
);

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA notification_service TO notification_service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA notification_service GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO notification_service_role;
