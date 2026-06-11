CREATE SCHEMA IF NOT EXISTS payment_service;
CREATE ROLE IF NOT EXISTS payment_service_role NOLOGIN;
GRANT USAGE ON SCHEMA payment_service TO payment_service_role;

CREATE TABLE IF NOT EXISTS payment_service.payment_transactions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id UUID NOT NULL,
  amount NUMERIC(12,2) NOT NULL,
  currency CHAR(3) NOT NULL,
  status VARCHAR(50) NOT NULL DEFAULT 'INITIATED',
  provider_reference VARCHAR(255),
  failure_reason TEXT,
  retry_count INTEGER NOT NULL DEFAULT 0,
  idempotency_key VARCHAR(255) UNIQUE NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA payment_service TO payment_service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA payment_service GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO payment_service_role;
