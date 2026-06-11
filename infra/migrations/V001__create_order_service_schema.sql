-- Create order_service schema and tables
CREATE SCHEMA IF NOT EXISTS order_service;
CREATE ROLE IF NOT EXISTS order_service_role NOLOGIN;
GRANT USAGE ON SCHEMA order_service TO order_service_role;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS order_service.orders (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL,
  status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
  total_amount NUMERIC(12,2) NOT NULL,
  currency CHAR(3) NOT NULL DEFAULT 'USD',
  metadata JSONB DEFAULT '{}',
  idempotency_key VARCHAR(255) UNIQUE NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_service.order_items (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id UUID NOT NULL REFERENCES order_service.orders(id),
  product_id UUID NOT NULL,
  quantity INTEGER NOT NULL CHECK (quantity > 0),
  unit_price NUMERIC(12,2) NOT NULL,
  reserved BOOLEAN NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS order_service.order_status_history (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id UUID NOT NULL REFERENCES order_service.orders(id),
  from_status VARCHAR(50),
  to_status VARCHAR(50) NOT NULL,
  reason TEXT,
  transitioned_at TIMESTAMPTZ DEFAULT now()
);

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA order_service TO order_service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA order_service GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO order_service_role;
