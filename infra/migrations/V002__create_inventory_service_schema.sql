-- Create inventory_service schema and tables
CREATE SCHEMA IF NOT EXISTS inventory_service;
CREATE ROLE IF NOT EXISTS inventory_service_role NOLOGIN;
GRANT USAGE ON SCHEMA inventory_service TO inventory_service_role;

CREATE TABLE IF NOT EXISTS inventory_service.products (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  sku VARCHAR(100) UNIQUE NOT NULL,
  name VARCHAR(255) NOT NULL,
  total_stock INTEGER NOT NULL CHECK (total_stock >= 0),
  reserved_stock INTEGER NOT NULL DEFAULT 0 CHECK (reserved_stock >= 0),
  available_stock INTEGER GENERATED ALWAYS AS (total_stock - reserved_stock) STORED,
  reorder_threshold INTEGER NOT NULL DEFAULT 10,
  version INTEGER NOT NULL DEFAULT 1,
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS inventory_service.stock_reservations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id UUID NOT NULL,
  product_id UUID NOT NULL REFERENCES inventory_service.products(id),
  quantity INTEGER NOT NULL,
  status VARCHAR(50) NOT NULL DEFAULT 'ACTIVE',
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS inventory_service.stock_movements (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  product_id UUID NOT NULL REFERENCES inventory_service.products(id),
  movement_type VARCHAR(50) NOT NULL,
  quantity_delta INTEGER NOT NULL,
  reference_id UUID,
  reference_type VARCHAR(100),
  created_at TIMESTAMPTZ DEFAULT now()
);

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA inventory_service TO inventory_service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA inventory_service GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO inventory_service_role;
