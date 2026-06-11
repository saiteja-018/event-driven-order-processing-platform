-- Seed products into inventory_service.products
INSERT INTO inventory_service.products (id, sku, name, total_stock, reserved_stock, reorder_threshold)
VALUES
  (gen_random_uuid(), 'SKU-A001', 'Product A001', 50, 0, 10),
  (gen_random_uuid(), 'SKU-A002', 'Product A002', 0, 0, 5),
  (gen_random_uuid(), 'SKU-A003', 'Product A003', 8, 0, 5),
  (gen_random_uuid(), 'SKU-A004', 'Product A004', 100, 0, 10),
  (gen_random_uuid(), 'SKU-A005', 'Product A005', 3, 0, 5),
  (gen_random_uuid(), 'SKU-A006', 'Product A006', 12, 0, 10),
  (gen_random_uuid(), 'SKU-A007', 'Product A007', 1, 0, 5),
  (gen_random_uuid(), 'SKU-A008', 'Product A008', 25, 0, 8),
  (gen_random_uuid(), 'SKU-A009', 'Product A009', 0, 0, 3),
  (gen_random_uuid(), 'SKU-A010', 'Product A010', 60, 0, 15),
  (gen_random_uuid(), 'SKU-A011', 'Product A011', 5, 0, 5),
  (gen_random_uuid(), 'SKU-A012', 'Product A012', 9, 0, 10),
  (gen_random_uuid(), 'SKU-A013', 'Product A013', 2, 0, 5),
  (gen_random_uuid(), 'SKU-A014', 'Product A014', 80, 0, 12),
  (gen_random_uuid(), 'SKU-A015', 'Product A015', 0, 0, 6),
  (gen_random_uuid(), 'SKU-A016', 'Product A016', 7, 0, 5),
  (gen_random_uuid(), 'SKU-A017', 'Product A017', 14, 0, 7),
  (gen_random_uuid(), 'SKU-A018', 'Product A018', 4, 0, 5),
  (gen_random_uuid(), 'SKU-A019', 'Product A019', 30, 0, 10),
  (gen_random_uuid(), 'SKU-A020', 'Product A020', 11, 0, 8)
ON CONFLICT (sku) DO NOTHING;
