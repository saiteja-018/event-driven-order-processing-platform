-- Create dead_letter_messages table and webhook_delivery_failures
CREATE TABLE IF NOT EXISTS public.dead_letter_messages (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  original_topic VARCHAR(255),
  original_partition INTEGER,
  original_offset VARCHAR(50),
  failed_at TIMESTAMPTZ,
  error_message TEXT,
  retry_count INTEGER,
  payload JSONB,
  resolved BOOLEAN DEFAULT false,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_service.webhook_delivery_failures (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id UUID,
  payload JSONB,
  attempts INTEGER,
  last_error TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);
