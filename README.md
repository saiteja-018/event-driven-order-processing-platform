# Event-Driven Order Processing Platform

A production-grade, distributed, event-driven order processing platform built with Apache Kafka (KRaft mode), Redis, PostgreSQL, and AWS LocalStack (SQS/SNS/Lambda). Implements saga orchestration, optimistic locking, dead letter queues, and circuit breakers.

## Architecture

```
                    ┌─────────────┐
                    │ API Gateway │  :3000
                    │  (public)   │
                    └──────┬──────┘
           ┌───────────────┼───────────────┐
           ▼               ▼               ▼
   ┌──────────────┐ ┌───────────────┐ ┌────────────────┐
   │Order Service │ │Inventory Svc  │ │Analytics Svc   │
   │   :3001      │ │  :3002        │ │  :3005         │
   └──────┬───────┘ └───────┬───────┘ └────────────────┘
          │                 │
          └─────────────────┴──── Apache Kafka (KRaft)
                                         │
                    ┌────────────────────┼──────────────┐
                    ▼                    ▼              ▼
           ┌────────────────┐  ┌───────────────┐  ┌──────────────┐
           │Payment Service │  │Notification   │  │LocalStack    │
           │  :3003         │  │Svc :3004      │  │SQS/SNS/Lambda│
           └────────────────┘  └───────────────┘  └──────────────┘
```

### Services

| Service | Port | Purpose |
|---------|------|---------|
| api-gateway | 3000 | Public HTTP gateway, rate limiting, circuit breaker |
| order-service | 3001 | Order lifecycle management, state machine |
| inventory-service | 3002 | Stock reservations with optimistic locking |
| payment-service | 3003 | Payment initiation via SQS → Lambda |
| notification-service | 3004 | Email/SMS/Push notifications via Kafka + SQS |
| analytics-service | 3005 | Event log, hourly metrics |

## Quick Start

### Prerequisites
- Docker Desktop (with Compose V2)
- 8GB RAM available for Docker

### 1. Start the platform

```powershell
cd C:\GPP\event-driven-order-processing-platform
docker compose up --build
```

This starts all services including:
- PostgreSQL (port 5432)
- Redis (port 6379)
- Kafka KRaft (port 29092 for host access, 9092 internally)
- LocalStack (port 4566) — SQS, SNS, Lambda, EventBridge
- All 6 microservices
- A `migrate` service that auto-runs all DB migrations and seed data

Wait until you see `LocalStack init complete` and all services log `listening` before testing.

### 2. Verify the system

```powershell
# Health check
curl http://localhost:3000/api/v1/health

# List products
curl http://localhost:3000/api/v1/products

# Create an order (replace <PRODUCT_ID> with one from /api/v1/products)
curl -X POST http://localhost:3000/api/v1/orders `
  -H "Content-Type: application/json" `
  -d '{
    "userId": "550e8400-e29b-41d4-a716-446655440001",
    "items": [{"productId": "<PRODUCT_ID>", "quantity": 1, "unitPrice": 99.99}],
    "totalAmount": 99.99,
    "currency": "USD",
    "idempotencyKey": "test-order-001"
  }'
```

### 3. Run integration tests

```powershell
cd tests
npm install
npm test
```

Or use the helper script:
```powershell
.\scripts\run-integration.ps1
```

## Database Migrations

Migrations run automatically via the `migrate` Docker Compose service. To run manually:

```powershell
# All migrations + seed in one go via Docker
docker exec -it <postgres-container-id> bash -c "
  psql -U postgres -d app -f /dev/stdin
"

# Or via psql locally
$env:PGPASSWORD = "postgres"
Get-ChildItem infra/migrations/V*.sql | Sort-Object Name | ForEach-Object {
    Write-Host "Running $($_.Name)"
    psql -h localhost -U postgres -d app -f $_.FullName
}
psql -h localhost -U postgres -d app -f infra/migrations/seed.sql
```

## Key Patterns Implemented

### Dual-Write Problem and Mitigation

**Risk**: Writing to PostgreSQL and then publishing to Kafka are two independent operations. If the service crashes between them, the DB write succeeds but the Kafka event is never published.

**Current Mitigation (documented):**
- The codebase minimizes the dual-write window by: (1) committing the DB transaction, (2) immediately publishing to Kafka, and (3) only storing the idempotency response in Redis after both succeed.
- If a crash occurs between DB commit and Kafka publish, the order is created in DB but no downstream events fire. On retry (via idempotency key), the client gets back the cached response without re-publishing.

**Recommended Production Mitigation — Transactional Outbox Pattern:**
1. Add `order_service.outbox_messages` table: `(id UUID PK, aggregate_id UUID, topic VARCHAR, payload JSONB, published BOOLEAN DEFAULT false, created_at TIMESTAMPTZ, published_at TIMESTAMPTZ)`
2. In `createOrder`, write the order row AND the outbox row within the **same DB transaction**
3. Run a background publisher that polls `outbox_messages WHERE published = false`, publishes to Kafka, and marks `published = true`
4. This guarantees at-least-once delivery without distributed transactions

### Optimistic Locking (Inventory Service)

Stock reservations use a `version` column on the `products` table:
```sql
UPDATE inventory_service.products
SET reserved_stock = reserved_stock + $quantity,
    version = version + 1
WHERE id = $productId AND version = $expectedVersion
```
If `rowCount = 0`, another process modified the row concurrently. The service retries up to 3 times with 50ms delay.

### Circuit Breaker (API Gateway)

Redis-backed, per-downstream-service state machine:
- `circuit:{service}:state` → CLOSED / OPEN / HALF_OPEN
- `circuit:{service}:failures` → integer counter
- Thresholds: 5 failures → OPEN, 30s timeout → HALF_OPEN, 1 probe success → CLOSED

### Consumer Idempotency

Every Kafka consumer tracks processed event IDs:
- Key: `processed_event:{consumerGroup}:{topic}:{eventId}`
- TTL: 3600 seconds
- On duplicate delivery: skip processing, commit offset

### Dead Letter Queue

Failed messages (after 3 retries with exponential backoff: 1s, 2s, 4s) are published to `dead.letter.queue` Kafka topic with an envelope:
```json
{
  "originalTopic": "...",
  "originalPartition": 0,
  "originalOffset": "42",
  "failedAt": "2024-01-01T00:00:00Z",
  "errorMessage": "...",
  "retryCount": 3,
  "payload": {}
}
```
The analytics service consumes `dead.letter.queue` and persists to `public.dead_letter_messages`.

## Environment Variables

Each service has a `.env.example` file. Key variables:

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | PostgreSQL connection string |
| `REDIS_URL` | Redis connection URL |
| `KAFKA_BROKER` | Kafka bootstrap server (e.g. `kafka:9092`) |
| `LOCALSTACK_URL` | LocalStack endpoint (e.g. `http://localstack:4566`) |
| `PORT` | HTTP server port |

## Lambda Functions

Lambdas run inside LocalStack and are auto-deployed by `infra/localstack/init.sh`:

| Function | Trigger | Purpose |
|----------|---------|---------|
| `process-payment` | SQS `payment-processing-queue` | Simulates payment (>10k always fails, 5k-10k 30% fail) |
| `expire-reservations` | EventBridge every 5 min | Releases expired stock reservations |
| `compute-hourly-metrics` | EventBridge top of hour | Aggregates order metrics |

Lambda environment vars injected: `DATABASE_URL`, `KAFKA_BROKERS`, `LOCALSTACK_URL`

## API Reference

### Orders
| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/orders` | Create order (idempotent via `idempotencyKey`) |
| `GET` | `/api/v1/orders/:orderId` | Get order by ID |
| `GET` | `/api/v1/orders/user/:userId` | List orders for user |
| `DELETE` | `/api/v1/orders/:orderId` | Cancel order |

### Products
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/products` | List products (`inStockOnly`, `page`, `limit`) |
| `GET` | `/api/v1/products/:productId` | Get product with stock levels |

### Analytics
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/analytics/metrics` | Hourly metrics (`from`, `to` ISO8601, max 7 days) |
| `GET` | `/api/v1/analytics/events` | Events log by `orderId` |

### Health
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/health` | Aggregate health of all downstream services |

## Order State Machine

```
PENDING ──► CONFIRMED ──► PAYMENT_PROCESSING ──► COMPLETED (terminal)
   │             │                 │
   └──► CANCELLED◄┘                └──► PAYMENT_FAILED ──► CANCELLED (terminal)
                                              │
                                              └──► PAYMENT_PROCESSING (retry, max 3)
```
