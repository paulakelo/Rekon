# Architecture & Lakehouse Design

## 1. Executive Summary

Rekon is an event-driven payment reconciliation lakehouse designed for digital commerce businesses operating in emerging markets (specifically Kenya). It reconciles real-time webhook streams from M-Pesa (Safaricom Daraja C2B confirmation API) and commercial bank payment gateways against ecommerce/POS orders.

The pipeline implements the **Medallion Architecture (Bronze -> Silver -> Gold)** using **PySpark Structured Streaming** and **Delta Lake**, running on a $0 local stack powered by Redpanda (Kafka-compatible broker).

---

## 2. Ingestion Tier: FastAPI & Redpanda

Payment callbacks from external rails are received by an asynchronous FastAPI gateway:
- `POST /api/v1/payments/mpesa`: Receives Safaricom Daraja C2B confirmations. Immediately acknowledges with `{"ResultCode": 0, "ResultDesc": "Accepted"}`.
- `POST /api/v1/payments/bank`: Receives bank/aggregator settlement webhooks. Immediately acknowledges with `{"status": "received"}`.

### Zero-Loss Edge Philosophy
The edge receiver does **not** validate business rules, parse complex payment logic, or discard unparseable JSON. It writes the exact raw string body along with transport metadata to dedicated Kafka topics:
- `payments.mpesa.raw`
- `payments.bank.raw`
- `orders.created`

This decouples webhook durability from processing latency. If downstream Spark workers are restarting or backlogged, webhooks are safely persisted in Redpanda's commit log.

---

## 3. Storage & Transformation Tier: Medallion Lakehouse

```
Kafka Topics
  payments.mpesa.raw
  payments.bank.raw
  orders.created
       │
       ▼ (PySpark Structured Streaming - Continuous Ingestion)
[ BRONZE TABLES ]
  bronze_payments
  bronze_orders
       │
       ▼ (foreachBatch - Parse JSON, Normalize, Hash PII, Delta MERGE)
[ SILVER TABLES ]
  silver_payments ──── (Invalid payloads) ────► silver_rejects
  silver_orders
       │
       ▼ (Micro-batch aggregation & Watermarked windows)
[ GOLD TABLES ]
  gold_order_payment_status ◄── (Periodic sweep for window timeout)
  gold_payment_anomalies
  gold_reconciliation_daily
  gold_rail_throughput_15m
```

### Bronze Layer
- **Write Pattern**: Streaming append with checkpointing.
- **Schema**: Kafka topic, partition, offset, timestamp, ingest timestamp, and unparsed raw payload string.
- **Value**: Guaranteed audit log. If a bug is found in the Silver parser 6 months later, Bronze can be replayed to regenerate the exact business state without re-querying external payment rails.

### Silver Layer
- **Write Pattern**: `foreachBatch` with ACID Delta Lake `MERGE`.
- **Primary Key**: `(rail, rail_receipt_id)` for payments; `order_ref` for orders.
- **PII Protection**: Customer MSISDN (phone number) and bank account numbers are cryptographically salted and hashed using SHA-256 before storage in Silver.
- **Dead-Letter Handling**: Any payload with invalid JSON or missing mandatory keys lands in `silver_rejects` with a structured `reject_reason`.

### Gold Layer
- **Order Status Mart (`gold_order_payment_status`)**:
  - Grain: One row per order (`order_ref`).
  - Fields: `order_amount_kes`, `paid_total_kes`, `payment_count`, `status`, `first_paid_at`, `last_paid_at`, `settled_at`.
  - Status Rules:
    - `paid_total == 0`: `UNPAID`
    - `0 < paid_total < order_amount` (within window): `PARTIAL`
    - `0 < paid_total < order_amount` (after window expires): `UNDERPAID`
    - `paid_total == order_amount`: `PAID`
    - `paid_total > order_amount`: `OVERPAID`
- **Anomalies Mart (`gold_payment_anomalies`)**:
  - Captures reason-coded issues: `DUPLICATE_CONFLICT`, `ORPHAN_PAYMENT`, `LATE_PAYMENT`, `UNDERPAID`, `OVERPAID`, `MALFORMED_PAYLOAD`.
- **Daily Reconciliation Mart (`gold_reconciliation_daily`)**:
  - Aggregates daily billed vs. collected cash and order counts by final status.
- **Rail Throughput Mart (`gold_rail_throughput_15m`)**:
  - Event-time 15-minute sliding/tumbling windows with watermark calculating transaction volume, total cash, and p50/p95 latency per rail.

---

## 4. Maintenance & Sweeps

A companion sweep job executes periodically to:
1. Identify expired payment windows (default 48 simulated hours) and transition lingering `PARTIAL` orders to `UNDERPAID`.
2. Detect orphan payments whose references never matched any order.
3. Perform Delta Lake storage maintenance (`OPTIMIZE` small files and `VACUUM` expired tombstones).
