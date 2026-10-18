# Rekon: Streaming Payment Reconciliation Engine

[![CI](https://github.com/paulakelo/rekon/actions/workflows/ci.yml/badge.svg)](https://github.com/paulakelo/rekon/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python: 3.11](https://img.shields.io/badge/Python-3.11-3776AB.svg?logo=python&logoColor=white)](pyproject.toml)
[![Apache Spark: 3.5](https://img.shields.io/badge/Apache_Spark-3.5-E25A1C.svg?logo=apachespark&logoColor=white)](pyproject.toml)
[![Delta Lake: 3.2](https://img.shields.io/badge/Delta_Lake-3.2-00A98F.svg)](pyproject.toml)
[![Redpanda: Kafka](https://img.shields.io/badge/Redpanda-Kafka_API-FF4655.svg)](docker-compose.yml)

> **Portfolio Project 2 of 3 | Streaming Data Engineering**  
> *Kafka + PySpark Structured Streaming + Delta Lake | $0 Stack*  
> Companion project to [dukastack](https://github.com/paulakelo/dukastack) (Retail Analytics Warehouse).

---

## 1. Problem Statement

A Kenyan SME that sells online accepts payments across multiple digital rails:
- **M-Pesa Paybill / Till** (Safaricom Daraja C2B confirmation webhooks)
- **Direct Bank Transfers / RTGS / PesaLink**
- **Other mobile wallets**

At month-end, the finance team must answer:
- *Which orders were actually paid in full?*
- *Which payments are duplicates from upstream retries?*
- *Which payments arrived with no matching order (orphans)?*
- *Which customers underpaid, overpaid, or paid in split installments?*

Today, this is an error-prone spreadsheet exercise. Webhooks arrive **duplicated, late, out-of-order, or inverted** (payment landing before the order is created). A naive batch pipeline produces wrong revenue totals and double-counts receipts.

**Rekon** is a local, event-driven reconciliation engine that:
1. Ingests raw callbacks via a resilient FastAPI edge into Kafka.
2. Streams events through PySpark Structured Streaming into a local **Delta Lake Medallion Architecture** (Bronze $\rightarrow$ Silver $\rightarrow$ Gold).
3. Produces a trustworthy per-order payment status table and reason-coded anomalies table via idempotent Delta `MERGE` operations and event-time watermarking.

---

## 2. Business Questions Answered

| ID | Persona | Question | Served By |
|:---|:---|:---|:---|
| **Q-1** | **Finance Analyst** | What is the payment status of every order right now? | `gold_order_payment_status` |
| **Q-2** | **Finance Analyst** | Which payments or orders look suspicious, and why? | `gold_payment_anomalies` |
| **Q-3** | **Operations Lead** | How many payments per rail per 15 minutes, and how late are they? | `gold_rail_throughput_15m` |
| **Q-4** | **Leadership / CFO** | What was collected versus billed per day? | `gold_reconciliation_daily` |

---

## 3. Architecture & Data Flow

```
  +-------------------------------------------------------------+
  |              Deterministic Simulator (Virtual Clock)         |
  +-------------------------------------------------------------+
        |                                 |                 |
        | Orders                          | M-Pesa          | Bank
        v                                 v                 v
   Kafka Topic                   FastAPI Receiver    FastAPI Receiver
 [orders.created]                 /payments/mpesa     /payments/bank
        |                                 |                 |
        |                                 +--------+--------+
        |                                          |
        |                                          v
        |                                     Kafka Topics
        |                          [payments.mpesa.raw, payments.bank.raw]
        |                                          |
        +--------------------+---------------------+
                             |
                             v
               +---------------------------+
               | PySpark Structured Stream |
               +---------------------------+
                             |
     BRONZE LAYER            v (Streaming Append)
     +-----------------------------------------------------+
     | bronze_orders | bronze_payments                     |
     +-----------------------------------------------------+
                             |
     SILVER LAYER            v (foreachBatch Delta MERGE + Rejects)
     +-----------------------------------------------------+
     | silver_orders | silver_payments | silver_rejects    |
     +-----------------------------------------------------+
                             |
     GOLD LAYER              v (Order Aggregation & 15m Windows)
     +-----------------------------------------------------+
     | gold_order_payment_status | gold_payment_anomalies  |
     | gold_reconciliation_daily | gold_rail_throughput_15m|
     +-----------------------------------------------------+
                             ^
                             | (Periodic Window Expiry & Compaction)
               +---------------------------+
               |      Sweep Batch Job      |
               +---------------------------+
```

### Medallion Layer Design

| Layer | Grain / Schema | Write Pattern | Key Responsibility |
|:---|:---|:---|:---|
| **Bronze** | Raw JSON payloads + Kafka metadata `(topic, partition, offset, ts, ingest_ts)` | Streaming Append | **Zero Loss**: Never drops payloads at the edge, even if malformed. Enables full auditability and topic replay. |
| **Silver** | Normalized canonical payments `(rail, rail_receipt_id, order_ref, amount_kes, paid_at, payer_hash)` | `foreachBatch` Delta `MERGE` | **Idempotency & Quality**: Filters malformed rows into `silver_rejects`, hashes PII (phone numbers), deduplicates receipts idempotently across restarts. |
| **Gold** | Per-order reconciled status, anomalies, daily financial ledger, 15m throughput | Windowed Aggregations & Delta `MERGE` | **Business Truth**: Tracks state machine (`UNPAID` $\rightarrow$ `PARTIAL` $\rightarrow$ `PAID` / `UNDERPAID` / `OVERPAID`), emits reason-coded anomaly rows. |

---

## 4. Key Architectural Decisions

1. **Delta MERGE over Watermark-based `dropDuplicates`**:
   - Spark Structured Streaming's native `dropDuplicates` relies on watermarks. Once state exceeds the watermark duration, the key is evicted from state storage; any subsequent duplicate arriving after that window is falsely re-emitted as a new payment.
   - Rekon uses `foreachBatch` with a Delta Lake `MERGE INTO silver_payments USING ... ON (rail, rail_receipt_id)` `WHEN NOT MATCHED THEN INSERT`. This guarantees strict idempotency even if duplicates arrive days late or across pipeline restarts.
2. **Event-Time Virtual Clock over Wall Clock**:
   - Streaming logic that relies on `current_timestamp()` or wall-clock `now()` breaks determinism during historical replays or test suites.
   - Rekon's simulator implements an accelerated virtual clock (e.g. 1 real second = 10 simulated minutes), and all downstream windows and timeouts evaluate against max-seen event time.
3. **Pure ForeachBatch Order Status Recomputation**:
   - Stream-stream outer joins in PySpark are notoriously fragile when handling late, out-of-order, or multi-payment splits (e.g., partial payment, remainder paid 4 hours later).
   - Rekon maintains orders and payments in Silver Delta tables and recomputes per-order payment balance in micro-batches, supporting arbitrary installment counts and arrival order.

---

## 5. Chaos Scenarios & Resilience Matrix

Rekon is verified against 11 real-world payment edge cases:

| ID | Scenario | Condition | Expected Behavior | Tested By |
|:---|:---|:---|:---|:---|
| **C1** | Duplicate Redelivery | Same receipt delivered 3 times | Exactly 1 row in `silver_payments`; duplicate metric incremented | `tests/scenarios/` |
| **C2** | Duplicate Conflict | Same receipt redelivered with different amount | First amount kept; `DUPLICATE_CONFLICT` anomaly recorded | `tests/scenarios/` |
| **C3** | Inversion / Pre-payment | Payment arrives 2 hours before order creation | Stored in Silver; matches automatically when order arrives; status `PAID` | `tests/scenarios/` |
| **C4** | Late Payment | Payment arrives after nominal 48h window | Matched to order; status updated; `LATE_PAYMENT` anomaly flagged | `tests/scenarios/` |
| **C5** | Orphan Payment | Payment with unknown order reference | Preserved in Silver; flagged as `ORPHAN_PAYMENT` by sweep job | `tests/scenarios/` |
| **C6** | Split Payment | Two distinct payments sum to order total | Transitions `PARTIAL` $\rightarrow$ `PAID`, `payment_count = 2` | `tests/scenarios/` |
| **C7** | Partial Underpayment | Customer pays 70% and never completes | `PARTIAL` during window; finalized as `UNDERPAID` by sweep job | `tests/scenarios/` |
| **C8** | Overpayment | Customer pays 125% of order total | Status marked `OVERPAID`, flagged with `OVERPAID` anomaly | `tests/scenarios/` |
| **C9** | Malformed Payload | Corrupted JSON or missing critical fields | Captured raw in Bronze; routed to `silver_rejects` with reason | `tests/scenarios/` |
| **C10**| Worker Crash | Spark killed mid-microbatch | Recovers from Delta ACID checkpoint; zero loss, zero duplicates | `tests/integration/`|
| **C11**| Checkpoint Wipe | Checkpoints cleared, topics replayed | Bit-identical Gold output reproduced | `make replay` |

---

## 6. Getting Started

### Prerequisites
- Linux or macOS
- Python 3.11 (`uv` recommended)
- Java 11 or 17 (for PySpark)
- Docker & Docker Compose

### Quickstart

```bash
# 1. Clone the repository
git clone https://github.com/paulakelo/rekon.git
cd rekon

# 2. Setup virtual environment & dependencies
make setup

# 3. Start local Kafka (Redpanda) & FastAPI webhook receiver
make up

# 4. Run the end-to-end demo (generates orders, payments & chaos events)
make demo

# 5. Inspect reconciliation and anomaly reports
make report

# 6. Run automated test suite
make test

# 7. Stop infrastructure
make down
```

---

## 7. Connection to Dukastack

Rekon forms **Portfolio Project 2 of 3** in Paula's data engineering series. It is directly connected to [dukastack](https://github.com/paulakelo/dukastack):
- **Shared Order Reference Scheme**: Both projects standardize on the `ORD-xxxxxxxx` order reference format.
- **Billed vs. Collected Reconciliation**: While `dukastack` models point-of-sale operational transactions and dimensional revenue metrics, `rekon` solves the external payment verification problem (closing the loop between POS invoices and digital cash collected).
