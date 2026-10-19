# 7. Periodic Sweep Finalization, Orphan Detection, and Storage Compaction (OPTIMIZE)

Date: 2026-10-19

## Status

Accepted

## Context

In high-throughput event-driven reconciliation, real-time streaming engines operate on narrow micro-batch horizons to maintain sub-second to low-minute latency. However, two fundamental accounting and storage challenges emerge:

1. **Window Expiry & Finalization**: Orders partially paid (`PARTIAL`) cannot linger indefinitely in active state. In retail and digital commerce, payment windows expire (e.g., 24h–48h). Once expired, partial payments must be formally finalized as `UNDERPAID` for warehouse accounting and partial fulfillment resolution. Similarly, payments arriving for unrecognized orders must be captured as `ORPHAN_PAYMENT` exceptions rather than dropped or left unallocated.
2. **Small File Problem in Streaming Delta Tables**: Frequent micro-batch writes append small Parquet files into the underlying Delta Lake storage directories. Over time, excessive metadata operations and small-file reads degrade query performance for business analysts and downstream reporting marts.

## Decision

We decouple real-time stream ingestion from nominal window expiry and storage maintenance by implementing an explicit periodic batch sweep job (`streaming/sweep.py`):

1. **State Machine Expiry via Event-Time Sweeps**: The sweep evaluates active orders against nominal expiry thresholds. Orders in `PARTIAL` status past the window are transitioned to `UNDERPAID`. Payments without corresponding orders are isolated into `gold_payment_anomalies` as `ORPHAN_PAYMENT`.
2. **Delta Lake Compaction (`OPTIMIZE`)**: The sweep job executes `OPTIMIZE delta.<path>` across all Bronze, Silver, and Gold Delta tables to merge small files into optimal size files without halting active writers.
3. **Decoupled Checkpointing**: The sweep job operates directly on the Delta tables without mutating or resetting streaming consumer checkpoints, preserving continuous Kafka stream ingestion.

## Consequences

### Positive
- **Clean Ledger Closure**: Financial accounting ledgers reflect finalized balances rather than perpetually pending partial orders.
- **Auditability**: Every orphan payment and underpayment is deterministically recorded with full lineage and reason codes in `gold_payment_anomalies`.
- **Sustained Query Performance**: Delta `OPTIMIZE` prevents read latency degradation on an 8 GB single-node deployment.
- **Idempotent Replay**: Running the sweep as part of cold replay (`verify_replay`) guarantees identical terminal Gold table states.

### Negative / Trade-offs
- The sweep requires invoking a Spark session periodically, temporarily increasing CPU and memory load. On an 8 GB laptop, running the sweep sequentially after active streaming batches avoids resource contention.
