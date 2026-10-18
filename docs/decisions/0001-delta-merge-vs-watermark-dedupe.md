# ADR 0001: Deduplication via Delta Lake MERGE instead of Watermark dropDuplicates

## Context
Payment gateways frequently retry callback notifications when they do not receive an immediate 200 HTTP response, or when network timeouts occur. In production, duplicate callbacks may arrive seconds, minutes, or even days after the original event.

Apache Spark Structured Streaming provides a built-in `dropDuplicates(["rail", "rail_receipt_id"])` operation. However, in streaming queries, `dropDuplicates` requires a watermark to evict old keys from state memory (e.g. `withWatermark("paid_at", "2 hours")`). Once a watermark passes, state for old receipts is evicted. Any late duplicate arriving after the watermark window will be treated as an entirely new transaction, causing double-counting in downstream financial metrics.

## Decision
We deduplicate payments by persisting Silver payments into Delta Lake tables and performing an idempotent Delta Lake `MERGE` inside `foreachBatch`:

```sql
MERGE INTO silver_payments AS target
USING microbatch_payments AS source
ON target.rail = source.rail AND target.rail_receipt_id = source.rail_receipt_id
WHEN NOT MATCHED THEN INSERT *
```

If a duplicate arrives with an identical receipt ID but conflicting amount, the merge logic flags the conflict as an anomaly row in `gold_payment_anomalies` rather than silently overwriting the original verified receipt.

## Consequences
- **Positive**: Exact-once state preservation survives arbitrary time delays, pipeline restarts, and cold replays.
- **Positive**: No state store memory ballooning inside PySpark memory.
- **Trade-off**: Requires ACID Delta Lake storage operations per micro-batch, introducing a small merge overhead compared to in-memory state stores. On local laptop mode, Delta MERGE on modest batch sizes completes in under 200ms.
