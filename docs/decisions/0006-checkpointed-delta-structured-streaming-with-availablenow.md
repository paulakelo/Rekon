# ADR 0006: Checkpointed Delta Structured Streaming with availableNow Micro-Batching

## Context
Running streaming data pipelines locally on developer laptops or in resource-constrained CI environments poses unique challenges:
1. Continuous streaming jobs occupy CPU threads indefinitely and can cause thread starvation or memory leaks during test execution.
2. Batch-only scripts do not test real-world Structured Streaming semantics (such as checkpoint state tracking, offset commits, and progress logging).

## Decision
We implement all lakehouse pipelines (`streaming/bronze.py`, `streaming/silver.py`, `streaming/gold.py`) using **PySpark Structured Streaming** paired with Delta Lake storage and checkpoint locations, and support two first-class execution modes:
1. **Continuous Mode**: Defaults to standard streaming execution with micro-batch intervals, suitable for live staging or continuous demo monitoring.
2. **Available-Now Mode (`trigger(availableNow=True)`)**: Processes all pending messages currently available in the source (Kafka topic or Delta table) across as many micro-batches as necessary, updates checkpoints, and terminates cleanly when all pending data has been consumed.

## Consequences
- **Positive**: Integration tests (`pytest`) run the exact same Structured Streaming engine, Delta MERGE, and checkpoint logic as production, without hanging or needing arbitrary `sleep()` calls.
- **Positive**: Exactly-once processing guarantees and offset tracking are maintained across runs.
- **Positive**: Enables scheduled batch executions (e.g. hourly cron jobs) alongside continuous streaming deployments using the same codebase.
- **Trade-off**: Requires dedicated checkpoint directories per stream (`checkpoints/bronze/`, `checkpoints/silver/`) which must be wiped when performing full cold replays.
