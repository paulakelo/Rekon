# ADR 0002: Event-Time Semantics and Virtual Clock over Wall-Clock Time

## Context
Payment lifecycles involve time intervals:
- Payments are matched against orders within an expected window (e.g. 48 hours).
- Windowed aggregations compute 15-minute throughput and lateness.
- Sweep jobs expire unpaid or underpaid orders after the window closes.

If the pipeline queries `current_timestamp()` or system wall-clock `datetime.now()` to make routing, windowing, or status decisions, historical replays and testing become non-deterministic. Re-running the same dataset weeks later would compute different results because the wall-clock has moved.

## Decision
All pipeline logic, windowing, and status evaluations strictly use **event time** or **max-seen event time**:
1. Every payment event extracts `paid_at` as its authoritative event timestamp.
2. The simulator operates on an accelerated virtual clock where 1 wall-clock second corresponds to an adjustable simulation increment (e.g., 10 minutes).
3. The sweep job evaluates order expiration based on the maximum event time present in the lakehouse rather than the host system clock.

## Consequences
- **Positive**: Strict determinism. Replaying raw topics months later from offset 0 yields bit-for-bit identical results.
- **Positive**: Integration tests can simulate multi-day payment flows in seconds.
- **Trade-off**: Requires explicit event time propagation and bookkeeping across all medallion stages.
