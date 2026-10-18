# ADR 0003: Raw Payload Preservation in Bronze Layer & Topic Replayability

## Context
Third-party payment gateways (such as Safaricom Daraja, equity banks, or fintech aggregators) periodically introduce undocumented fields, adjust header casing, or emit malformed callback payloads during gateway maintenance. If an edge webhook receiver validates or parses schemas strictly before persistence, non-conforming webhooks are dropped with HTTP 4xx/5xx errors, causing permanent financial data loss.

## Decision
1. The FastAPI receiver treats payloads as opaque binary/string objects and immediately enqueues them onto Kafka topics.
2. The PySpark Structured Streaming Bronze job appends raw payload strings directly into `bronze_payments` and `bronze_orders` alongside Kafka partition and offset metadata.
3. Parsing, schema validation, and normalization take place exclusively between Bronze and Silver. Malformed records are diverted to `silver_rejects` rather than discarded.
4. If schema parsing logic needs to be amended, the entire Silver and Gold layers can be wiped and re-derived from Bronze without requiring external rail re-transmission.

## Consequences
- **Positive**: Zero data loss at the network boundary.
- **Positive**: Complete auditability and bug-recovery via cold replay.
- **Trade-off**: Slightly increased storage consumption for raw strings in Bronze. In practice, gzip/snappy compression in Parquet/Delta renders this overhead negligible.
