# ADR 0004: FastAPI Edge Receiver and Zero-Loss Ingestion

## Context
Payment gateways (such as Safaricom Daraja and commercial core banking APIs) post webhook notifications to our public IP endpoints. If the webhook receiver runs slow or throws an HTTP 400 or 500 error when receiving unexpected payloads, external gateways will retry aggressively or drop the payload permanently.

In traditional architectures, ingestion services often deserialize JSON into database models, run schema validation, and reject payloads that violate validation rules at the edge.

## Decision
We decouple webhook ingestion durability from downstream business validation by deploying an ultra-lightweight FastAPI gateway that follows the **Zero-Loss Edge Principle**:
1. It immediately responds with HTTP 200 OK (with rail-specific acknowledgment schemas, such as Safaricom's `{"ResultCode": 0, "ResultDesc": "Accepted"}`).
2. It forwards the untouched raw payload bytes directly into Kafka topics (`payments.mpesa.raw`, `payments.bank.raw`, `orders.created`).
3. It performs no business rule validation, order verification, or payload discarding at the edge.

## Consequences
- **Positive**: Extremely low latency (< 15ms) response to payment gateways, preventing gateway timeouts and retry storms.
- **Positive**: Downstream consumer outages (e.g., PySpark cluster restarts or maintenance) do not impact edge receiver availability.
- **Positive**: Corrupted or malformed payloads are safely preserved in Kafka and Bronze Delta tables rather than dropped into the void.
- **Trade-off**: Requires a downstream dead-letter queue / rejects table (`silver_rejects`) to catch unparseable payloads during micro-batch processing.
