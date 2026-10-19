# Milestone 2: FastAPI Ingestion Receiver & Kafka Producer

## 1. Context & Objectives

External payment rails (specifically Safaricom M-Pesa Daraja C2B and commercial banking APIs) communicate settlement outcomes via asynchronous HTTP webhooks. If an edge webhook receiver responds slowly or throws an HTTP 500 error, payment gateways will either aggressively retry (causing duplicate callback storms) or silently drop the notification, leading to unrecorded payments and revenue leakage.

The objective of **Milestone 2 (M2)** is to build an ultra-resilient, non-blocking edge ingestion receiver that:
1. Immediately accepts incoming callbacks with the exact response format expected by the payment rail.
2. Forwards the 100% untouched raw payload string to Kafka.
3. Completely decouples webhook ingestion durability from downstream PySpark lakehouse processing.

---

## 2. Ingestion Flow & Sequence Diagram

```mermaid
sequenceDiagram
    autonumber
    actor Gateway as M-Pesa / Bank Gateway
    participant Edge as FastAPI Receiver (receiver/app.py)
    participant Producer as Kafka Producer (kafka_producer.py)
    participant Kafka as Redpanda Broker (Topic Log)

    Note over Gateway,Edge: Callback Arrives at Edge
    Gateway->>Edge: POST /api/v1/payments/mpesa (Raw JSON Body)
    activate Edge
    Edge->>Producer: publish_raw_event(topic, raw_payload, key)
    activate Producer
    Producer->>Kafka: send(payments.mpesa.raw, key=TransID, value=raw_bytes)
    Producer-->>Edge: Message queued / acknowledged
    deactivate Producer
    Edge-->>Gateway: 200 OK {"ResultCode": 0, "ResultDesc": "Accepted"}
    deactivate Edge
    Note over Gateway,Edge: Gateway satisfied in < 15ms; no retry storm

    Note over Gateway,Edge: Bank Webhook Flow
    Gateway->>Edge: POST /api/v1/payments/bank (Raw Settlement Body)
    activate Edge
    Edge->>Producer: publish_raw_event(topic, raw_payload, key)
    activate Producer
    Producer->>Kafka: send(payments.bank.raw, key=reference, value=raw_bytes)
    Producer-->>Edge: Message queued
    deactivate Producer
    Edge-->>Gateway: 200 OK {"status": "received", "receipt": "..."}
    deactivate Edge
```

---

## 3. Key Architectural Decisions

### Decision 1: The Zero-Loss Edge Philosophy (Dukastack Borrowed Rule)
- **Principle**: The edge service must be **dumb and indestructible**.
- **Implementation**: The receiver does not validate business logic, verify customer accounts, or check order existence.
- **Malformed Payload Handling**: If an external provider transmits corrupted syntax (`{"TransID": "CORRUPT", BAD_JSON`), the receiver does **not** reject with HTTP 400 Bad Request. Instead, it reads the raw bytes, decodes with replacement characters (`errors="replace"`), and flushes the exact payload string to Kafka. Downstream Bronze Delta tables preserve the exact evidence, and Silver routes it to `silver_rejects`.

### Decision 2: Wire Format Compatibility
- **M-Pesa Rail**: Safaricom Daraja specifically expects an HTTP 200 response with JSON `{ "ResultCode": 0, "ResultDesc": "Accepted" }`. Any deviation causes Safaricom's gateway to retry up to 5 times.
- **Bank Rail**: Returns HTTP 200 with `{ "status": "received", "receipt": reference }` confirming receipt acknowledgment.
- **Orders Rail**: Internal POS/ecommerce systems emit to `POST /api/v1/orders`, receiving HTTP 201 Created with `{ "status": "order_queued" }`.

### Decision 3: Partition Key Selection
- For M-Pesa, the record key is `TransID` (receipt ID).
- For Bank callbacks, the record key is `reference`.
- For Orders, the record key is `order_ref` (`ORD-xxxxxxxx`).
- This guarantees that retried webhooks for the same transaction hash to the same Kafka partition, preserving arrival ordering.

---

## 4. Build Process Step-by-Step

1. **Pydantic Schemas (`receiver/schemas.py`)**:
   - Modeled `MpesaC2BConfirmationPayload` matching Safaricom Daraja specifications.
   - Modeled `BankWebhookPayload` supporting ISO 8601 value dates and currency codes.
   - Modeled `OrderCreatedEvent` compatible with Dukastack POS systems.
2. **Resilient Kafka Producer (`receiver/kafka_producer.py`)**:
   - Thread-safe singleton producer using `kafka-python-ng`.
   - Serializes raw UTF-8 bytes without altering whitespaces or formatting.
3. **FastAPI Endpoints (`receiver/app.py`)**:
   - Exposed `/api/v1/payments/mpesa`, `/api/v1/payments/bank`, and `/api/v1/orders`.
   - Added container health check endpoint `/health`.
4. **Integration Testing**:
   - Built `tests/integration/test_receiver.py` using FastAPI `TestClient`.
   - Built `tests/integration/test_kafka_ingestion.py` testing live end-to-end publish and consume from Redpanda.

---

## 5. Verification & Proof of Done

All 8 M2 tests pass against the live Redpanda broker:
```bash
.venv/bin/pytest tests/integration/test_receiver.py tests/integration/test_kafka_ingestion.py
# Output:
# 8 passed in 1.45s
```
