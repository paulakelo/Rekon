# Architecture & Lakehouse Design

## 1. Executive Summary

**Rekon** is an event-driven, production-grade payment reconciliation lakehouse designed for digital commerce businesses operating across emerging markets (specifically Kenya and broader East Africa). It continuously reconciles real-time webhook streams from mobile money (Safaricom Daraja C2B confirmation API) and commercial banking settlement gateways against point-of-sale (POS) and ecommerce order records.

The pipeline implements the **Medallion Architecture (Bronze -> Silver -> Gold)** using **PySpark Structured Streaming** and **Delta Lake**, running entirely on a **$0 local stack** powered by Redpanda (Kafka-compatible broker in Docker) on an 8 GB developer laptop.

---

## 2. End-to-End System Architecture

```mermaid
graph TD
    subgraph ExternalSources["External Rails & POS Systems"]
        MpesaGateway["Safaricom Daraja API\n(C2B Confirmation Webhook)"]
        BankGateway["Commercial Core Banking Gateway\n(Settlement Webhook)"]
        POSSystem["Dukastack POS / Ecommerce System\n(Order Created Event)"]
    end

    subgraph IngestionTier["Edge Ingestion Tier (FastAPI + Redpanda Docker)"]
        Receiver["FastAPI Webhook Receiver (:8000)\n- Non-blocking async endpoints\n- Immediate 200 OK / 201 Created acknowledgment\n- Zero-loss edge principle"]
        
        subgraph KafkaTopics["Redpanda Kafka Topics (:9092)"]
            T_Orders["orders.created\n(Key: order_ref, 3 partitions)"]
            T_Mpesa["payments.mpesa.raw\n(Key: TransID, 3 partitions)"]
            T_Bank["payments.bank.raw\n(Key: reference, 3 partitions)"]
        end
    end

    subgraph StreamingCompute["Streaming & Transformation Tier (PySpark 3.5.1 + Delta Lake 3.2.0)"]
        subgraph BronzeIngestion["Bronze Streaming Ingestion (streaming/bronze.py)"]
            BronzeStream["readStream.format('kafka')\nAppend-only streaming\nFault-tolerant checkpointing"]
        end

        subgraph SilverEngine["Silver Engine (streaming/silver.py foreachBatch)"]
            Normalize["Payload Normalizer\n- Safaricom & Bank schemas\n- Event timestamps"]
            PIIHash["Salted SHA-256 PII Sanitizer\nMSISDN / Account -> payer_hash"]
            DedupeEngine["Delta Lake ACID MERGE Engine\n- Intra-batch deduplication\n- ON rail + rail_receipt_id\n- Conflict detection (no overwrite)"]
        end

        subgraph GoldEngine["Gold Marts Engine (streaming/gold.py)"]
            StatusSM["Order Status State Machine\nUNPAID | PARTIAL | PAID | OVERPAID | UNDERPAID"]
            AnomDetector["Anomaly Detector\nReason-coded anomaly generation"]
            ThroughputWin["Throughput Aggregator\n15-min event-time windowing & watermarks"]
            DailyRecon["Daily Reconciliation Mart\nBilled vs Collected cash summaries"]
        end
    end

    subgraph LakehouseStorage["Delta Lake Medallion Storage Layer (data/delta/)"]
        subgraph BronzeTables["Bronze (Immutable Raw Log)"]
            B_Orders["bronze_orders"]
            B_Payments["bronze_payments"]
        end

        subgraph SilverTables["Silver (Clean, Normalized, Deduplicated)"]
            S_Orders["silver_orders\n(Unique on order_ref)"]
            S_Payments["silver_payments\n(Unique on rail + rail_receipt_id)"]
            S_Rejects["silver_rejects\n(Malformed JSON / schema errors)"]
        end

        subgraph GoldTables["Gold (Business Marts & Observability)"]
            G_Status["gold_order_payment_status\n(Grain: order_ref)"]
            G_Anomalies["gold_payment_anomalies\n(Grain: anomaly_id)"]
            G_Daily["gold_reconciliation_daily\n(Grain: recon_date)"]
            G_Throughput["gold_rail_throughput_15m\n(Grain: rail + window)"]
        end
    end

    MpesaGateway -->|HTTP POST /api/v1/payments/mpesa| Receiver
    BankGateway -->|HTTP POST /api/v1/payments/bank| Receiver
    POSSystem -->|HTTP POST /api/v1/orders| Receiver

    Receiver --> T_Orders
    Receiver --> T_Mpesa
    Receiver --> T_Bank

    T_Orders --> BronzeStream --> B_Orders
    T_Mpesa --> BronzeStream --> B_Payments
    T_Bank --> BronzeStream --> B_Payments

    B_Orders --> SilverEngine --> S_Orders
    B_Payments --> SilverEngine
    SilverEngine --> Normalize --> PIIHash --> DedupeEngine
    DedupeEngine --> S_Payments
    Normalize -->|Corrupt / Missing Fields| S_Rejects
    DedupeEngine -->|Duplicate Conflict| G_Anomalies

    S_Orders --> GoldEngine
    S_Payments --> GoldEngine
    GoldEngine --> StatusSM --> G_Status
    GoldEngine --> AnomDetector --> G_Anomalies
    GoldEngine --> ThroughputWin --> G_Throughput
    GoldEngine --> DailyRecon --> G_Daily
```

---

## 3. Webhook Ingestion & Decoupled Durability

```mermaid
sequenceDiagram
    autonumber
    actor Gateway as External Payment Gateway (Safaricom / Bank)
    participant Receiver as FastAPI Receiver (:8000)
    participant Producer as Kafka Producer (kafka-python-ng)
    participant Redpanda as Redpanda Broker (Topic Log)
    participant PySpark as PySpark Structured Streaming

    Gateway->>Receiver: POST /api/v1/payments/mpesa (HTTP Callback)
    activate Receiver
    Note over Receiver: Read raw body bytes;<br/>Do NOT parse business rules
    Receiver->>Producer: publish_raw_event(topic, raw_payload, key)
    activate Producer
    Producer->>Redpanda: Append to payments.mpesa.raw
    Redpanda-->>Producer: Broker ACK (Offset committed)
    deactivate Producer
    Receiver-->>Gateway: 200 OK {"ResultCode": 0, "ResultDesc": "Accepted"}
    deactivate Receiver
    Note over Gateway: Gateway satisfied in < 15ms;<br/>Zero retry storm

    Note over Redpanda,PySpark: Asynchronous Lakehouse Ingestion
    Redpanda->>PySpark: readStream micro-batch
    activate PySpark
    PySpark->>PySpark: Append raw payload + metadata to Bronze Delta
    PySpark-->>Redpanda: Update Kafka consumer offset in checkpoint
    deactivate PySpark
```

---

## 4. Medallion Storage Tier Specifications

### 4.1 Bronze Layer (Immutable Raw Event Log)
- **Tables**:
  - `data/delta/bronze_orders`
  - `data/delta/bronze_payments`
- **Write Pattern**: Streaming append with checkpointing (`.format("delta").outputMode("append")`).
- **Schema Contracts**:
  - `topic` (`StringType`): Source topic (`orders.created`, `payments.mpesa.raw`, `payments.bank.raw`).
  - `partition` (`IntegerType`): Kafka partition ID (0, 1, 2).
  - `offset` (`IntegerType`): Kafka sequence offset.
  - `kafka_timestamp` (`TimestampType`): Broker commit timestamp.
  - `ingest_timestamp` (`TimestampType`): Micro-batch arrival timestamp (`current_timestamp()`).
  - `raw_payload` (`StringType`): Unparsed, untouched JSON payload string.
- **Key Guarantee**: 100% preservation of incoming bytes. If a parser bug is detected months later, Bronze can be replayed from offset 0 to reconstruct the entire analytical state without re-querying external providers.

---

### 4.2 Silver Layer (Clean, Validated, Deduplicated & Masked)
- **Tables**:
  - `data/delta/silver_orders` (Primary Key: `order_ref`)
  - `data/delta/silver_payments` (Primary Key: `(rail, rail_receipt_id)`)
  - `data/delta/silver_rejects` (Dead-letter store for invalid payloads)
- **Write Pattern**: Structured Streaming `foreachBatch` with Delta Lake ACID `MERGE`.
- **Deduplication Strategy (ADR 0001)**:
  - Intra-batch deduplication via `.dropDuplicates(["rail", "rail_receipt_id"])`.
  - Delta Lake `MERGE`:
    ```sql
    MERGE INTO silver_payments AS target
    USING microbatch_payments AS source
    ON target.rail = source.rail AND target.rail_receipt_id = source.rail_receipt_id
    WHEN NOT MATCHED THEN INSERT *
    ```
- **Duplicate Conflict Protection**:
  - If a receipt ID arrives a second time with a *differing* amount, the initial record is preserved without overwrite, and an anomaly row is dispatched to `gold_payment_anomalies`.
- **PII Sanitation (ADR 0005)**:
  - Phone numbers (`MSISDN`) and bank accounts are sanitized using salted SHA-256 (`hash_payer_identifier`), writing an anonymized 24-character `payer_hash`.
- **Rejects Routing**:
  - Payloads with invalid JSON syntax or missing mandatory fields land in `silver_rejects` with an explicit `reject_reason`.

---

### 4.3 Gold Layer (Analytical Marts & Financial Ledger)
- **Tables**:
  - `data/delta/gold_order_payment_status`: Order reconciliation grain.
    - Status state machine: `UNPAID` $\rightarrow$ `PARTIAL` $\rightarrow$ `PAID` (or `OVERPAID` / `UNDERPAID`).
  - `data/delta/gold_payment_anomalies`: Diagnostic grain.
    - Anomaly codes: `DUPLICATE_CONFLICT`, `ORPHAN_PAYMENT`, `LATE_PAYMENT`, `OVERPAID`, `UNDERPAID`, `MALFORMED_PAYLOAD`.
  - `data/delta/gold_reconciliation_daily`: Executive reporting grain.
    - Aggregates daily billed vs collected cash totals and order counts by status.
  - `data/delta/gold_rail_throughput_15m`: Operational telemetry grain.
    - 15-minute event-time tumbling windows tracking transaction velocity, total cash, and average order value per rail.

---

## 5. Summary of Architecture Decision Records (ADRs)

| ADR | Title | Core Decision |
| :--- | :--- | :--- |
| **ADR 0001** | Delta MERGE vs Watermark dropDuplicates | Persist keys in Delta Lake and merge on `(rail, rail_receipt_id)` to survive late retries and prevent memory leaks. |
| **ADR 0002** | Virtual Clock vs Wall Clock | Forbid wall-clock `now()` in reconciliation logic; use deterministic `VirtualClock` for 100% reproducible replays. |
| **ADR 0003** | Bronze Raw Payload Preservation | Store unparsed UTF-8 strings in Bronze to guarantee zero data loss and enable cold replays. |
| **ADR 0004** | FastAPI Edge Receiver & Zero-Loss Ingestion | Immediate 200 OK edge responses; forward raw payloads directly to Kafka without edge rejection. |
| **ADR 0005** | Salted SHA-256 PII Masking | Salt and hash customer phone numbers and accounts before insertion into `silver_payments`. |
| **ADR 0006** | Checkpointed Delta Structured Streaming | Implement `trigger(availableNow=True)` alongside continuous mode for deterministic CI and batch executions. |
