"""Integration test suite for Silver normalization, deduplication MERGE, and rejects (M5)."""

import json
import uuid
from datetime import datetime

import pytest

from streaming.common.schemas import (
    BRONZE_ORDERS_SCHEMA,
    BRONZE_PAYMENTS_SCHEMA,
    hash_payer_identifier,
)
from streaming.silver import get_spark_session, start_silver_streaming


@pytest.fixture(scope="module")
def spark():
    """Shared SparkSession fixture for Silver integration testing."""
    session = get_spark_session(app_name="test-silver-dedupe")
    yield session
    session.stop()


def test_silver_dedupe_exact_duplicates(spark, tmp_path):
    """M5 Proof: 3 duplicate webhook deliveries result in exactly 1 Silver row."""
    base_dir = tmp_path / "delta"
    checkpoint_dir = tmp_path / "checkpoints"

    receipt_id = f"RKT{uuid.uuid4().hex[:7].upper()}"
    order_ref = "ORD-00000001"
    raw_payload = json.dumps(
        {
            "TransactionType": "Pay Bill",
            "TransID": receipt_id,
            "TransTime": "20261019120000",
            "TransAmount": "1500.00",
            "BusinessShortCode": "600123",
            "BillRefNumber": order_ref,
            "MSISDN": "254711223344",
            "FirstName": "Mercy",
            "LastName": "Wanjiku",
        }
    )

    now = datetime.now()
    # 3 duplicate delivery records in bronze_payments
    bronze_rows = [
        {
            "rail": "mpesa",
            "topic": "payments.mpesa.raw",
            "partition": 0,
            "offset": i,
            "kafka_timestamp": now,
            "ingest_timestamp": now,
            "raw_payload": raw_payload,
        }
        for i in range(3)
    ]

    bronze_payments_path = str(base_dir / "bronze_payments")
    spark.createDataFrame(bronze_rows, schema=BRONZE_PAYMENTS_SCHEMA).write.format("delta").mode(
        "append"
    ).save(bronze_payments_path)

    # Run Silver streaming in availableNow mode
    queries = start_silver_streaming(
        spark=spark,
        base_dir=base_dir,
        checkpoint_dir=checkpoint_dir,
        trigger_available_now=True,
    )
    queries["orders_query"].awaitTermination()
    queries["payments_query"].awaitTermination()

    # Verify silver_payments has exactly 1 row for receipt_id
    silver_df = spark.read.format("delta").load(str(base_dir / "silver_payments"))
    matched = silver_df.filter(silver_df.rail_receipt_id == receipt_id).collect()
    assert len(matched) == 1
    row = matched[0]
    assert row["rail"] == "mpesa"
    assert row["order_ref"] == order_ref
    assert row["amount_kes"] == 1500.0
    assert row["status"] == "SETTLED"

    # Verify PII was properly hashed
    expected_hash = hash_payer_identifier("254711223344")
    assert row["payer_hash"] == expected_hash
    assert "254711223344" not in json.dumps(row.asDict(), default=str)


def test_silver_duplicate_conflict_detection(spark, tmp_path):
    """M5 Proof: Duplicate receipt with conflicting amount preserves initial row & logs anomaly."""
    base_dir = tmp_path / "delta"
    checkpoint_dir = tmp_path / "checkpoints"

    receipt_id = f"RKT{uuid.uuid4().hex[:7].upper()}"
    order_ref = "ORD-00000002"

    first_payload = json.dumps(
        {
            "TransactionType": "Pay Bill",
            "TransID": receipt_id,
            "TransTime": "20261019120000",
            "TransAmount": "2000.00",
            "BusinessShortCode": "600123",
            "BillRefNumber": order_ref,
            "MSISDN": "254700000001",
        }
    )

    conflicting_payload = json.dumps(
        {
            "TransactionType": "Pay Bill",
            "TransID": receipt_id,
            "TransTime": "20261019120500",
            "TransAmount": "3500.00",  # Conflicting amount
            "BusinessShortCode": "600123",
            "BillRefNumber": order_ref,
            "MSISDN": "254700000001",
        }
    )

    now = datetime.now()
    bronze_payments_path = str(base_dir / "bronze_payments")

    # 1. Ingest initial payment
    first_row = [
        {
            "rail": "mpesa",
            "topic": "payments.mpesa.raw",
            "partition": 0,
            "offset": 0,
            "kafka_timestamp": now,
            "ingest_timestamp": now,
            "raw_payload": first_payload,
        }
    ]
    spark.createDataFrame(first_row, schema=BRONZE_PAYMENTS_SCHEMA).write.format("delta").mode(
        "append"
    ).save(bronze_payments_path)

    q1 = start_silver_streaming(
        spark=spark, base_dir=base_dir, checkpoint_dir=checkpoint_dir, trigger_available_now=True
    )
    q1["payments_query"].awaitTermination()

    # 2. Ingest conflicting payment in second micro-batch
    conflict_row = [
        {
            "rail": "mpesa",
            "topic": "payments.mpesa.raw",
            "partition": 0,
            "offset": 1,
            "kafka_timestamp": now,
            "ingest_timestamp": now,
            "raw_payload": conflicting_payload,
        }
    ]
    spark.createDataFrame(conflict_row, schema=BRONZE_PAYMENTS_SCHEMA).write.format("delta").mode(
        "append"
    ).save(bronze_payments_path)

    q2 = start_silver_streaming(
        spark=spark, base_dir=base_dir, checkpoint_dir=checkpoint_dir, trigger_available_now=True
    )
    q2["payments_query"].awaitTermination()

    # Verify silver_payments STILL contains the original 2000.00 KES (not overwritten)
    silver_df = spark.read.format("delta").load(str(base_dir / "silver_payments"))
    matched = silver_df.filter(silver_df.rail_receipt_id == receipt_id).collect()
    assert len(matched) == 1
    assert matched[0]["amount_kes"] == 2000.0

    # Verify gold_payment_anomalies has DUPLICATE_CONFLICT recorded
    anom_df = spark.read.format("delta").load(str(base_dir / "gold_payment_anomalies"))
    anom_rows = anom_df.filter(anom_df.rail_receipt_id == receipt_id).collect()
    assert len(anom_rows) == 1
    assert anom_rows[0]["anomaly_type"] == "DUPLICATE_CONFLICT"
    details = json.loads(anom_rows[0]["details"])
    assert details["existing_amount"] == 2000.0
    assert details["conflicting_amount"] == 3500.0


def test_silver_rejects_routing_for_malformed_payloads(spark, tmp_path):
    """M5 Proof: Malformed or unparseable payloads route to silver_rejects with reason."""
    base_dir = tmp_path / "delta"
    checkpoint_dir = tmp_path / "checkpoints"

    now = datetime.now()
    bad_rows = [
        # 1. Corrupted syntax JSON
        {
            "rail": "mpesa",
            "topic": "payments.mpesa.raw",
            "partition": 0,
            "offset": 0,
            "kafka_timestamp": now,
            "ingest_timestamp": now,
            "raw_payload": '{"TransID": "CORRUPT", BAD_JSON',
        },
        # 2. Incomplete payload missing mandatory TransID
        {
            "rail": "mpesa",
            "topic": "payments.mpesa.raw",
            "partition": 0,
            "offset": 1,
            "kafka_timestamp": now,
            "ingest_timestamp": now,
            "raw_payload": '{"BillRefNumber": "ORD-00000003", "TransAmount": "500.00"}',
        },
    ]

    bronze_payments_path = str(base_dir / "bronze_payments")
    spark.createDataFrame(bad_rows, schema=BRONZE_PAYMENTS_SCHEMA).write.format("delta").mode(
        "append"
    ).save(bronze_payments_path)

    queries = start_silver_streaming(
        spark=spark, base_dir=base_dir, checkpoint_dir=checkpoint_dir, trigger_available_now=True
    )
    queries["payments_query"].awaitTermination()

    # Verify silver_rejects contains both bad records
    rejects_df = spark.read.format("delta").load(str(base_dir / "silver_rejects"))
    assert rejects_df.count() == 2
    reasons = [r["reject_reason"] for r in rejects_df.collect()]
    assert any("JSON" in r or "Expecting" in r for r in reasons)
    assert any("Missing mandatory" in r for r in reasons)


def test_silver_orders_deduplication(spark, tmp_path):
    """M5 Proof: Orders are merged idempotently on order_ref."""
    base_dir = tmp_path / "delta"
    checkpoint_dir = tmp_path / "checkpoints"

    order_ref = "ORD-00000010"
    order_json = json.dumps(
        {
            "order_ref": order_ref,
            "shop_id": "SHOP-001-NAIROBI-CBD",
            "amount_kes": 4500.0,
            "currency": "KES",
            "created_at": "2026-10-18T10:00:00Z",
        }
    )

    now = datetime.now()
    orders_rows = [
        {
            "topic": "orders.created",
            "partition": 0,
            "offset": i,
            "kafka_timestamp": now,
            "ingest_timestamp": now,
            "raw_payload": order_json,
        }
        for i in range(2)
    ]

    bronze_orders_path = str(base_dir / "bronze_orders")
    spark.createDataFrame(orders_rows, schema=BRONZE_ORDERS_SCHEMA).write.format("delta").mode(
        "append"
    ).save(bronze_orders_path)

    queries = start_silver_streaming(
        spark=spark, base_dir=base_dir, checkpoint_dir=checkpoint_dir, trigger_available_now=True
    )
    queries["orders_query"].awaitTermination()

    silver_orders_df = spark.read.format("delta").load(str(base_dir / "silver_orders"))
    matched = silver_orders_df.filter(silver_orders_df.order_ref == order_ref).collect()
    assert len(matched) == 1
    assert matched[0]["amount_kes"] == 4500.0
    assert matched[0]["shop_id"] == "SHOP-001-NAIROBI-CBD"
