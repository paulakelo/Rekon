"""Integration test suite for Bronze streaming append from Kafka to Delta Lake (M4)."""

import json
import uuid
from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from receiver.app import app
from streaming.bronze import get_spark_session, start_bronze_streaming


@pytest.fixture(scope="module")
def spark():
    """Create a shared PySpark SparkSession for Bronze integration testing."""
    session = get_spark_session(app_name="test-bronze-integration")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def api_client():
    """FastAPI TestClient fixture for dispatching webhooks to live Kafka topics."""
    with TestClient(app) as client:
        yield client


def test_bronze_streaming_append_end_to_end(spark, api_client, tmp_path):
    """M4 Proof: Streaming queries consume Kafka topics and append to Bronze Delta tables."""
    test_run_id = uuid.uuid4().hex[:8].upper()
    order_ref = f"ORD-M4-{test_run_id}"

    # 1. Ingest test order event via receiver endpoint
    order_payload = {
        "order_ref": order_ref,
        "shop_id": "SHOP-M4-TEST",
        "amount_kes": 5400.0,
        "currency": "KES",
        "created_at": datetime.now().isoformat(),
    }
    order_resp = api_client.post("/api/v1/orders", json=order_payload)
    assert order_resp.status_code == 201

    # 2. Ingest M-Pesa payment callback via receiver endpoint
    mpesa_payload = {
        "TransactionType": "Pay Bill",
        "TransID": f"RKT{test_run_id}",
        "TransTime": "20261019120000",
        "TransAmount": "3000.00",
        "BusinessShortCode": "600123",
        "BillRefNumber": order_ref,
        "OrgAccountBalance": "500000.00",
        "MSISDN": "254712345678",
        "FirstName": "Paula",
        "LastName": "Akelo",
    }
    mpesa_resp = api_client.post("/api/v1/payments/mpesa", json=mpesa_payload)
    assert mpesa_resp.status_code == 200

    # 3. Ingest Bank payment callback via receiver endpoint
    bank_payload = {
        "reference": f"BNK-M4-{test_run_id}",
        "amount": 2400.0,
        "currency": "KES",
        "value_date": datetime.now().isoformat(),
        "status": "SETTLED",
        "narration": f"Split settlement for {order_ref}",
        "order_reference": order_ref,
        "sender_account_masked": "XXXX-XXXX-9999",
        "bank_code": "011",
    }
    bank_resp = api_client.post("/api/v1/payments/bank", json=bank_payload)
    assert bank_resp.status_code == 200

    # 4. Start Bronze streaming queries in availableNow mode
    delta_dir = tmp_path / "delta"
    checkpoint_dir = tmp_path / "checkpoints"

    queries = start_bronze_streaming(
        spark=spark,
        base_dir=delta_dir,
        checkpoint_dir=checkpoint_dir,
        kafka_bootstrap="localhost:9092",
        trigger_available_now=True,
    )

    queries["orders_query"].awaitTermination()
    queries["payments_query"].awaitTermination()

    # 5. Read back bronze_orders Delta table and verify records
    bronze_orders_path = str(delta_dir / "bronze_orders")
    orders_df = spark.read.format("delta").load(bronze_orders_path)
    assert orders_df.count() >= 1

    matched_orders = orders_df.filter(orders_df.raw_payload.contains(order_ref)).collect()
    assert len(matched_orders) == 1
    order_row = matched_orders[0]

    assert order_row["topic"] == "orders.created"
    assert order_row["partition"] is not None
    assert order_row["offset"] is not None
    assert order_row["kafka_timestamp"] is not None
    assert order_row["ingest_timestamp"] is not None

    parsed_order = json.loads(order_row["raw_payload"])
    assert parsed_order["order_ref"] == order_ref
    assert float(parsed_order["amount_kes"]) == 5400.0

    # 6. Read back bronze_payments Delta table and verify M-Pesa & Bank records
    bronze_payments_path = str(delta_dir / "bronze_payments")
    payments_df = spark.read.format("delta").load(bronze_payments_path)
    assert payments_df.count() >= 2

    # Verify M-Pesa payment row
    mpesa_rows = payments_df.filter(payments_df.raw_payload.contains(f"RKT{test_run_id}")).collect()
    assert len(mpesa_rows) == 1
    m_row = mpesa_rows[0]
    assert m_row["rail"] == "mpesa"
    assert m_row["topic"] == "payments.mpesa.raw"
    parsed_mpesa = json.loads(m_row["raw_payload"])
    assert parsed_mpesa["TransID"] == f"RKT{test_run_id}"
    assert parsed_mpesa["BillRefNumber"] == order_ref

    # Verify Bank payment row
    bank_rows = payments_df.filter(
        payments_df.raw_payload.contains(f"BNK-M4-{test_run_id}")
    ).collect()
    assert len(bank_rows) == 1
    b_row = bank_rows[0]
    assert b_row["rail"] == "bank"
    assert b_row["topic"] == "payments.bank.raw"
    parsed_bank = json.loads(b_row["raw_payload"])
    assert parsed_bank["reference"] == f"BNK-M4-{test_run_id}"
    assert parsed_bank["order_reference"] == order_ref

    # 7. Test Checkpoint Idempotency: re-running availableNow does not produce duplicate writes
    rerun_queries = start_bronze_streaming(
        spark=spark,
        base_dir=delta_dir,
        checkpoint_dir=checkpoint_dir,
        kafka_bootstrap="localhost:9092",
        trigger_available_now=True,
    )
    rerun_queries["orders_query"].awaitTermination()
    rerun_queries["payments_query"].awaitTermination()

    post_rerun_orders = spark.read.format("delta").load(bronze_orders_path)
    post_rerun_matched = post_rerun_orders.filter(
        post_rerun_orders.raw_payload.contains(order_ref)
    ).collect()
    assert len(post_rerun_matched) == 1  # Exactly 1 row preserved, no duplicate
