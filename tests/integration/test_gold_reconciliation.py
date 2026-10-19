"""Integration test suite for Gold reconciliation, anomaly engine, and daily marts (M6)."""

import json
from datetime import datetime, timedelta

import pytest

from streaming.common.schemas import (
    SILVER_ORDERS_SCHEMA,
    SILVER_PAYMENTS_SCHEMA,
    hash_payer_identifier,
)
from streaming.gold import get_spark_session, run_gold_pipeline


@pytest.fixture(scope="module")
def spark():
    """Shared SparkSession fixture for Gold integration testing."""
    session = get_spark_session(app_name="test-gold-reconciliation")
    yield session
    session.stop()


def test_gold_split_payment_order_status_lifecycle(spark, tmp_path):
    """M6 Proof: Multi-installment payments correctly transition order from PARTIAL to PAID."""
    base_dir = tmp_path / "delta"
    order_ref = "ORD-M6-SPLIT"
    now = datetime(2026, 10, 19, 10, 0, 0)

    # 1. Order for 3,000 KES
    orders = [
        {
            "order_ref": order_ref,
            "shop_id": "SHOP-001",
            "amount_kes": 3000.0,
            "created_at": now,
            "_ingest_ts": now,
        }
    ]
    spark.createDataFrame(orders, schema=SILVER_ORDERS_SCHEMA).write.format("delta").mode(
        "append"
    ).save(str(base_dir / "silver_orders"))

    # 2. First installment: 1,000 KES via M-Pesa
    payments_p1 = [
        {
            "rail": "mpesa",
            "rail_receipt_id": "RKT-SPLIT-01",
            "order_ref": order_ref,
            "amount_kes": 1000.0,
            "currency": "KES",
            "paid_at": now + timedelta(minutes=10),
            "payer_hash": hash_payer_identifier("254700000001"),
            "status": "SETTLED",
            "topic": "payments.mpesa.raw",
            "partition": 0,
            "offset": 0,
            "_silver_ts": now,
        }
    ]
    spark.createDataFrame(payments_p1, schema=SILVER_PAYMENTS_SCHEMA).write.format("delta").mode(
        "append"
    ).save(str(base_dir / "silver_payments"))

    # Run Gold processing after first installment
    res1 = run_gold_pipeline(spark, base_dir=base_dir, is_window_expired=False)
    status_df1 = spark.read.format("delta").load(res1["gold_status_path"])
    row1 = status_df1.filter(status_df1.order_ref == order_ref).collect()[0]

    assert row1["status"] == "PARTIAL"
    assert row1["paid_total_kes"] == 1000.0
    assert row1["payment_count"] == 1
    assert row1["settled_at"] is None

    # 3. Second installment: 2,000 KES via Bank rail (completing 3,000 KES total)
    payments_p2 = [
        {
            "rail": "bank",
            "rail_receipt_id": "BNK-SPLIT-02",
            "order_ref": order_ref,
            "amount_kes": 2000.0,
            "currency": "KES",
            "paid_at": now + timedelta(minutes=45),
            "payer_hash": hash_payer_identifier("ACC-9999"),
            "status": "SETTLED",
            "topic": "payments.bank.raw",
            "partition": 0,
            "offset": 1,
            "_silver_ts": now,
        }
    ]
    spark.createDataFrame(payments_p2, schema=SILVER_PAYMENTS_SCHEMA).write.format("delta").mode(
        "append"
    ).save(str(base_dir / "silver_payments"))

    # Run Gold processing after second installment
    res2 = run_gold_pipeline(spark, base_dir=base_dir, is_window_expired=False)
    status_df2 = spark.read.format("delta").load(res2["gold_status_path"])
    row2 = status_df2.filter(status_df2.order_ref == order_ref).collect()[0]

    assert row2["status"] == "PAID"
    assert row2["paid_total_kes"] == 3000.0
    assert row2["payment_count"] == 2
    assert row2["settled_at"] == now + timedelta(minutes=45)


def test_gold_overpayment_and_late_payment_anomalies(spark, tmp_path):
    """M6 Proof: Overpaid and late payments trigger corresponding gold anomalies."""
    base_dir = tmp_path / "delta"
    order_ref_over = "ORD-M6-OVER"
    order_ref_late = "ORD-M6-LATE"
    t0 = datetime(2026, 10, 15, 8, 0, 0)

    orders = [
        {
            "order_ref": order_ref_over,
            "shop_id": "SHOP-002",
            "amount_kes": 2000.0,
            "created_at": t0,
            "_ingest_ts": t0,
        },
        {
            "order_ref": order_ref_late,
            "shop_id": "SHOP-003",
            "amount_kes": 1500.0,
            "created_at": t0,
            "_ingest_ts": t0,
        },
    ]
    spark.createDataFrame(orders, schema=SILVER_ORDERS_SCHEMA).write.format("delta").mode(
        "append"
    ).save(str(base_dir / "silver_orders"))

    payments = [
        # Overpayment: 2,500 paid for 2,000 order
        {
            "rail": "mpesa",
            "rail_receipt_id": "RKT-OVER-01",
            "order_ref": order_ref_over,
            "amount_kes": 2500.0,
            "currency": "KES",
            "paid_at": t0 + timedelta(hours=1),
            "payer_hash": hash_payer_identifier("254700000002"),
            "status": "SETTLED",
            "topic": "payments.mpesa.raw",
            "partition": 0,
            "offset": 0,
            "_silver_ts": t0,
        },
        # Late payment: Paid 54 hours later (> 48h window)
        {
            "rail": "mpesa",
            "rail_receipt_id": "RKT-LATE-01",
            "order_ref": order_ref_late,
            "amount_kes": 1500.0,
            "currency": "KES",
            "paid_at": t0 + timedelta(hours=54),
            "payer_hash": hash_payer_identifier("254700000003"),
            "status": "SETTLED",
            "topic": "payments.mpesa.raw",
            "partition": 0,
            "offset": 1,
            "_silver_ts": t0,
        },
        # Orphan payment: references nonexistent order
        {
            "rail": "bank",
            "rail_receipt_id": "BNK-ORP-01",
            "order_ref": "ORD-NONEXISTENT",
            "amount_kes": 800.0,
            "currency": "KES",
            "paid_at": t0,
            "payer_hash": hash_payer_identifier("ACC-0000"),
            "status": "SETTLED",
            "topic": "payments.bank.raw",
            "partition": 0,
            "offset": 2,
            "_silver_ts": t0,
        },
    ]
    spark.createDataFrame(payments, schema=SILVER_PAYMENTS_SCHEMA).write.format("delta").mode(
        "append"
    ).save(str(base_dir / "silver_payments"))

    res = run_gold_pipeline(spark, base_dir=base_dir, is_window_expired=False)
    status_df = spark.read.format("delta").load(res["gold_status_path"])
    anom_df = spark.read.format("delta").load(res["gold_anomalies_path"])

    # Verify status transitions
    over_row = status_df.filter(status_df.order_ref == order_ref_over).collect()[0]
    assert over_row["status"] == "OVERPAID"
    assert over_row["paid_total_kes"] == 2500.0

    late_row = status_df.filter(status_df.order_ref == order_ref_late).collect()[0]
    assert late_row["status"] == "PAID"  # Settled, but flagged with anomaly

    # Verify anomalies
    anomalies = anom_df.collect()
    types = [a["anomaly_type"] for a in anomalies]
    assert "OVERPAID" in types
    assert "LATE_PAYMENT" in types
    assert "ORPHAN_PAYMENT" in types

    over_ano = [a for a in anomalies if a["anomaly_type"] == "OVERPAID"][0]
    assert json.loads(over_ano["details"])["excess"] == 500.0

    orp_ano = [a for a in anomalies if a["anomaly_type"] == "ORPHAN_PAYMENT"][0]
    assert orp_ano["order_ref"] == "ORD-NONEXISTENT"


def test_gold_underpayment_sweep_and_daily_marts(spark, tmp_path):
    """M6 Proof: Window expiry transitions PARTIAL to UNDERPAID; daily mart aggregates correctly."""
    base_dir = tmp_path / "delta"
    order_ref = "ORD-M6-PARTIAL"
    now = datetime(2026, 10, 19, 9, 0, 0)

    orders = [
        {
            "order_ref": order_ref,
            "shop_id": "SHOP-004",
            "amount_kes": 5000.0,
            "created_at": now,
            "_ingest_ts": now,
        }
    ]
    spark.createDataFrame(orders, schema=SILVER_ORDERS_SCHEMA).write.format("delta").mode(
        "append"
    ).save(str(base_dir / "silver_orders"))

    payments = [
        {
            "rail": "mpesa",
            "rail_receipt_id": "RKT-UNDER-01",
            "order_ref": order_ref,
            "amount_kes": 3500.0,  # 1500 short of 5000
            "currency": "KES",
            "paid_at": now,
            "payer_hash": hash_payer_identifier("254700000004"),
            "status": "SETTLED",
            "topic": "payments.mpesa.raw",
            "partition": 0,
            "offset": 0,
            "_silver_ts": now,
        }
    ]
    spark.createDataFrame(payments, schema=SILVER_PAYMENTS_SCHEMA).write.format("delta").mode(
        "append"
    ).save(str(base_dir / "silver_payments"))

    # 1. Active window execution -> status PARTIAL
    res_active = run_gold_pipeline(spark, base_dir=base_dir, is_window_expired=False)
    status_active = spark.read.format("delta").load(res_active["gold_status_path"])
    active_row = status_active.filter(status_active.order_ref == order_ref).collect()[0]
    assert active_row["status"] == "PARTIAL"

    # 2. Sweep execution (window expired) -> status UNDERPAID
    res_sweep = run_gold_pipeline(spark, base_dir=base_dir, is_window_expired=True)
    status_sweep = spark.read.format("delta").load(res_sweep["gold_status_path"])
    row_sweep = status_sweep.filter(status_sweep.order_ref == order_ref).collect()[0]
    assert row_sweep["status"] == "UNDERPAID"

    anom_sweep = spark.read.format("delta").load(res_sweep["gold_anomalies_path"])
    under_anoms = anom_sweep.filter(anom_sweep.anomaly_type == "UNDERPAID").collect()
    assert len(under_anoms) == 1
    assert json.loads(under_anoms[0]["details"])["deficit"] == 1500.0

    # 3. Verify Daily Reconciliation Mart
    daily_df = spark.read.format("delta").load(res_sweep["gold_daily_path"])
    daily_row = daily_df.collect()[0]
    assert daily_row["billed_amount_kes"] == 5000.0
    assert daily_row["collected_amount_kes"] == 3500.0
    assert daily_row["underpaid_orders_count"] == 1
    assert daily_row["paid_orders_count"] == 0

    # 4. Verify 15-Minute Rail Throughput Mart
    throughput_df = spark.read.format("delta").load(res_sweep["gold_throughput_path"])
    tp_rows = throughput_df.filter(throughput_df.rail == "mpesa").collect()
    assert len(tp_rows) >= 1
    assert tp_rows[0]["transaction_count"] == 1
    assert tp_rows[0]["total_amount_kes"] == 3500.0
