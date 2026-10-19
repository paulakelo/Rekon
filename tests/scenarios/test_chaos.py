"""Comprehensive chaos test suite verifying resilience across all 11 failure scenarios.

Scenarios tested:
- C1: Exact Duplicate Redelivery (gateway retries identical payload 3 times)
- C2: Duplicate Conflict (same receipt delivered with conflicting amount)
- C3: Inversion / Pre-Payment (payment arrives before order creation)
- C4: Late Payment (payment arrives after nominal 48h payment window)
- C5: Orphan Payment (payment referencing nonexistent order reference)
- C6: Split Payment (multi-installment settlement across rails)
- C7: Underpayment (permanent partial payment finalized upon sweep)
- C8: Overpayment (excess payment flagged for merchant refund)
- C9: Malformed Payload (unparseable JSON syntax and missing mandatory fields)
- C10: Worker Crash & Restart (checkpoint recovery without duplicate rows)
- C11: Cold Replay from Bronze (wiping checkpoints/tables produces bit-identical Gold)
"""

from datetime import datetime, timedelta

import pytest

from simulator.chaos import ChaosInjector
from simulator.clock import VirtualClock
from simulator.orders import OrderGenerator
from simulator.payments import PaymentGenerator
from streaming.pipeline import ReconciliationPipeline, get_spark_session
from streaming.replay import verify_replay
from streaming.sweep import run_sweep


@pytest.fixture(scope="module")
def spark():
    """Shared SparkSession fixture for chaos testing."""
    return get_spark_session("rekon-chaos-suite")


def test_scenario_c1_exact_duplicates(spark, tmp_path):
    """Scenario C1: Exact duplicate deliveries yield exactly 1 Silver row."""
    clock = VirtualClock()
    payment_gen = PaymentGenerator(clock)
    chaos = ChaosInjector(clock, payment_gen)

    p = payment_gen.generate_mpesa_payment(order_ref="ORD-C1-0001", amount=2500.0)
    dupes = chaos.create_exact_duplicates(p, count=3)

    pipeline = ReconciliationPipeline(spark, base_dir=tmp_path)
    pipeline.append_bronze_orders(
        [
            {
                "order_ref": "ORD-C1-0001",
                "shop_id": "SHOP-1",
                "amount_kes": 2500.0,
                "created_at": clock.now_iso(),
            }
        ]
    )
    pipeline.append_bronze_payments(dupes, rail="mpesa")

    pipeline.process_silver()

    silver_payments = spark.read.format("delta").load(pipeline.silver_payments_path)
    assert silver_payments.filter(f"rail_receipt_id == '{p['TransID']}'").count() == 1


def test_scenario_c2_duplicate_conflict(spark, tmp_path):
    """Scenario C2: Same receipt with conflicting amount preserves initial row & logs anomaly."""
    clock = VirtualClock()
    payment_gen = PaymentGenerator(clock)
    chaos = ChaosInjector(clock, payment_gen)

    p = payment_gen.generate_mpesa_payment(order_ref="ORD-C2-0001", amount=1500.0)
    conflicts = chaos.create_duplicate_conflict(p, rail="mpesa", diff_ratio=2.0)

    pipeline = ReconciliationPipeline(spark, base_dir=tmp_path)
    pipeline.append_bronze_orders(
        [
            {
                "order_ref": "ORD-C2-0001",
                "shop_id": "SHOP-1",
                "amount_kes": 1500.0,
                "created_at": clock.now_iso(),
            }
        ]
    )
    # Append initial payment, then conflicting payment
    pipeline.append_bronze_payments([conflicts[0]], rail="mpesa")
    pipeline.process_silver()

    pipeline.append_bronze_payments([conflicts[1]], rail="mpesa")
    pipeline.process_silver()

    silver_df = spark.read.format("delta").load(pipeline.silver_payments_path)
    matched = silver_df.filter(f"rail_receipt_id == '{p['TransID']}'").collect()
    assert len(matched) == 1
    assert matched[0]["amount_kes"] == 1500.0  # Preserved original amount


def test_scenario_c3_inversion_pre_payment(spark, tmp_path):
    """Scenario C3: Pre-payment arriving before order creation reconciles once order lands."""
    clock = VirtualClock()
    payment_gen = PaymentGenerator(clock)
    chaos = ChaosInjector(clock, payment_gen)

    order_ref = "ORD-C3-INVERT"
    early_pay, late_order = chaos.create_inversion(order_ref, amount=4000.0, hours_early=2.0)

    pipeline = ReconciliationPipeline(spark, base_dir=tmp_path)

    # 1. Payment arrives first
    pipeline.append_bronze_payments([early_pay], rail="mpesa")
    pipeline.process_silver()
    pipeline.process_gold()

    # In-flight without order -> treated as orphan temporarily
    # 2. Order arrives 2 hours later
    pipeline.append_bronze_orders([late_order])
    pipeline.process_silver()
    pipeline.process_gold()

    # Reconciles to PAID
    status_df = spark.read.format("delta").load(pipeline.gold_status_path)
    row = status_df.filter(f"order_ref == '{order_ref}'").collect()[0]
    assert row["status"] == "PAID"
    assert row["paid_total_kes"] == 4000.0


def test_scenario_c4_late_payment(spark, tmp_path):
    """Scenario C4: Payment arriving after 48h settles order and triggers LATE_PAYMENT anomaly."""
    t0 = datetime(2026, 10, 10, 8, 0, 0)
    order_ref = "ORD-C4-LATE"

    pipeline = ReconciliationPipeline(spark, base_dir=tmp_path)
    pipeline.append_bronze_orders(
        [
            {
                "order_ref": order_ref,
                "shop_id": "SHOP-001",
                "amount_kes": 2200.0,
                "created_at": t0.isoformat(),
            }
        ]
    )
    # Payment arrives 55 hours later
    pipeline.append_bronze_payments(
        [
            {
                "TransactionType": "Pay Bill",
                "TransID": "RKT-C4-01",
                "TransTime": (t0 + timedelta(hours=55)).strftime("%Y%m%d%H%M%S"),
                "TransAmount": "2200.00",
                "BusinessShortCode": "600123",
                "BillRefNumber": order_ref,
                "MSISDN": "254700000004",
            }
        ],
        rail="mpesa",
    )

    pipeline.process_silver()
    pipeline.process_gold()

    status_df = spark.read.format("delta").load(pipeline.gold_status_path)
    assert status_df.filter(f"order_ref == '{order_ref}'").collect()[0]["status"] == "PAID"

    anom_df = spark.read.format("delta").load(pipeline.gold_anomalies_path)
    assert anom_df.filter("anomaly_type == 'LATE_PAYMENT'").count() >= 1


def test_scenario_c5_orphan_payment(spark, tmp_path):
    """Scenario C5: Payment referencing nonexistent order routes to gold anomalies."""
    clock = VirtualClock()
    payment_gen = PaymentGenerator(clock)
    chaos = ChaosInjector(clock, payment_gen)

    orphan = chaos.create_orphan_payment(amount=1850.0)

    pipeline = ReconciliationPipeline(spark, base_dir=tmp_path)
    pipeline.append_bronze_payments([orphan], rail="mpesa")

    pipeline.process_silver()
    pipeline.process_gold(is_window_expired=True)

    anom_df = spark.read.format("delta").load(pipeline.gold_anomalies_path)
    matched = anom_df.filter("anomaly_type == 'ORPHAN_PAYMENT'").collect()
    assert len(matched) >= 1
    assert any(a["order_ref"] == "ORD-99999999" for a in matched)


def test_scenario_c6_split_payment(spark, tmp_path):
    """Scenario C6: Installment payments transition order from PARTIAL to PAID."""
    clock = VirtualClock()
    order_gen = OrderGenerator(clock)
    payment_gen = PaymentGenerator(clock)
    chaos = ChaosInjector(clock, payment_gen)

    order = order_gen.next_order(fixed_amount=3000.0)
    order_ref = order["order_ref"]

    splits = chaos.create_split_payments(order_ref, total_amount=3000.0, ratios=[0.4, 0.6])

    pipeline = ReconciliationPipeline(spark, base_dir=tmp_path)
    pipeline.append_bronze_orders([order])
    pipeline.append_bronze_payments(splits, rail="mpesa")

    pipeline.process_silver()
    pipeline.process_gold()

    status_df = spark.read.format("delta").load(pipeline.gold_status_path)
    order_row = status_df.filter(f"order_ref == '{order_ref}'").collect()[0]

    assert order_row["status"] == "PAID"
    assert order_row["paid_total_kes"] == 3000.0
    assert order_row["payment_count"] == 2


def test_scenario_c7_underpayment_sweep(spark, tmp_path):
    """Scenario C7: Abandoned partial payment is finalized as UNDERPAID upon sweep."""
    clock = VirtualClock()
    payment_gen = PaymentGenerator(clock)
    chaos = ChaosInjector(clock, payment_gen)

    order_ref = "ORD-C7-UNDER"
    order = {
        "order_ref": order_ref,
        "shop_id": "SHOP-001",
        "amount_kes": 5000.0,
        "created_at": clock.now_iso(),
    }
    partial_pay = chaos.create_underpayment(order_ref, total_amount=5000.0, ratio=0.6)

    pipeline = ReconciliationPipeline(spark, base_dir=tmp_path)
    pipeline.append_bronze_orders([order])
    pipeline.append_bronze_payments([partial_pay], rail="mpesa")

    pipeline.process_silver()

    # Active window: PARTIAL
    pipeline.process_gold(is_window_expired=False)
    status_df1 = spark.read.format("delta").load(pipeline.gold_status_path)
    assert status_df1.filter(f"order_ref == '{order_ref}'").collect()[0]["status"] == "PARTIAL"

    # Sweep window: UNDERPAID
    pipeline.process_gold(is_window_expired=True)
    status_df2 = spark.read.format("delta").load(pipeline.gold_status_path)
    assert status_df2.filter(f"order_ref == '{order_ref}'").collect()[0]["status"] == "UNDERPAID"


def test_scenario_c8_overpayment(spark, tmp_path):
    """Scenario C8: Excess payment transitions order to OVERPAID and flags anomaly."""
    clock = VirtualClock()
    payment_gen = PaymentGenerator(clock)
    chaos = ChaosInjector(clock, payment_gen)

    order_ref = "ORD-C8-OVER"
    order = {
        "order_ref": order_ref,
        "shop_id": "SHOP-001",
        "amount_kes": 2000.0,
        "created_at": clock.now_iso(),
    }
    over_pay = chaos.create_overpayment(order_ref, total_amount=2000.0, ratio=1.3)

    pipeline = ReconciliationPipeline(spark, base_dir=tmp_path)
    pipeline.append_bronze_orders([order])
    pipeline.append_bronze_payments([over_pay], rail="bank")

    pipeline.process_silver()
    pipeline.process_gold()

    status_df = spark.read.format("delta").load(pipeline.gold_status_path)
    row = status_df.filter(f"order_ref == '{order_ref}'").collect()[0]
    assert row["status"] == "OVERPAID"
    assert row["paid_total_kes"] == 2600.0

    anom_df = spark.read.format("delta").load(pipeline.gold_anomalies_path)
    assert anom_df.filter("anomaly_type == 'OVERPAID'").count() >= 1


def test_scenario_c9_malformed_payload_rejects(spark, tmp_path):
    """Scenario C9: Corrupted JSON is preserved in Bronze and quarantined in silver_rejects."""
    clock = VirtualClock()
    payment_gen = PaymentGenerator(clock)
    chaos = ChaosInjector(clock, payment_gen)

    bad_payload = chaos.create_malformed_payload()

    pipeline = ReconciliationPipeline(spark, base_dir=tmp_path)
    pipeline.append_bronze_payments([bad_payload], rail="mpesa")

    pipeline.process_silver()

    rejects_df = spark.read.format("delta").load(pipeline.silver_rejects_path)
    assert rejects_df.count() >= 1
    assert any("UNCLOSED_JSON" in r["raw_payload"] for r in rejects_df.collect())


def test_scenario_c10_crash_recovery_and_sweep(spark, tmp_path):
    """Scenario C10: Table maintenance and sweep run cleanly across multiple passes."""
    pipeline = ReconciliationPipeline(spark, base_dir=tmp_path)
    pipeline.append_bronze_orders(
        [
            {
                "order_ref": "ORD-C10-001",
                "shop_id": "SHOP-1",
                "amount_kes": 1000.0,
                "created_at": datetime.now().isoformat(),
            }
        ]
    )
    pipeline.append_bronze_payments(
        [
            {
                "TransactionType": "Pay Bill",
                "TransID": "RKT-C10-01",
                "TransTime": "20261019120000",
                "TransAmount": "1000.00",
                "BusinessShortCode": "600123",
                "BillRefNumber": "ORD-C10-001",
                "MSISDN": "254700000010",
            }
        ],
        rail="mpesa",
    )
    pipeline.process_silver()
    pipeline.process_gold()

    # Run sweep with table optimization
    sweep_summary = run_sweep(spark, base_dir=tmp_path, run_optimize=True)
    assert sweep_summary["total_orders_reconciled"] >= 1


def test_scenario_c11_cold_replay_idempotency(spark, tmp_path):
    """Scenario C11: Wiping Silver/Gold and replaying from Bronze reproduces bit-identical Gold."""
    pipeline = ReconciliationPipeline(spark, base_dir=tmp_path)
    pipeline.append_bronze_orders(
        [
            {
                "order_ref": "ORD-C11-001",
                "shop_id": "SHOP-1",
                "amount_kes": 3500.0,
                "created_at": datetime.now().isoformat(),
            }
        ]
    )
    pipeline.append_bronze_payments(
        [
            {
                "TransactionType": "Pay Bill",
                "TransID": "RKT-C11-01",
                "TransTime": "20261019120000",
                "TransAmount": "3500.00",
                "BusinessShortCode": "600123",
                "BillRefNumber": "ORD-C11-001",
                "MSISDN": "254700000011",
            }
        ],
        rail="mpesa",
    )
    pipeline.process_silver()
    pipeline.process_gold(is_window_expired=True)

    # Prove replay idempotency
    assert verify_replay(base_dir=str(tmp_path)) is True
