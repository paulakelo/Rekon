"""Chaos scenario tests verifying resilience against all payment rail edge cases."""

import pytest

from simulator.chaos import ChaosInjector
from simulator.clock import VirtualClock
from simulator.orders import OrderGenerator
from simulator.payments import PaymentGenerator
from streaming.pipeline import ReconciliationPipeline, get_spark_session


@pytest.fixture(scope="module")
def spark():
    """Shared SparkSession fixture for scenario testing."""
    return get_spark_session("rekon-scenario-tests")


def test_scenario_c1_exact_duplicates(spark, tmp_path):
    """Scenario C1: Verify exact duplicate deliveries yield exactly 1 Silver row."""
    clock = VirtualClock()
    payment_gen = PaymentGenerator(clock)
    chaos = ChaosInjector(clock, payment_gen)

    # Generate single payment and duplicate 3 times
    p = payment_gen.generate_mpesa_payment(order_ref="ORD-00000001", amount=2500.0)
    dupes = chaos.create_exact_duplicates(p, count=3)

    pipeline = ReconciliationPipeline(spark, base_dir=tmp_path)
    pipeline.append_bronze_orders(
        [
            {
                "order_ref": "ORD-00000001",
                "shop_id": "SHOP-1",
                "amount_kes": 2500.0,
                "created_at": clock.now_iso(),
            }
        ]
    )
    pipeline.append_bronze_payments(dupes, rail="mpesa")

    pipeline.process_silver()

    silver_payments = spark.read.format("delta").load(pipeline.silver_payments_path)
    # Even though 3 identical records were ingested in Bronze, Silver must have exactly 1
    assert silver_payments.filter("rail_receipt_id == 'RKT0000001'").count() == 1


def test_scenario_c6_split_payment(spark, tmp_path):
    """Scenario C6: Verify split payments transition from PARTIAL to PAID."""
    clock = VirtualClock()
    order_gen = OrderGenerator(clock)
    payment_gen = PaymentGenerator(clock)
    chaos = ChaosInjector(clock, payment_gen)

    order = order_gen.next_order(fixed_amount=3000.0)
    order_ref = order["order_ref"]

    # Split into 1,200 and 1,800 KES
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
