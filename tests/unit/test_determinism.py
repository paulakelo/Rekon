"""Determinism and reproducibility tests for the simulator (M3)."""

import json

import httpx

from simulator.chaos import ChaosInjector
from simulator.cli import generate_dataset, stream_dataset
from simulator.clock import VirtualClock
from simulator.orders import OrderGenerator
from simulator.payments import PaymentGenerator


def test_simulator_seed_determinism(tmp_path):
    """M3 Proof: Generating dataset twice with identical seed produces identical output."""
    dir1 = tmp_path / "run1"
    dir2 = tmp_path / "run2"

    run1 = generate_dataset(num_orders=25, seed=42, output_dir=dir1)
    run2 = generate_dataset(num_orders=25, seed=42, output_dir=dir2)

    # 1. Compare in-memory structures
    assert json.dumps(run1, sort_keys=True) == json.dumps(run2, sort_keys=True)

    # 2. Compare serialized JSON files on disk
    for filename in [
        "orders.json",
        "payments_mpesa.json",
        "payments_bank.json",
        "chaos_events.json",
    ]:
        content1 = (dir1 / filename).read_text()
        content2 = (dir2 / filename).read_text()
        assert content1 == content2, f"Discrepancy found in {filename} between identical seeds"


def test_simulator_different_seed_divergence(tmp_path):
    """Different seeds produce distinct simulated sequences."""
    run1 = generate_dataset(num_orders=15, seed=42, output_dir=tmp_path / "seed42")
    run2 = generate_dataset(num_orders=15, seed=99, output_dir=tmp_path / "seed99")

    assert json.dumps(run1["orders"]) != json.dumps(run2["orders"])


def test_dukastack_order_reference_format():
    """Verify generated order references follow dukastack format: ORD-xxxxxxxx."""
    clock = VirtualClock()
    order_gen = OrderGenerator(clock=clock, seed=123)

    orders = order_gen.generate_batch(10)
    for i, order in enumerate(orders, start=1):
        expected_ref = f"ORD-{i:08d}"
        assert order["order_ref"] == expected_ref
        assert order["amount_kes"] >= 350.0
        assert "SHOP-" in order["shop_id"]


def test_all_chaos_injections_coverage():
    """Verify all chaos methods generate valid structures."""
    clock = VirtualClock()
    payment_gen = PaymentGenerator(clock=clock, seed=42)
    chaos = ChaosInjector(clock=clock, payment_gen=payment_gen)

    # C1: Exact duplicates
    p = payment_gen.generate_mpesa_payment("ORD-00000001", 1500.0)
    dupes = chaos.create_exact_duplicates(p, count=4)
    assert len(dupes) == 4
    assert all(d["TransID"] == p["TransID"] for d in dupes)

    # C2: Conflict duplicate
    conflicts = chaos.create_duplicate_conflict(p, rail="mpesa", diff_ratio=2.0)
    assert len(conflicts) == 2
    assert conflicts[0]["TransID"] == conflicts[1]["TransID"]
    assert float(conflicts[1]["TransAmount"]) == float(conflicts[0]["TransAmount"]) * 2.0

    # C3: Inversion / Pre-payment
    inv_pay, inv_order = chaos.create_inversion("ORD-00000099", 2500.0, hours_early=3.0)
    assert inv_pay["BillRefNumber"] == "ORD-00000099"
    assert float(inv_pay["TransAmount"]) == 2500.0
    assert inv_order["order_ref"] == "ORD-00000099"
    assert inv_order["amount_kes"] == 2500.0

    # C6: Split payment
    splits = chaos.create_split_payments("ORD-00000001", 5000.0, ratios=[0.3, 0.7])
    assert len(splits) == 2
    sum_split = sum(float(s["TransAmount"]) for s in splits)
    assert round(sum_split, 2) == 5000.0

    # C7: Underpayment
    under = chaos.create_underpayment("ORD-00000001", 4000.0, ratio=0.75)
    assert float(under["TransAmount"]) == 3000.0

    # C8: Overpayment
    over = chaos.create_overpayment("ORD-00000001", 2000.0, ratio=1.5)
    assert over["amount"] == 3000.0

    # C5: Orphan payment
    orphan = chaos.create_orphan_payment(3200.0)
    assert orphan["BillRefNumber"] == "ORD-99999999"

    # C9: Malformed payload
    malformed = chaos.create_malformed_payload()
    assert "UNCLOSED_JSON" in malformed


def test_stream_dataset_dry_run():
    """Test that stream_dataset executes successfully in dry-run mode."""
    summary = stream_dataset(num_orders=10, seed=42, dry_run=True, interval=0)
    assert summary["orders_sent"] == 11  # 10 nominal + 1 C3 early order
    assert summary["mpesa_sent"] > 0
    assert summary["bank_sent"] > 0
    assert summary["malformed_sent"] == 1
    assert summary["errors"] == 0


def test_stream_dataset_mock_transport():
    """Test that stream_dataset transmits expected HTTP requests."""
    recorded_requests = []

    def mock_handler(request: httpx.Request) -> httpx.Response:
        recorded_requests.append(request.url.path)
        if request.url.path == "/health":
            return httpx.Response(200, json={"status": "ok", "service": "rekon-receiver"})
        elif request.url.path == "/api/v1/orders":
            return httpx.Response(201, json={"status": "order_queued"})
        elif request.url.path in ("/api/v1/payments/mpesa", "/api/v1/payments/bank"):
            return httpx.Response(200, json={"status": "received"})
        return httpx.Response(404)

    transport = httpx.MockTransport(mock_handler)
    original_client_init = httpx.Client.__init__

    def patched_init(self, *args, **kwargs):
        kwargs["transport"] = transport
        original_client_init(self, *args, **kwargs)

    try:
        httpx.Client.__init__ = patched_init
        summary = stream_dataset(
            num_orders=5, seed=42, target_url="http://mock-receiver", interval=0
        )
        assert summary["errors"] == 0
        assert "/health" in recorded_requests
        assert "/api/v1/orders" in recorded_requests
        assert "/api/v1/payments/mpesa" in recorded_requests
    finally:
        httpx.Client.__init__ = original_client_init
