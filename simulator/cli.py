"""Command-line interface for the Rekon payment stream simulator.

Provides subcommands:
- generate: Produce deterministic JSON files for offline testing and replays.
- stream: Stream events in real-time over HTTP to the edge receiver.
- demo: Run the local PySpark reconciliation pipeline against simulated data.
"""

import argparse
import json
import logging
import time
from pathlib import Path
from typing import Any

from simulator.chaos import ChaosInjector
from simulator.clock import VirtualClock
from simulator.orders import OrderGenerator
from simulator.payments import PaymentGenerator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("rekon.simulator.cli")


def generate_dataset(
    num_orders: int = 50, seed: int = 42, output_dir: Path | None = None
) -> dict[str, list]:
    """Generate a deterministic dataset of orders, payments, and chaos events."""
    if output_dir is None:
        output_dir = Path("data/simulated")
    output_dir.mkdir(parents=True, exist_ok=True)

    clock = VirtualClock(start_iso="2026-10-18T08:00:00Z")
    order_gen = OrderGenerator(clock=clock, seed=seed)
    payment_gen = PaymentGenerator(clock=clock, seed=seed)
    chaos = ChaosInjector(clock=clock, payment_gen=payment_gen)

    orders = []
    mpesa_payments = []
    bank_payments = []
    chaos_events = []

    logger.info("Generating %d orders with seed %d...", num_orders, seed)

    for i in range(1, num_orders + 1):
        order = order_gen.next_order()
        orders.append(order)
        order_ref = order["order_ref"]
        amount = order["amount_kes"]

        # Deterministic scenario assignment based on order index:
        if i == 1:
            # C1: Exact Duplicate Redelivery (3 identical M-Pesa callbacks)
            p = payment_gen.generate_mpesa_payment(order_ref, amount)
            dupes = chaos.create_exact_duplicates(p, count=3)
            mpesa_payments.extend(dupes)
            chaos_events.append(
                {"scenario": "C1_EXACT_DUPLICATE", "order_ref": order_ref, "count": 3}
            )

        elif i == 2:
            # C2: Duplicate Conflict (Same receipt, different amount)
            p = payment_gen.generate_mpesa_payment(order_ref, amount)
            conflict_dupes = chaos.create_duplicate_conflict(p, rail="mpesa", diff_ratio=1.5)
            mpesa_payments.extend(conflict_dupes)
            chaos_events.append({"scenario": "C2_DUPLICATE_CONFLICT", "order_ref": order_ref})

        elif i == 3:
            # C6: Split Payment (2 payments summing to order total)
            splits = chaos.create_split_payments(order_ref, amount, rail="mpesa", ratios=[0.4, 0.6])
            mpesa_payments.extend(splits)
            chaos_events.append(
                {"scenario": "C6_SPLIT_PAYMENT", "order_ref": order_ref, "parts": 2}
            )

        elif i == 4:
            # C7: Underpayment (partial payment of 70%, never finished)
            under_pay = chaos.create_underpayment(order_ref, amount, ratio=0.7)
            mpesa_payments.append(under_pay)
            chaos_events.append(
                {"scenario": "C7_UNDERPAYMENT", "order_ref": order_ref, "ratio": 0.7}
            )

        elif i == 5:
            # C8: Overpayment (125% of amount paid via Bank rail)
            over_pay = chaos.create_overpayment(order_ref, amount, ratio=1.25)
            bank_payments.append(over_pay)
            chaos_events.append(
                {"scenario": "C8_OVERPAYMENT", "order_ref": order_ref, "ratio": 1.25}
            )

        elif i == 6:
            # C4: Late Payment (paid 52 hours later, after nominal window)
            late_pay = chaos.create_late_payment(order_ref, amount, hours_late=52.0)
            mpesa_payments.append(late_pay)
            chaos_events.append(
                {"scenario": "C4_LATE_PAYMENT", "order_ref": order_ref, "hours_late": 52}
            )

        elif i == 7:
            # Normal Bank payment
            p = payment_gen.generate_bank_payment(order_ref, amount)
            bank_payments.append(p)

        elif i == 8:
            # Unpaid order (customer abandons cart - no payment emitted)
            chaos_events.append({"scenario": "UNPAID_ORDER", "order_ref": order_ref})

        else:
            # Standard single full payment: alternating between M-Pesa (75%) and Bank (25%)
            if i % 4 == 0:
                p = payment_gen.generate_bank_payment(order_ref, amount)
                bank_payments.append(p)
            else:
                p = payment_gen.generate_mpesa_payment(order_ref, amount)
                mpesa_payments.append(p)

        # Advance virtual clock by 5-15 simulated minutes
        clock.advance_minutes(10.0)

    # Invert order & payment for C3 (Pre-payment emitted before order creation):
    early_order_ref = f"ORD-{(num_orders + 1):08d}"
    early_pay, early_order = chaos.create_inversion(early_order_ref, 4500.0, hours_early=2.0)
    mpesa_payments.insert(0, early_pay)
    chaos_events.append({"scenario": "C3_INVERSION_PRE_PAYMENT", "order_ref": early_order_ref})
    orders.append(early_order)

    # C5: Orphan payment (payment referencing nonexistent order)
    orphan = chaos.create_orphan_payment(amount=1850.0)
    mpesa_payments.append(orphan)
    chaos_events.append({"scenario": "C5_ORPHAN_PAYMENT", "order_ref": orphan["BillRefNumber"]})

    # Save to disk for reproducibility
    (output_dir / "orders.json").write_text(json.dumps(orders, indent=2))
    (output_dir / "payments_mpesa.json").write_text(json.dumps(mpesa_payments, indent=2))
    (output_dir / "payments_bank.json").write_text(json.dumps(bank_payments, indent=2))
    (output_dir / "chaos_events.json").write_text(json.dumps(chaos_events, indent=2))

    logger.info(
        "Dataset generated: %d orders, %d M-Pesa payments, %d Bank payments, %d chaos events.",
        len(orders),
        len(mpesa_payments),
        len(bank_payments),
        len(chaos_events),
    )
    return {
        "orders": orders,
        "payments_mpesa": mpesa_payments,
        "payments_bank": bank_payments,
        "chaos_events": chaos_events,
    }


def stream_dataset(
    num_orders: int = 30,
    seed: int = 42,
    target_url: str = "http://localhost:8000",
    interval: float = 0.05,
    dry_run: bool = False,
    include_malformed: bool = True,
) -> dict[str, Any]:
    """Stream simulated events directly to HTTP receiver endpoints.

    Emits events in realistic order:
    1. C3 Pre-payment is emitted before its corresponding order
    2. Regular orders are emitted to /api/v1/orders
    3. M-Pesa callbacks are emitted to /api/v1/payments/mpesa
    4. Bank callbacks are emitted to /api/v1/payments/bank
    5. C5 Orphan payments and C9 Malformed payloads are dispatched
    """
    dataset = generate_dataset(num_orders=num_orders, seed=seed)
    orders = dataset["orders"]
    mpesa_payments = dataset["payments_mpesa"]
    bank_payments = dataset["payments_bank"]

    mpesa_by_order: dict[str, list[dict]] = {}
    for p in mpesa_payments:
        ref = p.get("BillRefNumber", "UNKNOWN") if isinstance(p, dict) else "UNKNOWN"
        mpesa_by_order.setdefault(ref, []).append(p)

    bank_by_order: dict[str, list[dict]] = {}
    for p in bank_payments:
        ref = p.get("order_reference", "UNKNOWN")
        bank_by_order.setdefault(ref, []).append(p)

    summary = {
        "orders_sent": 0,
        "mpesa_sent": 0,
        "bank_sent": 0,
        "malformed_sent": 0,
        "errors": 0,
    }

    client = None
    if not dry_run:
        import httpx

        client = httpx.Client(base_url=target_url, timeout=10.0)
        try:
            resp = client.get("/health")
            resp.raise_for_status()
            logger.info("Connected to receiver at %s: %s", target_url, resp.json())
        except Exception as exc:
            logger.error("Receiver unavailable at %s: %s", target_url, exc)
            raise ConnectionError(f"Receiver at {target_url} unreachable: {exc}") from exc

    def emit_order(o: dict):
        if not dry_run and client:
            resp = client.post("/api/v1/orders", json=o)
            if resp.status_code != 201:
                summary["errors"] += 1
                logger.warning("Order %s failed with %s", o["order_ref"], resp.status_code)
            else:
                summary["orders_sent"] += 1
        else:
            summary["orders_sent"] += 1
        if interval > 0:
            time.sleep(interval)

    def emit_mpesa(p: dict | str):
        if not dry_run and client:
            if isinstance(p, str):
                resp = client.post(
                    "/api/v1/payments/mpesa",
                    content=p,
                    headers={"Content-Type": "application/json"},
                )
                summary["malformed_sent"] += 1
            else:
                resp = client.post("/api/v1/payments/mpesa", json=p)
                summary["mpesa_sent"] += 1
            if resp.status_code != 200:
                summary["errors"] += 1
        else:
            if isinstance(p, str):
                summary["malformed_sent"] += 1
            else:
                summary["mpesa_sent"] += 1
        if interval > 0:
            time.sleep(interval)

    def emit_bank(p: dict):
        if not dry_run and client:
            resp = client.post("/api/v1/payments/bank", json=p)
            if resp.status_code != 200:
                summary["errors"] += 1
            else:
                summary["bank_sent"] += 1
        else:
            summary["bank_sent"] += 1
        if interval > 0:
            time.sleep(interval)

    # 1. C3 Pre-payment: payment arrives before order is created
    early_order_ref = orders[-1]["order_ref"]
    if early_order_ref in mpesa_by_order:
        for p in mpesa_by_order.pop(early_order_ref):
            logger.info(
                "[STREAM] C3 Inversion: Emitting early payment for %s before order creation",
                early_order_ref,
            )
            emit_mpesa(p)

    # 2. Regular orders and their matching payments
    for order in orders[:-1]:
        logger.info(
            "[STREAM] Emitting order %s (%.2f KES)", order["order_ref"], order["amount_kes"]
        )
        emit_order(order)
        ref = order["order_ref"]
        for p in mpesa_by_order.pop(ref, []):
            logger.info("[STREAM] Emitting M-Pesa payment for %s", ref)
            emit_mpesa(p)
        for p in bank_by_order.pop(ref, []):
            logger.info("[STREAM] Emitting Bank payment for %s", ref)
            emit_bank(p)

    # 3. Delayed order for C3
    logger.info("[STREAM] C3 Inversion: Now emitting delayed order %s", early_order_ref)
    emit_order(orders[-1])

    # 4. C5 Orphan payment (BillRefNumber == 'ORD-99999999')
    for p in mpesa_by_order.pop("ORD-99999999", []):
        logger.info("[STREAM] C5 Orphan: Emitting orphan payment referencing ORD-99999999")
        emit_mpesa(p)

    # Any remaining payments
    for _ref, p_list in mpesa_by_order.items():
        for p in p_list:
            emit_mpesa(p)
    for _ref, p_list in bank_by_order.items():
        for p in p_list:
            emit_bank(p)

    # 5. C9 Malformed payload
    if include_malformed:
        malformed = (
            '{"TransactionType": "Pay Bill", "TransID": "RKT-CORRUPTED", '
            '"TransAmount": 500, UNCLOSED_JSON'
        )
        logger.info("[STREAM] C9 Malformed: Emitting malformed payload")
        emit_mpesa(malformed)

    if client:
        client.close()

    logger.info("Streaming complete: %s", summary)
    return summary


def run_demo(seed: int = 42, num_orders: int = 30):
    """Run full demonstration workflow: generate data and execute pipeline."""
    from streaming.pipeline import run_local_pipeline

    dataset = generate_dataset(num_orders=num_orders, seed=seed)
    print("\n" + "=" * 60)
    print("REKON DEMO: STREAMING PAYMENT RECONCILIATION")
    print("=" * 60)
    print(f"Generated {len(dataset['orders'])} orders")
    print(
        f"Generated {len(dataset['payments_mpesa'])} M-Pesa callbacks "
        "(including duplicates & splits)"
    )
    print(f"Generated {len(dataset['payments_bank'])} Bank callbacks")
    print(f"Injected {len(dataset['chaos_events'])} Chaos Scenarios (C1-C8)")
    print("=" * 60 + "\n")

    logger.info("Executing local PySpark & Delta Lake reconciliation pipeline...")
    run_local_pipeline(dataset)


def main():
    parser = argparse.ArgumentParser(description="Rekon Payment Stream Simulator CLI")
    subparsers = parser.add_subparsers(dest="command")

    gen_parser = subparsers.add_parser(
        "generate", help="Generate simulated orders and payment callbacks"
    )
    gen_parser.add_argument("--orders", type=int, default=50, help="Number of orders to generate")
    gen_parser.add_argument("--seed", type=int, default=42, help="Random seed for determinism")
    gen_parser.add_argument("--output", type=str, default="data/simulated", help="Output directory")

    stream_parser = subparsers.add_parser("stream", help="Stream events over HTTP to receiver")
    stream_parser.add_argument("--orders", type=int, default=30, help="Number of orders to stream")
    stream_parser.add_argument("--seed", type=int, default=42, help="Random seed for determinism")
    stream_parser.add_argument(
        "--interval", type=float, default=0.05, help="Interval (seconds) between events"
    )
    stream_parser.add_argument(
        "--target", type=str, default="http://localhost:8000", help="Receiver base URL"
    )
    stream_parser.add_argument(
        "--dry-run", action="store_true", help="Simulate without network transmission"
    )

    demo_parser = subparsers.add_parser("demo", help="Run end-to-end demo reconciliation flow")
    demo_parser.add_argument("--orders", type=int, default=25, help="Number of orders to demo")
    demo_parser.add_argument("--seed", type=int, default=42, help="Random seed for demo")

    args = parser.parse_args()

    if args.command == "generate":
        generate_dataset(num_orders=args.orders, seed=args.seed, output_dir=Path(args.output))
    elif args.command == "stream":
        stream_dataset(
            num_orders=args.orders,
            seed=args.seed,
            target_url=args.target,
            interval=args.interval,
            dry_run=args.dry_run,
        )
    elif args.command == "demo":
        run_demo(seed=args.seed, num_orders=args.orders)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
