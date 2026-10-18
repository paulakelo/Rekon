"""Command-line interface for the Rekon payment stream simulator."""

import argparse
import json
import logging
from pathlib import Path

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
    # Payment for an order that will be generated later
    early_order_ref = f"ORD-{(num_orders + 1):08d}"
    early_pay = payment_gen.generate_mpesa_payment(early_order_ref, 4500.0)
    mpesa_payments.insert(0, early_pay)
    chaos_events.append({"scenario": "C3_INVERSION_PRE_PAYMENT", "order_ref": early_order_ref})

    # Add the corresponding order later in the sequence
    clock.advance_hours(2.0)
    early_order = {
        "order_ref": early_order_ref,
        "shop_id": "SHOP-002-WESTLANDS",
        "amount_kes": 4500.0,
        "currency": "KES",
        "created_at": clock.now_iso(),
    }
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

    demo_parser = subparsers.add_parser("demo", help="Run end-to-end demo reconciliation flow")
    demo_parser.add_argument("--orders", type=int, default=25, help="Number of orders to demo")
    demo_parser.add_argument("--seed", type=int, default=42, help="Random seed for demo")

    args = parser.parse_args()

    if args.command == "generate":
        generate_dataset(num_orders=args.orders, seed=args.seed, output_dir=Path(args.output))
    elif args.command == "demo":
        run_demo(seed=args.seed, num_orders=args.orders)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
