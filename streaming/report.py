"""Terminal report generator for Gold reconciliation tables."""

import sys
from pathlib import Path

from streaming.pipeline import get_spark_session


def generate_report(base_dir: str = "data/delta"):
    """Read Gold Delta tables and print formatted financial reconciliation summary."""
    base_path = Path(base_dir)
    status_path = base_path / "gold_order_payment_status"

    if not status_path.exists():
        print(f"Error: No reconciled tables found at {base_path}. Run 'make demo' first.")
        sys.exit(1)

    spark = get_spark_session("rekon-reporter")

    print("\n" + "=" * 90)
    print(" 📊 REKON: PAYMENT RECONCILIATION & ANOMALIES REPORT")
    print("=" * 90)

    # 1. Daily Reconciliation Summary (Q-4)
    daily_path = base_path / "gold_reconciliation_daily"
    if daily_path.exists():
        daily_df = spark.read.format("delta").load(str(daily_path))
        print("\n--- [Q-4] DAILY RECONCILIATION LEDGER (BILLED VS COLLECTED) ---")
        rows = daily_df.collect()
        for r in rows:
            billed = r["billed_amount_kes"] or 0.0
            collected = r["collected_amount_kes"] or 0.0
            recovery_rate = (collected / billed * 100) if billed > 0 else 0.0
            print(f" Recon Date         : {r['recon_date']}")
            print(f" Billed Revenue     : KES {billed:,.2f} ({r['billed_orders_count']} orders)")
            print(
                f" Collected Cash     : KES {collected:,.2f} ({recovery_rate:.1f}% settlement rate)"
            )
            print(f" Paid Orders        : {r['paid_orders_count']}")
            print(f" Partial Orders     : {r['partial_orders_count']}")
            print(f" Underpaid Orders   : {r['underpaid_orders_count']}")
            print(f" Overpaid Orders    : {r['overpaid_orders_count']}")
            print(f" Unpaid Orders      : {r['unpaid_orders_count']}")

    # 2. Reconciled Order Payment Status (Q-1)
    status_df = spark.read.format("delta").load(str(status_path))
    print("\n--- [Q-1] ORDER PAYMENT STATUS (SAMPLE AUDIT TRAIL) ---")
    hdr = (
        f"{'Order Ref':<15} {'Shop ID':<22} {'Billed KES':<12} "
        f"{'Paid KES':<12} {'Count':<6} {'Status':<12}"
    )
    print(hdr)
    print("-" * 85)
    sample_orders = status_df.limit(15).collect()
    for o in sample_orders:
        paid_val = o["paid_total_kes"] or 0.0
        print(
            f"{o['order_ref']:<15} {o['shop_id'][:20]:<22} {o['order_amount_kes']:>10.2f} "
            f"{paid_val:>10.2f} {o['payment_count']:>5}   {o['status']:<12}"
        )

    # 3. Detected Payment Anomalies (Q-2)
    anomalies_path = base_path / "gold_payment_anomalies"
    if anomalies_path.exists():
        ano_df = spark.read.format("delta").load(str(anomalies_path))
        print("\n--- [Q-2] DETECTED PAYMENT ANOMALIES WITH REASON CODES ---")
        print(f"{'Anomaly ID':<14} {'Anomaly Type':<22} {'Rail':<8} {'Order Ref':<15} {'Details'}")
        print("-" * 90)
        anomalies = ano_df.collect()
        for a in anomalies:
            details_snippet = (a["details"] or "")[:40]
            print(
                f"{a['anomaly_id']:<14} {a['anomaly_type']:<22} {str(a['rail']):<8} "
                f"{str(a['order_ref']):<15} {details_snippet}"
            )

    # 4. Rail Throughput & Lateness (Q-3)
    throughput_path = base_path / "gold_rail_throughput_15m"
    if throughput_path.exists():
        tput_df = spark.read.format("delta").load(str(throughput_path))
        print("\n--- [Q-3] RAIL THROUGHPUT (15-MINUTE EVENT-TIME WINDOWS) ---")
        print(
            f"{'Rail':<10} {'Window Start':<20} {'Window End':<20} {'Tx Count':<10} {'Total KES'}"
        )
        print("-" * 75)
        for t in tput_df.limit(10).collect():
            w_start = str(t["window_start"])[:19]
            w_end = str(t["window_end"])[:19]
            amt = t["total_amount_kes"]
            cnt = t["transaction_count"]
            print(f"{t['rail']:<10} {w_start:<20} {w_end:<20} {cnt:<10} {amt:>10.2f}")

    # 5. Silver Dead-Letter Rejects (FR-11)
    rejects_path = base_path / "silver_rejects"
    if rejects_path.exists():
        rej_df = spark.read.format("delta").load(str(rejects_path))
        rej_msg = (
            f"\n--- [FR-11] DEAD-LETTER REJECTS: {rej_df.count()} malformed payloads "
            "quarantined in silver_rejects ---"
        )
        print(rej_msg)

    print("\n" + "=" * 90 + "\n")


if __name__ == "__main__":
    generate_report()
