"""Replay verification: Rebuilds Silver and Gold from Bronze to prove idempotency."""

import logging
import shutil
from pathlib import Path

from streaming.pipeline import ReconciliationPipeline, get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("rekon.streaming.replay")


def verify_replay(base_dir: str = "data/delta") -> bool:
    """Wipe Silver and Gold tables, then replay from Bronze to verify identical output."""
    base_path = Path(base_dir)
    bronze_orders = base_path / "bronze_orders"
    bronze_payments = base_path / "bronze_payments"

    if not bronze_orders.exists() or not bronze_payments.exists():
        logger.error("Bronze tables do not exist. Run 'make demo' first.")
        return False

    spark = get_spark_session("rekon-replay")

    # 1. Snapshot original gold_order_payment_status
    orig_status_path = base_path / "gold_order_payment_status"
    orig_df = spark.read.format("delta").load(str(orig_status_path))
    original_rows = sorted(
        orig_df.select(
            "order_ref", "order_amount_kes", "paid_total_kes", "payment_count", "status"
        ).collect(),
        key=lambda r: r["order_ref"],
    )

    logger.info("Captured original Gold status: %d rows.", len(original_rows))

    # 2. Wipe Silver and Gold directories (simulating cold rebuild from Bronze)
    logger.info("Wiping Silver and Gold directories to simulate cold recovery...")
    for target in [
        base_path / "silver_orders",
        base_path / "silver_payments",
        base_path / "silver_rejects",
        base_path / "gold_order_payment_status",
        base_path / "gold_payment_anomalies",
        base_path / "gold_reconciliation_daily",
        base_path / "gold_rail_throughput_15m",
    ]:
        if target.exists():
            shutil.rmtree(target)

    # 3. Re-execute pipeline exclusively from Bronze
    logger.info("Replaying entire lakehouse from Bronze Delta tables...")
    pipeline = ReconciliationPipeline(spark, base_dir=base_path)
    pipeline.process_silver()
    pipeline.process_gold(is_window_expired=True)

    # 4. Compare replayed Gold status against original snapshot
    replayed_df = spark.read.format("delta").load(str(orig_status_path))
    replayed_rows = sorted(
        replayed_df.select(
            "order_ref", "order_amount_kes", "paid_total_kes", "payment_count", "status"
        ).collect(),
        key=lambda r: r["order_ref"],
    )

    if len(original_rows) != len(replayed_rows):
        logger.error(
            "Replay mismatch: original had %d rows, replayed has %d rows.",
            len(original_rows),
            len(replayed_rows),
        )
        return False

    for orig, rep in zip(original_rows, replayed_rows, strict=True):
        if orig.asDict() != rep.asDict():
            logger.error("Row discrepancy found:\n  Original: %s\n  Replayed: %s", orig, rep)
            return False

    print("\n" + "=" * 70)
    print(" ✅ REPLAY VERIFICATION PASSED (FR-10, Scenario C11)")
    print(f" Verified {len(replayed_rows)} orders recomputed with bit-for-bit identity.")
    print("=" * 70 + "\n")
    return True


if __name__ == "__main__":
    success = verify_replay()
    if not success:
        exit(1)
