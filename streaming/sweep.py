"""Periodic sweep and maintenance job for Rekon lakehouse (M7).

Responsibilities:
1. Finalizes expired payment windows: transitions lingering PARTIAL orders to UNDERPAID.
2. Detects orphan payments referencing nonexistent order references.
3. Performs Delta Lake maintenance: executes OPTIMIZE on tables to compact small files.
4. Emits operational telemetry and reconciliation summary metrics.
"""

import argparse
import logging
from pathlib import Path
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from streaming.gold import run_gold_pipeline
from streaming.pipeline import ensure_java_compatibility, get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("rekon.streaming.sweep")


def optimize_delta_tables(spark: SparkSession, base_dir: Path | str) -> list[str]:
    """Execute OPTIMIZE on all active Delta Lake tables to compact small files."""
    base_path = Path(base_dir)
    tables_to_optimize = [
        "bronze_orders",
        "bronze_payments",
        "silver_orders",
        "silver_payments",
        "silver_rejects",
        "gold_order_payment_status",
        "gold_payment_anomalies",
        "gold_reconciliation_daily",
        "gold_rail_throughput_15m",
    ]
    optimized = []

    for tbl_name in tables_to_optimize:
        tbl_path = base_path / tbl_name
        if tbl_path.exists() and DeltaTable.isDeltaTable(spark, str(tbl_path)):
            try:
                logger.info("Optimizing Delta table: %s", tbl_name)
                spark.sql(f"OPTIMIZE delta.`{tbl_path.resolve()}`")
                optimized.append(tbl_name)
            except Exception as exc:
                logger.warning("Failed to optimize %s: %s", tbl_name, exc)

    return optimized


def run_sweep(
    spark: SparkSession,
    base_dir: Path | str = "data/delta",
    run_optimize: bool = True,
) -> dict[str, Any]:
    """Execute periodic reconciliation sweep and table optimization."""
    logger.info("Executing Rekon periodic reconciliation sweep on %s...", base_dir)

    # 1. Finalize expired payment windows and detect orphan payments
    gold_results = run_gold_pipeline(
        spark=spark,
        base_dir=base_dir,
        is_window_expired=True,
    )

    status_df = gold_results["status_df"]
    underpaid_count = status_df.filter(status_df.status == "UNDERPAID").count()
    total_orders = status_df.count()

    # 2. Compact small Delta files
    optimized_tables = []
    if run_optimize:
        optimized_tables = optimize_delta_tables(spark, base_dir)

    summary = {
        "total_orders_reconciled": total_orders,
        "underpaid_orders_finalized": underpaid_count,
        "anomalies_detected": gold_results["anomalies_count"],
        "optimized_tables": optimized_tables,
    }

    print("\n" + "=" * 70)
    print(" 🧹 REKON PERIODIC SWEEP & MAINTENANCE COMPLETE (M7)")
    print(f" Total Orders Evaluated     : {summary['total_orders_reconciled']}")
    print(f" Underpaid Orders Finalized : {summary['underpaid_orders_finalized']}")
    print(f" Total Anomalies Logged     : {summary['anomalies_detected']}")
    print(f" Delta Tables Compacted     : {len(summary['optimized_tables'])}")
    print("=" * 70 + "\n")

    logger.info("Sweep execution summary: %s", summary)
    return summary


def main():
    parser = argparse.ArgumentParser(description="Rekon Periodic Sweep and Maintenance Job")
    parser.add_argument(
        "--base-dir", type=str, default="data/delta", help="Base storage directory for Delta tables"
    )
    parser.add_argument(
        "--skip-optimize", action="store_true", help="Skip Delta OPTIMIZE compaction"
    )

    args = parser.parse_args()
    ensure_java_compatibility()
    spark = get_spark_session("rekon-sweep-job")
    run_sweep(
        spark=spark,
        base_dir=args.base_dir,
        run_optimize=not args.skip_optimize,
    )


if __name__ == "__main__":
    main()
