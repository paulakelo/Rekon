"""Gold layer analytical marts, financial reconciliation, and anomaly engine.

Generates the core financial ledger and business mart tables in Delta Lake:
1. gold_order_payment_status: Order reconciliation grain (UNPAID, PARTIAL, PAID, OVERPAID).
2. gold_payment_anomalies: Operational exceptions (OVERPAID, UNDERPAID, LATE_PAYMENT, ORPHANS).
3. gold_reconciliation_daily: Daily billed vs collected cash summaries.
4. gold_rail_throughput_15m: 15-minute event-time tumbling window metrics per payment rail.
"""

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from streaming.common.schemas import (
    GOLD_ANOMALIES_SCHEMA,
    GOLD_ORDER_STATUS_SCHEMA,
    SILVER_ORDERS_SCHEMA,
    SILVER_PAYMENTS_SCHEMA,
)
from streaming.common.status import (
    AnomalyType,
    check_lateness,
    determine_order_status,
)
from streaming.pipeline import ensure_java_compatibility, get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("rekon.streaming.gold")


def compute_gold_order_status(
    spark: SparkSession,
    silver_orders_path: str,
    silver_payments_path: str,
    gold_status_path: str,
    gold_anomalies_path: str,
    is_window_expired: bool = False,
    current_time: datetime | None = None,
) -> tuple[DataFrame, list[dict[str, Any]]]:
    """Compute gold_order_payment_status and detect financial anomalies."""
    now = current_time or datetime.now()

    # Ensure source tables exist
    if not DeltaTable.isDeltaTable(spark, silver_orders_path):
        spark.createDataFrame([], schema=SILVER_ORDERS_SCHEMA).write.format("delta").mode(
            "ignore"
        ).save(silver_orders_path)
    if not DeltaTable.isDeltaTable(spark, silver_payments_path):
        spark.createDataFrame([], schema=SILVER_PAYMENTS_SCHEMA).write.format("delta").mode(
            "ignore"
        ).save(silver_payments_path)

    orders_df = spark.read.format("delta").load(silver_orders_path)
    payments_df = spark.read.format("delta").load(silver_payments_path)

    collected_orders = orders_df.collect()
    payments_by_order: dict[str, list[Any]] = {}
    for p in payments_df.collect():
        payments_by_order.setdefault(p["order_ref"], []).append(p)

    gold_status_rows = []
    anomalies_to_add = []
    ano_counter = 0

    # 1. Reconcile known orders
    for o in collected_orders:
        order_ref = o["order_ref"]
        shop_id = o["shop_id"]
        order_amount = round(float(o["amount_kes"]), 2)
        created_at = o["created_at"]

        matching_payments = payments_by_order.get(order_ref, [])
        payment_count = len(matching_payments)

        if payment_count > 0:
            paid_total = round(sum(float(p["amount_kes"]) for p in matching_payments), 2)
            first_paid_at = min(p["paid_at"] for p in matching_payments)
            last_paid_at = max(p["paid_at"] for p in matching_payments)
        else:
            paid_total = 0.0
            first_paid_at = None
            last_paid_at = None

        status = determine_order_status(
            order_amount=order_amount,
            paid_total=paid_total,
            is_window_expired=is_window_expired,
        )

        settled_at = last_paid_at if status == "PAID" else None

        gold_status_rows.append(
            {
                "order_ref": order_ref,
                "shop_id": shop_id,
                "order_amount_kes": order_amount,
                "paid_total_kes": paid_total,
                "payment_count": payment_count,
                "status": status,
                "first_paid_at": first_paid_at,
                "last_paid_at": last_paid_at,
                "settled_at": settled_at,
                "last_updated_at": now,
            }
        )

        # Detect Overpayment Anomaly
        if status == "OVERPAID":
            ano_counter += 1
            anomalies_to_add.append(
                {
                    "anomaly_id": f"ANO-OVP-{now.strftime('%Y%m%d%H%M%S')}-{ano_counter:03d}",
                    "anomaly_type": AnomalyType.OVERPAID,
                    "rail": "ALL",
                    "rail_receipt_id": None,
                    "order_ref": order_ref,
                    "detected_at": now,
                    "details": json.dumps(
                        {
                            "order_amount": order_amount,
                            "paid_total": paid_total,
                            "excess": round(paid_total - order_amount, 2),
                        }
                    ),
                }
            )

        # Detect Underpayment Anomaly (finalized after window expiry)
        if status == "UNDERPAID":
            ano_counter += 1
            anomalies_to_add.append(
                {
                    "anomaly_id": f"ANO-UND-{now.strftime('%Y%m%d%H%M%S')}-{ano_counter:03d}",
                    "anomaly_type": AnomalyType.UNDERPAID,
                    "rail": "ALL",
                    "rail_receipt_id": None,
                    "order_ref": order_ref,
                    "detected_at": now,
                    "details": json.dumps(
                        {
                            "order_amount": order_amount,
                            "paid_total": paid_total,
                            "deficit": round(order_amount - paid_total, 2),
                        }
                    ),
                }
            )

        # Detect Late Payment Anomaly (> 48h nominal window)
        if last_paid_at and check_lateness(last_paid_at, created_at, window_hours=48.0):
            ano_counter += 1
            anomalies_to_add.append(
                {
                    "anomaly_id": f"ANO-LAT-{now.strftime('%Y%m%d%H%M%S')}-{ano_counter:03d}",
                    "anomaly_type": AnomalyType.LATE_PAYMENT,
                    "rail": "ALL",
                    "rail_receipt_id": None,
                    "order_ref": order_ref,
                    "detected_at": now,
                    "details": json.dumps(
                        {"paid_at": str(last_paid_at), "order_created_at": str(created_at)}
                    ),
                }
            )

    # 2. Detect Orphan Payments (payments referencing nonexistent orders)
    all_order_refs = {o["order_ref"] for o in collected_orders}
    for order_ref, p_list in payments_by_order.items():
        if order_ref not in all_order_refs:
            for p in p_list:
                ano_counter += 1
                anomalies_to_add.append(
                    {
                        "anomaly_id": f"ANO-ORP-{now.strftime('%Y%m%d%H%M%S')}-{ano_counter:03d}",
                        "anomaly_type": AnomalyType.ORPHAN_PAYMENT,
                        "rail": p["rail"],
                        "rail_receipt_id": p["rail_receipt_id"],
                        "order_ref": order_ref,
                        "detected_at": now,
                        "details": json.dumps(
                            {"amount_kes": p["amount_kes"], "paid_at": str(p["paid_at"])}
                        ),
                    }
                )

    status_df = spark.createDataFrame(gold_status_rows, schema=GOLD_ORDER_STATUS_SCHEMA)
    status_df.write.format("delta").mode("overwrite").save(gold_status_path)

    if anomalies_to_add:
        ano_df = spark.createDataFrame(anomalies_to_add, schema=GOLD_ANOMALIES_SCHEMA)
        ano_df.write.format("delta").mode("append").save(gold_anomalies_path)
        logger.info("Recorded %d new anomalies in %s", len(anomalies_to_add), gold_anomalies_path)

    logger.info(
        "Gold order reconciliation completed: %d orders processed, %d anomalies.",
        len(gold_status_rows),
        len(anomalies_to_add),
    )
    return status_df, anomalies_to_add


def compute_gold_daily_reconciliation(
    spark: SparkSession,
    gold_status_path: str,
    gold_daily_path: str,
) -> DataFrame:
    """Aggregate daily billed vs collected cash summaries from gold_order_payment_status."""
    if not DeltaTable.isDeltaTable(spark, gold_status_path):
        spark.createDataFrame([], schema=GOLD_ORDER_STATUS_SCHEMA).write.format("delta").mode(
            "ignore"
        ).save(gold_status_path)

    status_df = spark.read.format("delta").load(gold_status_path)

    daily_df = status_df.groupBy(
        F.to_date(F.coalesce(F.col("first_paid_at"), F.col("last_updated_at"))).alias("recon_date")
    ).agg(
        F.round(F.sum("order_amount_kes"), 2).alias("billed_amount_kes"),
        F.round(F.sum("paid_total_kes"), 2).alias("collected_amount_kes"),
        F.count("order_ref").alias("billed_orders_count"),
        F.sum(F.when(F.col("status") == "PAID", 1).otherwise(0)).alias("paid_orders_count"),
        F.sum(F.when(F.col("status") == "PARTIAL", 1).otherwise(0)).alias("partial_orders_count"),
        F.sum(F.when(F.col("status") == "UNPAID", 1).otherwise(0)).alias("unpaid_orders_count"),
        F.sum(F.when(F.col("status") == "OVERPAID", 1).otherwise(0)).alias("overpaid_orders_count"),
        F.sum(F.when(F.col("status") == "UNDERPAID", 1).otherwise(0)).alias(
            "underpaid_orders_count"
        ),
    )

    daily_df.write.format("delta").mode("overwrite").save(gold_daily_path)
    logger.info("Updated gold_reconciliation_daily in %s", gold_daily_path)
    return daily_df


def compute_gold_rail_throughput(
    spark: SparkSession,
    silver_payments_path: str,
    gold_throughput_path: str,
) -> DataFrame:
    """Compute 15-minute event-time tumbling window metrics per payment rail."""
    if not DeltaTable.isDeltaTable(spark, silver_payments_path):
        spark.createDataFrame([], schema=SILVER_PAYMENTS_SCHEMA).write.format("delta").mode(
            "ignore"
        ).save(silver_payments_path)

    silver_payments_df = spark.read.format("delta").load(silver_payments_path)

    throughput_df = (
        silver_payments_df.groupBy(
            "rail",
            F.window("paid_at", "15 minutes"),
        )
        .agg(
            F.count("rail_receipt_id").alias("transaction_count"),
            F.round(F.sum("amount_kes"), 2).alias("total_amount_kes"),
            F.round(F.avg("amount_kes"), 2).alias("avg_amount_kes"),
        )
        .select(
            F.col("rail"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("transaction_count"),
            F.col("total_amount_kes"),
            F.col("avg_amount_kes"),
        )
    )

    throughput_df.write.format("delta").mode("overwrite").save(gold_throughput_path)
    logger.info("Updated gold_rail_throughput_15m in %s", gold_throughput_path)
    return throughput_df


def run_gold_pipeline(
    spark: SparkSession,
    base_dir: Path | str = "data/delta",
    is_window_expired: bool = False,
) -> dict[str, Any]:
    """Execute all Gold marts computations and return summary results."""
    base_path = Path(base_dir)

    silver_orders_path = str(base_path / "silver_orders")
    silver_payments_path = str(base_path / "silver_payments")
    gold_status_path = str(base_path / "gold_order_payment_status")
    gold_anomalies_path = str(base_path / "gold_payment_anomalies")
    gold_daily_path = str(base_path / "gold_reconciliation_daily")
    gold_throughput_path = str(base_path / "gold_rail_throughput_15m")

    status_df, anomalies = compute_gold_order_status(
        spark=spark,
        silver_orders_path=silver_orders_path,
        silver_payments_path=silver_payments_path,
        gold_status_path=gold_status_path,
        gold_anomalies_path=gold_anomalies_path,
        is_window_expired=is_window_expired,
    )

    daily_df = compute_gold_daily_reconciliation(
        spark=spark,
        gold_status_path=gold_status_path,
        gold_daily_path=gold_daily_path,
    )

    throughput_df = compute_gold_rail_throughput(
        spark=spark,
        silver_payments_path=silver_payments_path,
        gold_throughput_path=gold_throughput_path,
    )

    return {
        "status_df": status_df,
        "daily_df": daily_df,
        "throughput_df": throughput_df,
        "anomalies_count": len(anomalies),
        "gold_status_path": gold_status_path,
        "gold_anomalies_path": gold_anomalies_path,
        "gold_daily_path": gold_daily_path,
        "gold_throughput_path": gold_throughput_path,
    }


def main():
    parser = argparse.ArgumentParser(description="Rekon Gold Reconciliation & Anomaly Engine")
    parser.add_argument(
        "--base-dir", type=str, default="data/delta", help="Delta Lake base storage directory"
    )
    parser.add_argument(
        "--sweep", action="store_true", help="Execute sweep mode finalizing expired windows"
    )

    args = parser.parse_args()
    ensure_java_compatibility()
    spark = get_spark_session("rekon-gold-engine")
    run_gold_pipeline(
        spark=spark,
        base_dir=args.base_dir,
        is_window_expired=args.sweep,
    )


if __name__ == "__main__":
    main()
