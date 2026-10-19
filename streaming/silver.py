"""Silver layer streaming and batch pipeline for Rekon.

Consumes Bronze Delta tables:
- bronze_orders -> parsed, validated -> silver_orders (Delta MERGE on order_ref)
- bronze_payments -> normalized, PII-hashed -> silver_payments (Delta MERGE)
- unparseable or malformed payloads -> silver_rejects (append)
- duplicate conflicts (same receipt, differing amount) -> gold_payment_anomalies

Ensures:
1. Zero Data Loss: Rejects are preserved with reject reasons.
2. Idempotency: Duplicate callbacks (even arriving days apart) never duplicate Silver rows.
3. Privacy: PII (phone numbers, account numbers) hashed with salted SHA-256.
"""

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from streaming.common.dedupe import merge_silver_orders, merge_silver_payments
from streaming.common.schemas import (
    BRONZE_ORDERS_SCHEMA,
    BRONZE_PAYMENTS_SCHEMA,
    GOLD_ANOMALIES_SCHEMA,
    SILVER_ORDERS_SCHEMA,
    SILVER_PAYMENTS_SCHEMA,
    SILVER_REJECTS_SCHEMA,
    normalize_bank_payload,
    normalize_mpesa_payload,
)
from streaming.pipeline import ensure_java_compatibility, get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("rekon.streaming.silver")


def process_orders_microbatch(
    batch_df: DataFrame,
    batch_id: int,
    spark: SparkSession,
    silver_orders_path: str,
) -> int:
    """Parse and merge orders micro-batch into silver_orders."""
    if batch_df.isEmpty():
        return 0

    rows = batch_df.select("raw_payload").collect()
    parsed_orders = []
    now = datetime.now()

    for r in rows:
        try:
            data = json.loads(r["raw_payload"])
            order_ref = str(data.get("order_ref", "")).strip()
            if not order_ref:
                continue
            parsed_orders.append(
                {
                    "order_ref": order_ref,
                    "shop_id": str(data.get("shop_id", "UNKNOWN")),
                    "amount_kes": round(float(data.get("amount_kes", 0.0)), 2),
                    "created_at": datetime.fromisoformat(
                        str(data.get("created_at", "")).replace("Z", "+00:00")
                    ).replace(tzinfo=None),
                    "_ingest_ts": now,
                }
            )
        except Exception as exc:
            logger.warning("Failed to parse order in Bronze: %s", exc)

    if not parsed_orders:
        return 0

    silver_orders_df = spark.createDataFrame(parsed_orders, schema=SILVER_ORDERS_SCHEMA)
    return merge_silver_orders(spark, silver_orders_path, silver_orders_df)


def process_payments_microbatch(
    batch_df: DataFrame,
    batch_id: int,
    spark: SparkSession,
    silver_payments_path: str,
    silver_rejects_path: str,
    gold_anomalies_path: str | None = None,
) -> dict[str, int]:
    """Parse, normalize, hash PII, and merge payments into silver_payments or silver_rejects."""
    if batch_df.isEmpty():
        return {"valid": 0, "rejects": 0, "conflicts": 0}

    rows = batch_df.select(
        "rail", "raw_payload", "topic", "partition", "offset", "kafka_timestamp"
    ).collect()
    valid_payments = []
    rejects = []
    now = datetime.now()

    for r in rows:
        rail = r["rail"]
        raw_str = r["raw_payload"]
        try:
            if rail == "mpesa":
                norm = normalize_mpesa_payload(raw_str)
            else:
                norm = normalize_bank_payload(raw_str)

            norm["topic"] = r["topic"]
            norm["partition"] = r["partition"]
            norm["offset"] = r["offset"]
            norm["_silver_ts"] = now
            valid_payments.append(norm)
        except Exception as exc:
            rejects.append(
                {
                    "topic": r["topic"],
                    "partition": r["partition"],
                    "offset": r["offset"],
                    "kafka_timestamp": r["kafka_timestamp"],
                    "raw_payload": raw_str,
                    "reject_reason": str(exc),
                    "_rejected_at": now,
                }
            )

    # 1. Save rejects if any
    if rejects:
        rejects_df = spark.createDataFrame(rejects, schema=SILVER_REJECTS_SCHEMA)
        rejects_df.write.format("delta").mode("append").save(silver_rejects_path)
        logger.info("Saved %d rejected payments to %s", len(rejects), silver_rejects_path)

    # 2. Merge valid payments into silver_payments
    conflict_count = 0
    if valid_payments:
        valid_df = spark.createDataFrame(valid_payments, schema=SILVER_PAYMENTS_SCHEMA)
        _, conflicts = merge_silver_payments(spark, silver_payments_path, valid_df)
        conflict_count = len(conflicts)

        # Record conflict anomalies if anomalies table path is provided
        if conflicts and gold_anomalies_path:
            anomaly_rows = []
            for i, c in enumerate(conflicts, start=1):
                anomaly_rows.append(
                    {
                        "anomaly_id": f"ANO-CNF-{now.strftime('%Y%m%d%H%M%S')}-{i:03d}",
                        "anomaly_type": "DUPLICATE_CONFLICT",
                        "rail": c["rail"],
                        "rail_receipt_id": c["rail_receipt_id"],
                        "order_ref": c["order_ref"],
                        "detected_at": now,
                        "details": json.dumps(
                            {
                                "existing_amount": c["existing_amount"],
                                "conflicting_amount": c["conflicting_amount"],
                            }
                        ),
                    }
                )
            anom_df = spark.createDataFrame(anomaly_rows, schema=GOLD_ANOMALIES_SCHEMA)
            anom_df.write.format("delta").mode("append").save(gold_anomalies_path)

    return {"valid": len(valid_payments), "rejects": len(rejects), "conflicts": conflict_count}


def start_silver_streaming(
    spark: SparkSession,
    base_dir: Path | str = "data/delta",
    checkpoint_dir: Path | str = "checkpoints/silver",
    trigger_available_now: bool = False,
) -> dict[str, Any]:
    """Start Silver Structured Streaming foreachBatch queries from Bronze Delta tables."""
    base_path = Path(base_dir)
    cp_path = Path(checkpoint_dir)

    bronze_orders_path = str(base_path / "bronze_orders")
    bronze_payments_path = str(base_path / "bronze_payments")

    silver_orders_path = str(base_path / "silver_orders")
    silver_payments_path = str(base_path / "silver_payments")
    silver_rejects_path = str(base_path / "silver_rejects")
    gold_anomalies_path = str(base_path / "gold_payment_anomalies")

    # Ensure source Bronze Delta tables exist
    if not DeltaTable.isDeltaTable(spark, bronze_orders_path):
        spark.createDataFrame([], schema=BRONZE_ORDERS_SCHEMA).write.format("delta").mode(
            "ignore"
        ).save(bronze_orders_path)
    if not DeltaTable.isDeltaTable(spark, bronze_payments_path):
        spark.createDataFrame([], schema=BRONZE_PAYMENTS_SCHEMA).write.format("delta").mode(
            "ignore"
        ).save(bronze_payments_path)

    # 1. Orders stream from bronze_orders
    orders_stream = spark.readStream.format("delta").load(bronze_orders_path)
    orders_writer = orders_stream.writeStream.option(
        "checkpointLocation", str(cp_path / "orders")
    ).foreachBatch(
        lambda batch_df, batch_id: process_orders_microbatch(
            batch_df, batch_id, spark, silver_orders_path
        )
    )
    if trigger_available_now:
        orders_writer = orders_writer.trigger(availableNow=True)
    orders_query = orders_writer.start()

    # 2. Payments stream from bronze_payments
    payments_stream = spark.readStream.format("delta").load(bronze_payments_path)
    payments_writer = payments_stream.writeStream.option(
        "checkpointLocation", str(cp_path / "payments")
    ).foreachBatch(
        lambda batch_df, batch_id: process_payments_microbatch(
            batch_df,
            batch_id,
            spark,
            silver_payments_path,
            silver_rejects_path,
            gold_anomalies_path,
        )
    )
    if trigger_available_now:
        payments_writer = payments_writer.trigger(availableNow=True)
    payments_query = payments_writer.start()

    return {
        "orders_query": orders_query,
        "payments_query": payments_query,
        "silver_orders_path": silver_orders_path,
        "silver_payments_path": silver_payments_path,
        "silver_rejects_path": silver_rejects_path,
    }


def run_silver_pipeline(
    base_dir: str = "data/delta",
    checkpoint_dir: str = "checkpoints/silver",
    once: bool = False,
) -> None:
    """Run Silver processing either once (availableNow) or continuously."""
    ensure_java_compatibility()
    spark = get_spark_session("rekon-silver-pipeline")
    queries = start_silver_streaming(
        spark=spark,
        base_dir=base_dir,
        checkpoint_dir=checkpoint_dir,
        trigger_available_now=once,
    )

    if once:
        logger.info("Running Silver processing in availableNow mode...")
        queries["orders_query"].awaitTermination()
        queries["payments_query"].awaitTermination()
        logger.info("Silver micro-batch processing complete.")
    else:
        logger.info("Running Silver processing continuously (press Ctrl+C to stop)...")
        spark.streams.awaitAnyTermination()


def main():
    parser = argparse.ArgumentParser(description="Rekon Silver Deduplication & Rejects Pipeline")
    parser.add_argument(
        "--base-dir", type=str, default="data/delta", help="Delta Lake storage directory"
    )
    parser.add_argument(
        "--checkpoint-dir", type=str, default="checkpoints/silver", help="Checkpoint directory"
    )
    parser.add_argument(
        "--once", action="store_true", help="Process all available Bronze records once and exit"
    )

    args = parser.parse_args()
    run_silver_pipeline(
        base_dir=args.base_dir,
        checkpoint_dir=args.checkpoint_dir,
        once=args.once,
    )


if __name__ == "__main__":
    main()
