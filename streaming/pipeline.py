"""PySpark Structured Streaming & Delta Lake reconciliation engine."""

import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from streaming.common.dedupe import merge_silver_orders, merge_silver_payments
from streaming.common.metrics import metrics_tracker
from streaming.common.schemas import (
    BRONZE_ORDERS_SCHEMA,
    BRONZE_PAYMENTS_SCHEMA,
    GOLD_ANOMALIES_SCHEMA,
    GOLD_ORDER_STATUS_SCHEMA,
    SILVER_ORDERS_SCHEMA,
    SILVER_PAYMENTS_SCHEMA,
    SILVER_REJECTS_SCHEMA,
    normalize_bank_payload,
    normalize_mpesa_payload,
)
from streaming.common.status import (
    AnomalyType,
    check_lateness,
    determine_order_status,
)

logger = logging.getLogger("rekon.streaming.pipeline")


def ensure_java_compatibility():
    """Ensure JAVA_HOME points to a Spark JVM (Java 11/17) and Python matches driver."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    current = os.environ.get("JAVA_HOME", "")
    if not current or "java-25" in current:
        candidates = [
            "/usr/lib/jvm/java-11-temurin-jdk",
            "/usr/lib/jvm/temurin-11-jdk",
            "/usr/lib/jvm/java-17-openjdk",
            "/usr/lib/jvm/java-21-temurin-jdk",
        ]
        for c in candidates:
            if os.path.exists(c):
                os.environ["JAVA_HOME"] = c
                os.environ["PATH"] = f"{c}/bin:" + os.environ.get("PATH", "")
                break


def get_spark_session(app_name: str = "rekon-reconciliation") -> SparkSession:
    """Create or retrieve a PySpark SparkSession configured for local Delta Lake."""
    ensure_java_compatibility()
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
    )
    builder = configure_spark_with_delta_pip(builder)
    return builder.getOrCreate()


class ReconciliationPipeline:
    """End-to-end lakehouse manager for Bronze, Silver, and Gold Delta tables."""

    def __init__(self, spark: SparkSession, base_dir: Path | str = "data/delta"):
        self.spark = spark
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

        self.bronze_orders_path = str(self.base_dir / "bronze_orders")
        self.bronze_payments_path = str(self.base_dir / "bronze_payments")
        self.silver_orders_path = str(self.base_dir / "silver_orders")
        self.silver_payments_path = str(self.base_dir / "silver_payments")
        self.silver_rejects_path = str(self.base_dir / "silver_rejects")
        self.gold_status_path = str(self.base_dir / "gold_order_payment_status")
        self.gold_anomalies_path = str(self.base_dir / "gold_payment_anomalies")
        self.gold_daily_path = str(self.base_dir / "gold_reconciliation_daily")
        self.gold_throughput_path = str(self.base_dir / "gold_rail_throughput_15m")

    # -------------------------------------------------------------------------
    # Bronze Ingestion
    # -------------------------------------------------------------------------
    def append_bronze_orders(self, orders: list[dict[str, Any]]) -> int:
        """Append raw orders to bronze_orders Delta table."""
        now = datetime.now()
        rows = []
        for i, o in enumerate(orders):
            rows.append(
                {
                    "topic": "orders.created",
                    "partition": 0,
                    "offset": i,
                    "kafka_timestamp": now,
                    "ingest_timestamp": now,
                    "raw_payload": json.dumps(o),
                }
            )

        df = self.spark.createDataFrame(rows, schema=BRONZE_ORDERS_SCHEMA)
        df.write.format("delta").mode("append").save(self.bronze_orders_path)
        logger.info("Appended %d records to bronze_orders", len(rows))
        return len(rows)

    def append_bronze_payments(self, payments: list[dict[str, Any]], rail: str) -> int:
        """Append raw payment callbacks to bronze_payments Delta table."""
        now = datetime.now()
        rows = []
        topic = f"payments.{rail}.raw"
        for i, p in enumerate(payments):
            raw = p if isinstance(p, str) else json.dumps(p)
            rows.append(
                {
                    "rail": rail,
                    "topic": topic,
                    "partition": 0,
                    "offset": i,
                    "kafka_timestamp": now,
                    "ingest_timestamp": now,
                    "raw_payload": raw,
                }
            )

        df = self.spark.createDataFrame(rows, schema=BRONZE_PAYMENTS_SCHEMA)
        df.write.format("delta").mode("append").save(self.bronze_payments_path)
        logger.info("Appended %d records to bronze_payments for rail %s", len(rows), rail)
        return len(rows)

    # -------------------------------------------------------------------------
    # Silver Processing (Parse, Normalize, Rejects, MERGE)
    # -------------------------------------------------------------------------
    def process_silver(self) -> dict[str, int]:
        """Process Bronze data into Silver tables with normalization and dedupe."""
        start_time = time.time()
        bronze_payments_df = self.spark.read.format("delta").load(self.bronze_payments_path)
        bronze_orders_df = self.spark.read.format("delta").load(self.bronze_orders_path)

        # 1. Process Orders into silver_orders
        raw_orders = bronze_orders_df.select("raw_payload").collect()
        parsed_orders = []
        now = datetime.now()
        for r in raw_orders:
            try:
                data = json.loads(r["raw_payload"])
                parsed_orders.append(
                    {
                        "order_ref": str(data["order_ref"]).strip(),
                        "shop_id": str(data.get("shop_id", "UNKNOWN")),
                        "amount_kes": round(float(data["amount_kes"]), 2),
                        "created_at": datetime.fromisoformat(
                            data["created_at"].replace("Z", "+00:00")
                        ).replace(tzinfo=None),
                        "_ingest_ts": now,
                    }
                )
            except Exception as exc:
                logger.warning("Failed to parse order in Bronze: %s", exc)

        silver_orders_df = self.spark.createDataFrame(parsed_orders, schema=SILVER_ORDERS_SCHEMA)
        merge_silver_orders(self.spark, self.silver_orders_path, silver_orders_df)

        # 2. Process Payments into silver_payments & silver_rejects
        raw_payments = bronze_payments_df.select(
            "rail", "raw_payload", "topic", "partition", "offset"
        ).collect()
        valid_payments = []
        rejects = []

        for r in raw_payments:
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
                        "kafka_timestamp": now,
                        "raw_payload": raw_str,
                        "reject_reason": str(exc),
                        "_rejected_at": now,
                    }
                )

        # Save rejects if any
        if rejects:
            rejects_df = self.spark.createDataFrame(rejects, schema=SILVER_REJECTS_SCHEMA)
            rejects_df.write.format("delta").mode("append").save(self.silver_rejects_path)
            logger.info("Recorded %d rows in silver_rejects", len(rejects))

        # Idempotent MERGE for valid payments
        inserted_count = 0
        conflict_anomalies = []
        if valid_payments:
            silver_df = self.spark.createDataFrame(valid_payments, schema=SILVER_PAYMENTS_SCHEMA)
            inserted_count, conflict_anomalies = merge_silver_payments(
                self.spark, self.silver_payments_path, silver_df
            )

        duration = (time.time() - start_time) * 1000
        metrics_tracker.record_batch(
            batch_id=1,
            layer="silver",
            rows_in=len(raw_payments),
            valid_rows=len(valid_payments),
            duplicates_dropped=len(raw_payments) - len(valid_payments),
            rejects=len(rejects),
            duration_ms=duration,
        )

        # Save any conflict anomalies to gold anomalies
        if conflict_anomalies:
            ano_rows = []
            for i, c in enumerate(conflict_anomalies):
                ano_rows.append(
                    {
                        "anomaly_id": f"ANO-DUP-{i + 1:04d}",
                        "anomaly_type": AnomalyType.DUPLICATE_CONFLICT,
                        "rail": c["rail"],
                        "rail_receipt_id": c["rail_receipt_id"],
                        "order_ref": c["order_ref"],
                        "detected_at": now,
                        "details": json.dumps(c),
                    }
                )
            ano_df = self.spark.createDataFrame(ano_rows, schema=GOLD_ANOMALIES_SCHEMA)
            ano_df.write.format("delta").mode("append").save(self.gold_anomalies_path)

        return {
            "orders": len(parsed_orders),
            "payments_processed": len(valid_payments),
            "rejects": len(rejects),
            "conflicts": len(conflict_anomalies),
        }

    # -------------------------------------------------------------------------
    # Gold Reconciliation Engine & Anomalies
    # -------------------------------------------------------------------------
    def process_gold(self, is_window_expired: bool = False) -> None:
        """Aggregate Silver data to generate Gold marts and anomalies."""
        now = datetime.now()
        silver_orders_df = self.spark.read.format("delta").load(self.silver_orders_path)
        silver_payments_df = self.spark.read.format("delta").load(self.silver_payments_path)

        # 1. gold_order_payment_status: Group payments by order_ref
        payment_agg = silver_payments_df.groupBy("order_ref").agg(
            F.sum("amount_kes").alias("paid_total_kes"),
            F.count("rail_receipt_id").alias("payment_count"),
            F.min("paid_at").alias("first_paid_at"),
            F.max("paid_at").alias("last_paid_at"),
        )

        joined_orders = silver_orders_df.join(payment_agg, on="order_ref", how="left")
        collected_orders = joined_orders.collect()

        gold_status_rows = []
        anomalies_to_add = []
        ano_counter = 0

        for r in collected_orders:
            order_ref = r["order_ref"]
            shop_id = r["shop_id"]
            order_amount = r["amount_kes"]
            created_at = r["created_at"]

            paid_total = r["paid_total_kes"] if r["paid_total_kes"] is not None else 0.0
            paid_total = round(paid_total, 2)
            payment_count = int(r["payment_count"]) if r["payment_count"] is not None else 0
            first_paid_at = r["first_paid_at"]
            last_paid_at = r["last_paid_at"]

            status = determine_order_status(
                order_amount, paid_total, is_window_expired=is_window_expired
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

            # Check for anomalies:
            if status == "OVERPAID":
                ano_counter += 1
                anomalies_to_add.append(
                    {
                        "anomaly_id": f"ANO-OVP-{ano_counter:04d}",
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

            if status == "UNDERPAID":
                ano_counter += 1
                anomalies_to_add.append(
                    {
                        "anomaly_id": f"ANO-UND-{ano_counter:04d}",
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

            if last_paid_at and check_lateness(last_paid_at, created_at, window_hours=48.0):
                ano_counter += 1
                anomalies_to_add.append(
                    {
                        "anomaly_id": f"ANO-LAT-{ano_counter:04d}",
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

        # Save gold_order_payment_status
        status_df = self.spark.createDataFrame(gold_status_rows, schema=GOLD_ORDER_STATUS_SCHEMA)
        status_df.write.format("delta").mode("overwrite").save(self.gold_status_path)

        # 2. Check for Orphan Payments (payments matching no order)
        all_order_refs = {r["order_ref"] for r in collected_orders}
        payments_list = silver_payments_df.collect()
        for p in payments_list:
            if p["order_ref"] not in all_order_refs:
                ano_counter += 1
                anomalies_to_add.append(
                    {
                        "anomaly_id": f"ANO-ORP-{ano_counter:04d}",
                        "anomaly_type": AnomalyType.ORPHAN_PAYMENT,
                        "rail": p["rail"],
                        "rail_receipt_id": p["rail_receipt_id"],
                        "order_ref": p["order_ref"],
                        "detected_at": now,
                        "details": json.dumps(
                            {"amount_kes": p["amount_kes"], "paid_at": str(p["paid_at"])}
                        ),
                    }
                )

        if anomalies_to_add:
            ano_df = self.spark.createDataFrame(anomalies_to_add, schema=GOLD_ANOMALIES_SCHEMA)
            ano_df.write.format("delta").mode("append").save(self.gold_anomalies_path)

        # 3. gold_reconciliation_daily: Aggregate daily billed vs collected
        daily_df = status_df.groupBy(F.to_date("last_updated_at").alias("recon_date")).agg(
            F.sum("order_amount_kes").alias("billed_amount_kes"),
            F.sum("paid_total_kes").alias("collected_amount_kes"),
            F.count("order_ref").alias("billed_orders_count"),
            F.sum(F.when(F.col("status") == "PAID", 1).otherwise(0)).alias("paid_orders_count"),
            F.sum(F.when(F.col("status") == "PARTIAL", 1).otherwise(0)).alias(
                "partial_orders_count"
            ),
            F.sum(F.when(F.col("status") == "UNPAID", 1).otherwise(0)).alias("unpaid_orders_count"),
            F.sum(F.when(F.col("status") == "OVERPAID", 1).otherwise(0)).alias(
                "overpaid_orders_count"
            ),
            F.sum(F.when(F.col("status") == "UNDERPAID", 1).otherwise(0)).alias(
                "underpaid_orders_count"
            ),
        )
        daily_df.write.format("delta").mode("overwrite").save(self.gold_daily_path)

        # 4. gold_rail_throughput_15m: Windowed metrics with event time
        throughput_df = (
            silver_payments_df.groupBy(
                "rail",
                F.window("paid_at", "15 minutes"),
            )
            .agg(
                F.count("rail_receipt_id").alias("transaction_count"),
                F.sum("amount_kes").alias("total_amount_kes"),
                F.avg("amount_kes").alias("avg_amount_kes"),
            )
            .select(
                "rail",
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                "transaction_count",
                "total_amount_kes",
                "avg_amount_kes",
            )
        )
        throughput_df.write.format("delta").mode("overwrite").save(self.gold_throughput_path)

        logger.info(
            "Gold processing complete: %d orders reconciled, %d anomalies logged.",
            len(gold_status_rows),
            len(anomalies_to_add),
        )


def run_local_pipeline(dataset: dict[str, list], base_dir: str = "data/delta") -> None:
    """Execute full local PySpark & Delta Lake pipeline from dataset."""
    spark = get_spark_session()
    pipeline = ReconciliationPipeline(spark, base_dir=base_dir)

    # 1. Ingest into Bronze
    pipeline.append_bronze_orders(dataset["orders"])
    pipeline.append_bronze_payments(dataset["payments_mpesa"], rail="mpesa")
    pipeline.append_bronze_payments(dataset["payments_bank"], rail="bank")

    # 2. Ingest malformed scenario C9 into Bronze
    pipeline.append_bronze_payments(
        ['{"TransactionType": "Pay Bill", "TransID": "RKT-CORRUPT", BAD_JSON'], rail="mpesa"
    )

    # 3. Process into Silver
    pipeline.process_silver()

    # 4. Process Gold (initial window)
    pipeline.process_gold(is_window_expired=False)

    # 5. Execute sweep for expired window (finalizing UNDERPAID & ORPHANS)
    pipeline.process_gold(is_window_expired=True)

    print("\n✅ Pipeline successfully processed Bronze, Silver, and Gold Delta tables.")
