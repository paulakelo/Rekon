"""Bronze layer streaming ingestion from Kafka into Delta Lake.

Consumes raw events from Kafka topics:
- orders.created -> bronze_orders Delta table
- payments.mpesa.raw, payments.bank.raw -> bronze_payments Delta table

Preserves 100% of raw payloads (Zero Data Loss) along with Kafka metadata:
topic, partition, offset, kafka_timestamp, ingest_timestamp.
"""

import argparse
import logging
from pathlib import Path
from typing import Any

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from streaming.pipeline import ensure_java_compatibility

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("rekon.streaming.bronze")

KAFKA_PACKAGES = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"]
TOPIC_ORDERS = "orders.created"
TOPIC_PAYMENTS_MPESA = "payments.mpesa.raw"
TOPIC_PAYMENTS_BANK = "payments.bank.raw"


def get_spark_session(app_name: str = "rekon-bronze-streaming") -> SparkSession:
    """Create or retrieve a PySpark SparkSession configured for Kafka & Delta Lake."""
    ensure_java_compatibility()
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
    builder = configure_spark_with_delta_pip(builder, extra_packages=KAFKA_PACKAGES)
    return builder.getOrCreate()


def create_orders_stream(
    spark: SparkSession,
    kafka_bootstrap: str = "localhost:9092",
    starting_offsets: str = "earliest",
) -> DataFrame:
    """Create streaming DataFrame consuming orders from Kafka."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", TOPIC_ORDERS)
        .option("startingOffsets", starting_offsets)
        .load()
        .select(
            F.col("topic"),
            F.col("partition"),
            F.col("offset").cast("integer").alias("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.current_timestamp().alias("ingest_timestamp"),
            F.col("value").cast("string").alias("raw_payload"),
        )
    )


def create_payments_stream(
    spark: SparkSession,
    kafka_bootstrap: str = "localhost:9092",
    starting_offsets: str = "earliest",
) -> DataFrame:
    """Create streaming DataFrame consuming payments from M-Pesa & Bank Kafka topics."""
    topics = f"{TOPIC_PAYMENTS_MPESA},{TOPIC_PAYMENTS_BANK}"
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", topics)
        .option("startingOffsets", starting_offsets)
        .load()
        .select(
            F.when(F.col("topic") == TOPIC_PAYMENTS_MPESA, "mpesa").otherwise("bank").alias("rail"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset").cast("integer").alias("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.current_timestamp().alias("ingest_timestamp"),
            F.col("value").cast("string").alias("raw_payload"),
        )
    )


def start_bronze_streaming(
    spark: SparkSession,
    base_dir: Path | str = "data/delta",
    checkpoint_dir: Path | str = "checkpoints/bronze",
    kafka_bootstrap: str = "localhost:9092",
    trigger_available_now: bool = False,
    starting_offsets: str = "earliest",
) -> dict[str, Any]:
    """Start Bronze Structured Streaming append queries for orders and payments."""
    base_path = Path(base_dir)
    cp_path = Path(checkpoint_dir)

    orders_target = str(base_path / "bronze_orders")
    orders_cp = str(cp_path / "orders")

    payments_target = str(base_path / "bronze_payments")
    payments_cp = str(cp_path / "payments")

    orders_stream = create_orders_stream(
        spark, kafka_bootstrap=kafka_bootstrap, starting_offsets=starting_offsets
    )
    payments_stream = create_payments_stream(
        spark, kafka_bootstrap=kafka_bootstrap, starting_offsets=starting_offsets
    )

    logger.info("Starting Bronze orders stream -> %s", orders_target)
    orders_writer = (
        orders_stream.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", orders_cp)
    )
    if trigger_available_now:
        orders_writer = orders_writer.trigger(availableNow=True)
    orders_query: StreamingQuery = orders_writer.start(orders_target)

    logger.info("Starting Bronze payments stream -> %s", payments_target)
    payments_writer = (
        payments_stream.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", payments_cp)
    )
    if trigger_available_now:
        payments_writer = payments_writer.trigger(availableNow=True)
    payments_query: StreamingQuery = payments_writer.start(payments_target)

    return {
        "orders_query": orders_query,
        "payments_query": payments_query,
        "bronze_orders_path": orders_target,
        "bronze_payments_path": payments_target,
    }


def run_bronze_ingestion(
    kafka_bootstrap: str = "localhost:9092",
    base_dir: str = "data/delta",
    checkpoint_dir: str = "checkpoints/bronze",
    once: bool = False,
) -> None:
    """Run Bronze streaming pipeline either continuously or once."""
    spark = get_spark_session()
    queries = start_bronze_streaming(
        spark=spark,
        base_dir=base_dir,
        checkpoint_dir=checkpoint_dir,
        kafka_bootstrap=kafka_bootstrap,
        trigger_available_now=once,
    )

    if once:
        logger.info("Running Bronze ingestion in availableNow mode...")
        queries["orders_query"].awaitTermination()
        queries["payments_query"].awaitTermination()
        logger.info("Bronze ingestion micro-batch completed.")
    else:
        logger.info("Running Bronze ingestion continuously (press Ctrl+C to stop)...")
        spark.streams.awaitAnyTermination()


def main():
    parser = argparse.ArgumentParser(description="Rekon Bronze Kafka-to-Delta Streaming Ingestion")
    parser.add_argument(
        "--kafka-bootstrap", type=str, default="localhost:9092", help="Kafka broker address"
    )
    parser.add_argument(
        "--base-dir", type=str, default="data/delta", help="Delta Lake storage directory"
    )
    parser.add_argument(
        "--checkpoint-dir", type=str, default="checkpoints/bronze", help="Checkpoint directory"
    )
    parser.add_argument(
        "--once", action="store_true", help="Process all available events once and exit"
    )

    args = parser.parse_args()
    run_bronze_ingestion(
        kafka_bootstrap=args.kafka_bootstrap,
        base_dir=args.base_dir,
        checkpoint_dir=args.checkpoint_dir,
        once=args.once,
    )


if __name__ == "__main__":
    main()
