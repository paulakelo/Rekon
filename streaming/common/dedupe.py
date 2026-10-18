"""Delta Lake idempotent MERGE and deduplication logic."""

import logging

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger("rekon.streaming.dedupe")


def merge_silver_payments(
    spark: SparkSession,
    silver_table_path: str,
    microbatch_df: DataFrame,
) -> tuple[int, list[dict]]:
    """Idempotently MERGE normalized payments into silver_payments Delta table.

    Returns:
        (inserted_count, conflict_anomalies_list)
    """
    from delta.tables import DeltaTable

    # Intra-batch deduplication: keep first occurrence within microbatch
    microbatch_df = microbatch_df.dropDuplicates(["rail", "rail_receipt_id"])

    # Check if target Delta table exists
    if not DeltaTable.isDeltaTable(spark, silver_table_path):
        microbatch_df.write.format("delta").mode("overwrite").save(silver_table_path)
        return microbatch_df.count(), []

    delta_target = DeltaTable.forPath(spark, silver_table_path)
    target_df = delta_target.toDF()

    # Identify duplicate conflicts (matching rail + receipt, but differing amount)
    joined = microbatch_df.alias("s").join(
        target_df.alias("t"),
        (microbatch_df["rail"] == target_df["rail"])
        & (microbatch_df["rail_receipt_id"] == target_df["rail_receipt_id"]),
        "inner",
    )

    conflicts = joined.filter("abs(s.amount_kes - t.amount_kes) > 0.01").collect()
    conflict_anomalies = []
    for row in conflicts:
        anomaly = {
            "rail": row["s.rail"],
            "rail_receipt_id": row["s.rail_receipt_id"],
            "order_ref": row["s.order_ref"],
            "existing_amount": row["t.amount_kes"],
            "conflicting_amount": row["s.amount_kes"],
        }
        conflict_anomalies.append(anomaly)
        logger.warning(
            "Duplicate conflict detected on receipt %s: %s", row["s.rail_receipt_id"], anomaly
        )

    # Perform idempotent MERGE: WHEN NOT MATCHED THEN INSERT
    (
        delta_target.alias("t")
        .merge(
            microbatch_df.alias("s"),
            "t.rail = s.rail AND t.rail_receipt_id = s.rail_receipt_id",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    return microbatch_df.count(), conflict_anomalies


def merge_silver_orders(
    spark: SparkSession,
    silver_orders_path: str,
    orders_df: DataFrame,
) -> int:
    """Idempotently MERGE orders into silver_orders Delta table on order_ref."""
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, silver_orders_path):
        orders_df.write.format("delta").mode("overwrite").save(silver_orders_path)
        return orders_df.count()

    delta_target = DeltaTable.forPath(spark, silver_orders_path)
    (
        delta_target.alias("t")
        .merge(orders_df.alias("s"), "t.order_ref = s.order_ref")
        .whenNotMatchedInsertAll()
        .execute()
    )
    return orders_df.count()
