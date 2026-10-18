"""Schema contracts, PII hashing, and normalizers for Rekon pipeline."""

import hashlib
import json
import logging
from datetime import datetime
from typing import Any

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger("rekon.streaming.schemas")

# Default PII salt for hashing customer MSISDN and bank account numbers
PII_SALT = "rekon_privacy_salt_2026"


def hash_payer_identifier(identifier: str | None, salt: str = PII_SALT) -> str:
    """Hash customer phone numbers and account identifiers using salted SHA-256."""
    if not identifier:
        return "UNKNOWN_PAYER"
    cleaned = str(identifier).strip()
    return hashlib.sha256(f"{cleaned}:{salt}".encode()).hexdigest()[:24]


# -------------------------------------------------------------------------
# Bronze StructType Schemas
# -------------------------------------------------------------------------
BRONZE_PAYMENTS_SCHEMA = StructType(
    [
        StructField("rail", StringType(), False),
        StructField("topic", StringType(), False),
        StructField("partition", IntegerType(), False),
        StructField("offset", IntegerType(), False),
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("ingest_timestamp", TimestampType(), False),
        StructField("raw_payload", StringType(), False),
    ]
)

BRONZE_ORDERS_SCHEMA = StructType(
    [
        StructField("topic", StringType(), False),
        StructField("partition", IntegerType(), False),
        StructField("offset", IntegerType(), False),
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("ingest_timestamp", TimestampType(), False),
        StructField("raw_payload", StringType(), False),
    ]
)

# -------------------------------------------------------------------------
# Silver StructType Schemas
# -------------------------------------------------------------------------
SILVER_PAYMENTS_SCHEMA = StructType(
    [
        StructField("rail", StringType(), False),
        StructField("rail_receipt_id", StringType(), False),
        StructField("order_ref", StringType(), False),
        StructField("amount_kes", DoubleType(), False),
        StructField("currency", StringType(), False),
        StructField("paid_at", TimestampType(), False),
        StructField("payer_hash", StringType(), False),
        StructField("status", StringType(), False),
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", IntegerType(), True),
        StructField("_silver_ts", TimestampType(), False),
    ]
)

SILVER_ORDERS_SCHEMA = StructType(
    [
        StructField("order_ref", StringType(), False),
        StructField("shop_id", StringType(), False),
        StructField("amount_kes", DoubleType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("_ingest_ts", TimestampType(), False),
    ]
)

SILVER_REJECTS_SCHEMA = StructType(
    [
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", IntegerType(), True),
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("raw_payload", StringType(), False),
        StructField("reject_reason", StringType(), False),
        StructField("_rejected_at", TimestampType(), False),
    ]
)

# -------------------------------------------------------------------------
# Gold StructType Schemas
# -------------------------------------------------------------------------
GOLD_ORDER_STATUS_SCHEMA = StructType(
    [
        StructField("order_ref", StringType(), False),
        StructField("shop_id", StringType(), True),
        StructField("order_amount_kes", DoubleType(), True),
        StructField("paid_total_kes", DoubleType(), False),
        StructField("payment_count", IntegerType(), False),
        StructField("status", StringType(), False),
        StructField("first_paid_at", TimestampType(), True),
        StructField("last_paid_at", TimestampType(), True),
        StructField("settled_at", TimestampType(), True),
        StructField("last_updated_at", TimestampType(), False),
    ]
)

GOLD_ANOMALIES_SCHEMA = StructType(
    [
        StructField("anomaly_id", StringType(), False),
        StructField("anomaly_type", StringType(), False),
        StructField("rail", StringType(), True),
        StructField("rail_receipt_id", StringType(), True),
        StructField("order_ref", StringType(), True),
        StructField("detected_at", TimestampType(), False),
        StructField("details", StringType(), True),
    ]
)


# -------------------------------------------------------------------------
# Pure Function Normalizers
# -------------------------------------------------------------------------
def normalize_mpesa_payload(raw_json: str) -> dict[str, Any]:
    """Parse and normalize M-Pesa Daraja C2B confirmation payload into canonical form."""
    data = json.loads(raw_json)

    trans_id = data.get("TransID")
    order_ref = data.get("BillRefNumber")
    amount_str = data.get("TransAmount")
    trans_time_str = data.get("TransTime")  # yyyyMMddHHmmss

    if not trans_id or not order_ref or amount_str is None or not trans_time_str:
        raise ValueError(f"Missing mandatory M-Pesa fields in payload: {data}")

    amount_kes = round(float(amount_str), 2)
    dt = datetime.strptime(trans_time_str, "%Y%m%d%H%M%S")

    payer_msisdn = data.get("MSISDN", "")
    payer_hash = hash_payer_identifier(payer_msisdn)

    return {
        "rail": "mpesa",
        "rail_receipt_id": str(trans_id).strip(),
        "order_ref": str(order_ref).strip(),
        "amount_kes": amount_kes,
        "currency": "KES",
        "paid_at": dt,
        "payer_hash": payer_hash,
        "status": "SETTLED",
    }


def normalize_bank_payload(raw_json: str) -> dict[str, Any]:
    """Parse and normalize Bank settlement payload into canonical form."""
    data = json.loads(raw_json)

    reference = data.get("reference")
    order_ref = data.get("order_reference")
    amount = data.get("amount")
    value_date_str = data.get("value_date")

    if not reference or not order_ref or amount is None or not value_date_str:
        raise ValueError(f"Missing mandatory Bank callback fields in payload: {data}")

    amount_kes = round(float(amount), 2)
    # Parse ISO 8601 (e.g. 2026-10-18T14:32:15Z or with offset)
    clean_date = value_date_str.replace("Z", "+00:00")
    dt = datetime.fromisoformat(clean_date).replace(tzinfo=None)

    raw_sender = data.get("sender_account_masked", "")
    payer_hash = hash_payer_identifier(raw_sender)

    return {
        "rail": "bank",
        "rail_receipt_id": str(reference).strip(),
        "order_ref": str(order_ref).strip(),
        "amount_kes": amount_kes,
        "currency": data.get("currency", "KES"),
        "paid_at": dt,
        "payer_hash": payer_hash,
        "status": data.get("status", "SETTLED"),
    }
