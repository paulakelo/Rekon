"""Integration tests verifying HTTP edge receiver to real Kafka broker flow (M2)."""

import json
import os
import uuid

import pytest
from fastapi.testclient import TestClient
from kafka import KafkaConsumer

from receiver import kafka_producer
from receiver.app import app

# Force Kafka producer to connect to localhost:9092
os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"
kafka_producer._producer = None  # Reset singleton to pick up localhost env


@pytest.fixture(scope="module")
def client():
    """FastAPI TestClient fixture."""
    return TestClient(app)


def test_mpesa_ingestion_end_to_end_to_kafka(client):
    """FR-1: M-Pesa webhook receives 200 Accepted and publishes raw payload to Kafka."""
    test_id = f"RKT-M2-{uuid.uuid4().hex[:6].upper()}"
    payload = {
        "TransactionType": "Pay Bill",
        "TransID": test_id,
        "TransTime": "20261019120000",
        "TransAmount": "1750.00",
        "BusinessShortCode": "600123",
        "BillRefNumber": "ORD-00000042",
        "MSISDN": "254712345678",
    }

    # 1. POST callback to edge receiver
    response = client.post("/api/v1/payments/mpesa", json=payload)
    assert response.status_code == 200
    assert response.json() == {"ResultCode": 0, "ResultDesc": "Accepted"}

    # 2. Verify payload on Kafka topic payments.mpesa.raw
    consumer = KafkaConsumer(
        "payments.mpesa.raw",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
        value_deserializer=lambda v: v.decode("utf-8"),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )

    found = False
    for msg in consumer:
        if msg.key == test_id:
            found = True
            consumed_data = json.loads(msg.value)
            assert consumed_data["TransID"] == test_id
            assert consumed_data["TransAmount"] == "1750.00"
            assert consumed_data["BillRefNumber"] == "ORD-00000042"
            break

    consumer.close()
    assert found, f"Record with key {test_id} was not found on payments.mpesa.raw topic"


def test_bank_ingestion_end_to_end_to_kafka(client):
    """FR-1: Bank webhook receives status received and publishes raw payload to Kafka."""
    test_ref = f"BNK-M2-{uuid.uuid4().hex[:6].upper()}"
    payload = {
        "reference": test_ref,
        "amount": 8900.50,
        "currency": "KES",
        "value_date": "2026-10-19T12:05:00Z",
        "status": "SETTLED",
        "order_reference": "ORD-00000099",
        "sender_account_masked": "XXXX-XXXX-1122",
    }

    # 1. POST callback to edge receiver
    response = client.post("/api/v1/payments/bank", json=payload)
    assert response.status_code == 200
    assert response.json() == {"status": "received", "receipt": test_ref}

    # 2. Verify payload on Kafka topic payments.bank.raw
    consumer = KafkaConsumer(
        "payments.bank.raw",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
        value_deserializer=lambda v: v.decode("utf-8"),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )

    found = False
    for msg in consumer:
        if msg.key == test_ref:
            found = True
            consumed_data = json.loads(msg.value)
            assert consumed_data["reference"] == test_ref
            assert consumed_data["amount"] == 8900.50
            assert consumed_data["order_reference"] == "ORD-00000099"
            break

    consumer.close()
    assert found, f"Record with key {test_ref} was not found on payments.bank.raw topic"


def test_malformed_payload_preserved_to_kafka(client):
    """FR-2: Malformed payload is acknowledged and preserved on Kafka without dropping."""
    unique_marker = f"CORRUPT-{uuid.uuid4().hex[:6]}"
    malformed_body = (
        f'"TransactionType": "Pay Bill", "marker": "{unique_marker}", INVALID_JSON_SYNTAX'
    )

    # 1. POST corrupt string to receiver
    response = client.post(
        "/api/v1/payments/mpesa",
        content=malformed_body,
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 200
    assert response.json() == {"ResultCode": 0, "ResultDesc": "Accepted"}

    # 2. Verify corrupt string lands on Kafka topic unaltered
    consumer = KafkaConsumer(
        "payments.mpesa.raw",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
        value_deserializer=lambda v: v.decode("utf-8", errors="replace"),
    )

    found = False
    for msg in consumer:
        if unique_marker in msg.value:
            found = True
            assert malformed_body in msg.value
            break

    consumer.close()
    assert found, f"Corrupt payload containing {unique_marker} was not found on payments.mpesa.raw"


def test_orders_ingestion_end_to_end_to_kafka(client):
    """Order creation event lands on orders.created topic."""
    test_order_ref = f"ORD-M2-{uuid.uuid4().hex[:6].upper()}"
    payload = {
        "order_ref": test_order_ref,
        "shop_id": "SHOP-001-NAIROBI-CBD",
        "amount_kes": 4200.0,
        "currency": "KES",
        "created_at": "2026-10-19T12:10:00Z",
    }

    response = client.post("/api/v1/orders", json=payload)
    assert response.status_code == 201
    assert response.json() == {"status": "order_queued"}

    consumer = KafkaConsumer(
        "orders.created",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
        value_deserializer=lambda v: v.decode("utf-8"),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )

    found = False
    for msg in consumer:
        if msg.key == test_order_ref:
            found = True
            consumed_data = json.loads(msg.value)
            assert consumed_data["order_ref"] == test_order_ref
            assert consumed_data["amount_kes"] == 4200.0
            break

    consumer.close()
    assert found, f"Order with key {test_order_ref} was not found on orders.created topic"
