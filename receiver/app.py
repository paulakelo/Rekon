"""FastAPI edge webhook receiver for payment rails.

Implements FR-1 and FR-2:
- Accepts M-Pesa Daraja and Bank callbacks.
- Immediately publishes raw payload string to Kafka.
- Acknowledges with rail-specific expected response format.
- Zero Loss: Malformed payloads are preserved and forwarded to Kafka.
"""

import json
import logging

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from receiver.kafka_producer import publish_raw_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("rekon.receiver.app")

app = FastAPI(
    title="Rekon Webhook Receiver",
    description="Resilient edge ingestion service for M-Pesa C2B and Bank payment rails",
    version="0.1.0",
)

TOPIC_MPESA = "payments.mpesa.raw"
TOPIC_BANK = "payments.bank.raw"
TOPIC_ORDERS = "orders.created"


@app.get("/health")
def health_check():
    """Health check endpoint for container readiness."""
    return {"status": "ok", "service": "rekon-receiver"}


@app.post("/api/v1/payments/mpesa")
async def receive_mpesa_callback(request: Request):
    """Receive M-Pesa Daraja C2B confirmation callback.

    Always responds with ResultCode 0 and ResultDesc Accepted.
    Writes raw body to payments.mpesa.raw topic.
    """
    body_bytes = await request.body()
    raw_payload = body_bytes.decode("utf-8", errors="replace")

    # Extract TransID if parseable JSON for Kafka record key (optional)
    key = None
    try:
        data = json.loads(raw_payload)
        key = data.get("TransID")
    except Exception:
        pass

    # Publish raw payload string directly to Kafka
    publish_raw_event(TOPIC_MPESA, raw_payload=raw_payload, key=key)

    # Return standard Daraja expected response
    return {"ResultCode": 0, "ResultDesc": "Accepted"}


@app.post("/api/v1/payments/bank")
async def receive_bank_callback(request: Request):
    """Receive bank/aggregator settlement callback.

    Acknowledges with status received and echoes transaction reference.
    Writes raw body to payments.bank.raw topic.
    """
    body_bytes = await request.body()
    raw_payload = body_bytes.decode("utf-8", errors="replace")

    key = None
    receipt_ref = "UNKNOWN"
    try:
        data = json.loads(raw_payload)
        receipt_ref = data.get("reference", "UNKNOWN")
        key = receipt_ref
    except Exception:
        pass

    # Publish raw payload string directly to Kafka
    publish_raw_event(TOPIC_BANK, raw_payload=raw_payload, key=key)

    return {"status": "received", "receipt": receipt_ref}


@app.post("/api/v1/orders", status_code=status.HTTP_201_CREATED)
async def receive_order_event(request: Request):
    """Receive orders created in POS/ecommerce systems (dukastack companion)."""
    body_bytes = await request.body()
    raw_payload = body_bytes.decode("utf-8", errors="replace")

    key = None
    try:
        data = json.loads(raw_payload)
        key = data.get("order_ref")
    except Exception:
        pass

    publish_raw_event(TOPIC_ORDERS, raw_payload=raw_payload, key=key)
    return JSONResponse(status_code=201, content={"status": "order_queued"})
