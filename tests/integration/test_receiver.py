"""Integration tests for edge FastAPI webhook receiver."""

from fastapi.testclient import TestClient

from receiver.app import app

client = TestClient(app)


def test_health_endpoint():
    """Verify receiver health endpoint returns 200 OK."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "rekon-receiver"}


def test_receive_mpesa_callback_valid():
    """Verify M-Pesa C2B callback receives immediate accept response (FR-1)."""
    payload = {
        "TransactionType": "Pay Bill",
        "TransID": "RKT9182746",
        "TransTime": "20261018143000",
        "TransAmount": "1500.00",
        "BusinessShortCode": "600123",
        "BillRefNumber": "ORD-00000001",
        "MSISDN": "254712345678",
    }
    response = client.post("/api/v1/payments/mpesa", json=payload)
    assert response.status_code == 200
    assert response.json() == {"ResultCode": 0, "ResultDesc": "Accepted"}


def test_receive_mpesa_callback_malformed():
    """Verify malformed JSON is accepted at edge and never dropped (FR-2 No Loss)."""
    malformed_raw = '{"TransactionType": "Pay Bill", INVALID_JSON_PAYLOAD'
    response = client.post(
        "/api/v1/payments/mpesa",
        content=malformed_raw,
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 200
    assert response.json() == {"ResultCode": 0, "ResultDesc": "Accepted"}


def test_receive_bank_callback_valid():
    """Verify bank settlement callback returns received status and echoes receipt."""
    payload = {
        "reference": "BNK-20261018-998811",
        "amount": 4200.50,
        "currency": "KES",
        "value_date": "2026-10-18T14:32:15Z",
        "status": "SETTLED",
        "order_reference": "ORD-00000088",
    }
    response = client.post("/api/v1/payments/bank", json=payload)
    assert response.status_code == 200
    assert response.json() == {"status": "received", "receipt": "BNK-20261018-998811"}
