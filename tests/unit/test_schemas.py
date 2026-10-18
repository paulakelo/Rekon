"""Unit tests for schema contracts, PII hashing, and rail normalizers."""

import json

import pytest

from streaming.common.schemas import (
    hash_payer_identifier,
    normalize_bank_payload,
    normalize_mpesa_payload,
)


def test_hash_payer_identifier_determinism():
    """Verify phone numbers and account numbers are hashed deterministically and securely."""
    phone = "254712345678"
    hash1 = hash_payer_identifier(phone)
    hash2 = hash_payer_identifier(phone)

    assert hash1 == hash2
    assert phone not in hash1
    assert len(hash1) == 24


def test_normalize_mpesa_payload_valid():
    """Verify standard M-Pesa Daraja callback normalizes to canonical payment schema."""
    raw = {
        "TransactionType": "Pay Bill",
        "TransID": "RKT9182746",
        "TransTime": "20261018143000",
        "TransAmount": "1500.50",
        "BusinessShortCode": "600123",
        "BillRefNumber": "ORD-00000042",
        "MSISDN": "254712345678",
    }
    normalized = normalize_mpesa_payload(json.dumps(raw))

    assert normalized["rail"] == "mpesa"
    assert normalized["rail_receipt_id"] == "RKT9182746"
    assert normalized["order_ref"] == "ORD-00000042"
    assert normalized["amount_kes"] == 1500.50
    assert normalized["currency"] == "KES"
    assert normalized["paid_at"].year == 2026
    assert normalized["paid_at"].month == 10
    assert normalized["paid_at"].day == 18
    assert normalized["status"] == "SETTLED"
    assert "254712345678" not in normalized["payer_hash"]


def test_normalize_mpesa_missing_fields():
    """Verify missing mandatory fields raise ValueError for routing to silver_rejects."""
    raw = {
        "TransactionType": "Pay Bill",
        "TransID": "RKT9182746",
    }  # missing BillRefNumber and TransAmount
    with pytest.raises(ValueError, match="Missing mandatory M-Pesa fields"):
        normalize_mpesa_payload(json.dumps(raw))


def test_normalize_bank_payload_valid():
    """Verify bank settlement webhook normalizes to canonical payment schema."""
    raw = {
        "reference": "BNK-20261018-001122",
        "amount": 3400.0,
        "currency": "KES",
        "value_date": "2026-10-18T14:32:15Z",
        "status": "SETTLED",
        "order_reference": "ORD-00000088",
        "sender_account_masked": "XXXX-XXXX-9912",
    }
    normalized = normalize_bank_payload(json.dumps(raw))

    assert normalized["rail"] == "bank"
    assert normalized["rail_receipt_id"] == "BNK-20261018-001122"
    assert normalized["order_ref"] == "ORD-00000088"
    assert normalized["amount_kes"] == 3400.0
    assert normalized["currency"] == "KES"
    assert normalized["paid_at"].hour == 14
    assert normalized["status"] == "SETTLED"
