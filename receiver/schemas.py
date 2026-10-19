"""Pydantic schemas and serialization models for Rekon edge webhook receiver."""

from pydantic import BaseModel, Field


class MpesaC2BConfirmation(BaseModel):
    """Safaricom Daraja C2B confirmation payload model."""

    TransactionType: str = Field(default="Pay Bill", description="Transaction type")
    TransID: str = Field(description="Unique Safaricom receipt identifier")
    TransTime: str = Field(description="Timestamp formatted as yyyyMMddHHmmss")
    TransAmount: str = Field(description="Payment amount as string or decimal")
    BusinessShortCode: str = Field(default="600123", description="Paybill or Till number")
    BillRefNumber: str = Field(description="Order reference number, e.g. ORD-00000001")
    InvoiceNumber: str | None = Field(default="")
    OrgAccountBalance: str | None = Field(default="")
    ThirdPartyTransID: str | None = Field(default="")
    MSISDN: str = Field(description="Customer phone number (synthetic, hashed in Silver)")
    FirstName: str | None = Field(default="")
    MiddleName: str | None = Field(default="")
    LastName: str | None = Field(default="")


class MpesaResponse(BaseModel):
    """Standard Daraja acknowledgment response."""

    ResultCode: int = 0
    ResultDesc: str = "Accepted"


class BankWebhookPayload(BaseModel):
    """Core banking and PesaLink settlement callback schema."""

    reference: str = Field(description="Unique bank transaction reference")
    amount: float = Field(description="Payment amount in KES")
    currency: str = Field(default="KES")
    value_date: str = Field(description="Value date in ISO-8601 format")
    status: str = Field(default="SETTLED")
    narration: str | None = Field(default="")
    order_reference: str = Field(description="Dukastack-compatible order ref (ORD-xxxxxxxx)")
    sender_account_masked: str | None = Field(default="XXXX-XXXX-0000")
    bank_code: str | None = Field(default="011")


class BankResponse(BaseModel):
    """Standard banking webhook acknowledgment response."""

    status: str = "received"
    receipt: str
