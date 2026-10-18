"""Payment callback generator for M-Pesa Daraja C2B and Bank rails."""

import json
import random
from typing import Any

from simulator.clock import VirtualClock

KENYAN_FIRST_NAMES = [
    "Faith",
    "Brian",
    "Jane",
    "Kevin",
    "Mercy",
    "John",
    "Grace",
    "Dennis",
    "Paul",
    "Joy",
]
KENYAN_LAST_NAMES = [
    "Ochieng",
    "Wanjiku",
    "Mwangi",
    "Kiprono",
    "Muthoni",
    "Otieno",
    "Kamau",
    "Akinyi",
    "Njoroge",
]
BANK_CODES = ["011", "001", "068", "063"]  # Co-op, KCB, Equity, NCBA


class PaymentGenerator:
    """Generates synthetic payment callbacks across M-Pesa and Bank rails."""

    def __init__(self, clock: VirtualClock, seed: int = 42):
        self.clock = clock
        self.rng = random.Random(seed)
        self.receipt_counter = 0

    def generate_mpesa_payment(
        self,
        order_ref: str,
        amount: float,
        trans_id: str | None = None,
        trans_time: str | None = None,
    ) -> dict[str, Any]:
        """Generate a Safaricom Daraja C2B confirmation payload."""
        self.receipt_counter += 1
        if trans_id is None:
            # Safaricom receipt structure: 3 letters + 7 numbers/letters, e.g. RKT9182746
            trans_id = f"RKT{self.receipt_counter:07d}"

        if trans_time is None:
            trans_time = self.clock.now_mpesa()

        msisdn = f"2547{self.rng.randint(10000000, 99999999)}"
        first_name = self.rng.choice(KENYAN_FIRST_NAMES)
        last_name = self.rng.choice(KENYAN_LAST_NAMES)

        return {
            "TransactionType": "Pay Bill",
            "TransID": trans_id,
            "TransTime": trans_time,
            "TransAmount": f"{amount:.2f}",
            "BusinessShortCode": "600123",
            "BillRefNumber": order_ref,
            "InvoiceNumber": "",
            "OrgAccountBalance": "450000.00",
            "ThirdPartyTransID": "",
            "MSISDN": msisdn,
            "FirstName": first_name,
            "MiddleName": "",
            "LastName": last_name,
        }

    def generate_bank_payment(
        self,
        order_ref: str,
        amount: float,
        reference: str | None = None,
        value_date: str | None = None,
    ) -> dict[str, Any]:
        """Generate a core banking settlement callback."""
        self.receipt_counter += 1
        date_str = self.clock.now().strftime("%Y%m%d")
        if reference is None:
            reference = f"BNK-{date_str}-{self.receipt_counter:06d}"

        if value_date is None:
            value_date = self.clock.now_iso()

        bank_code = self.rng.choice(BANK_CODES)
        account_suffix = self.rng.randint(1000, 9999)

        return {
            "reference": reference,
            "amount": round(amount, 2),
            "currency": "KES",
            "value_date": value_date,
            "status": "SETTLED",
            "narration": f"Payment transfer for {order_ref}",
            "order_reference": order_ref,
            "sender_account_masked": f"XXXX-XXXX-{account_suffix}",
            "bank_code": bank_code,
        }

    @staticmethod
    def to_json(payload: dict[str, Any]) -> str:
        """Serialize payload dict to JSON string."""
        return json.dumps(payload)
