"""Chaos scenario generator for Rekon simulator.

Implements all 11 chaos scenarios from technical specifications:
- C1: Exact Duplicate Redelivery (same receipt sent multiple times)
- C2: Duplicate Conflict (same receipt sent with conflicting amount)
- C3: Inversion (Payment emitted before Order creation)
- C4: Late Payment (Payment arrives after nominal 48h window)
- C5: Orphan Payment (Valid payment payload referencing nonexistent order)
- C6: Split Payment (Multiple payments summing to order total)
- C7: Partial Underpayment (Customer pays part of amount, never finishes)
- C8: Overpayment (Customer pays more than order total)
- C9: Malformed Payload (Corrupted JSON or missing critical fields)
"""

from copy import deepcopy
from typing import Any

from simulator.clock import VirtualClock
from simulator.payments import PaymentGenerator


class ChaosInjector:
    """Injects real-world payment edge cases into event streams."""

    def __init__(self, clock: VirtualClock, payment_gen: PaymentGenerator):
        self.clock = clock
        self.payment_gen = payment_gen

    def create_exact_duplicates(
        self, payload: dict[str, Any], count: int = 3
    ) -> list[dict[str, Any]]:
        """Scenario C1: Same receipt delivered multiple times."""
        return [deepcopy(payload) for _ in range(count)]

    def create_duplicate_conflict(
        self, payload: dict[str, Any], rail: str = "mpesa", diff_ratio: float = 1.5
    ) -> list[dict[str, Any]]:
        """Scenario C2: Same receipt delivered with different amount."""
        first = deepcopy(payload)
        second = deepcopy(payload)

        if rail == "mpesa":
            original_amount = float(first["TransAmount"])
            second["TransAmount"] = f"{round(original_amount * diff_ratio, 2):.2f}"
        else:
            original_amount = float(first["amount"])
            second["amount"] = round(original_amount * diff_ratio, 2)

        return [first, second]

    def create_inversion(
        self, order_ref: str, amount: float, hours_early: float = 2.0
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Scenario C3: Inversion (Payment emitted before Order creation)."""
        payment = self.payment_gen.generate_mpesa_payment(order_ref=order_ref, amount=amount)
        self.clock.advance_hours(hours_early)
        order = {
            "order_ref": order_ref,
            "shop_id": "SHOP-002-WESTLANDS",
            "amount_kes": amount,
            "currency": "KES",
            "created_at": self.clock.now_iso(),
        }
        return payment, order

    def create_split_payments(
        self,
        order_ref: str,
        total_amount: float,
        rail: str = "mpesa",
        ratios: list[float] | None = None,
    ) -> list[dict[str, Any]]:
        """Scenario C6: Two or more distinct payments summing to total order amount."""
        if ratios is None:
            ratios = [0.4, 0.6]

        payments = []
        for i, ratio in enumerate(ratios):
            part_amount = round(total_amount * ratio, 2)
            if i == len(ratios) - 1:
                # Ensure sum matches total exactly
                sum_previous = sum(
                    float(p["TransAmount"] if "TransAmount" in p else p["amount"]) for p in payments
                )
                part_amount = round(total_amount - sum_previous, 2)

            if rail == "mpesa":
                p = self.payment_gen.generate_mpesa_payment(order_ref=order_ref, amount=part_amount)
            else:
                p = self.payment_gen.generate_bank_payment(order_ref=order_ref, amount=part_amount)

            payments.append(p)
            self.clock.advance_minutes(30.0)

        return payments

    def create_underpayment(
        self, order_ref: str, total_amount: float, ratio: float = 0.7
    ) -> dict[str, Any]:
        """Scenario C7: Partial payment that is never finalized."""
        partial_amount = round(total_amount * ratio, 2)
        return self.payment_gen.generate_mpesa_payment(order_ref=order_ref, amount=partial_amount)

    def create_overpayment(
        self, order_ref: str, total_amount: float, ratio: float = 1.25
    ) -> dict[str, Any]:
        """Scenario C8: Payment exceeding total order amount."""
        excess_amount = round(total_amount * ratio, 2)
        return self.payment_gen.generate_bank_payment(order_ref=order_ref, amount=excess_amount)

    def create_orphan_payment(self, amount: float = 2500.0) -> dict[str, Any]:
        """Scenario C5: Payment referencing nonexistent order (typo in Paybill bill ref)."""
        return self.payment_gen.generate_mpesa_payment(order_ref="ORD-99999999", amount=amount)

    def create_late_payment(
        self, order_ref: str, amount: float, hours_late: float = 52.0
    ) -> dict[str, Any]:
        """Scenario C4: Payment arriving after nominal 48h payment window."""
        self.clock.advance_hours(hours_late)
        return self.payment_gen.generate_mpesa_payment(order_ref=order_ref, amount=amount)

    def create_malformed_payload(self) -> str:
        """Scenario C9: Corrupted or unparseable JSON payload."""
        return (
            '{"TransactionType": "Pay Bill", "TransID": "RKT-CORRUPTED", '
            '"TransAmount": 500, UNCLOSED_JSON'
        )
