"""Pure function state machine and anomaly classification for order reconciliation."""

from datetime import datetime, timedelta


class OrderPaymentStatus:
    UNPAID = "UNPAID"
    PARTIAL = "PARTIAL"
    PAID = "PAID"
    OVERPAID = "OVERPAID"
    UNDERPAID = "UNDERPAID"


class AnomalyType:
    DUPLICATE_CONFLICT = "DUPLICATE_CONFLICT"
    ORPHAN_PAYMENT = "ORPHAN_PAYMENT"
    LATE_PAYMENT = "LATE_PAYMENT"
    UNDERPAID = "UNDERPAID"
    OVERPAID = "OVERPAID"
    MALFORMED_PAYLOAD = "MALFORMED_PAYLOAD"


def determine_order_status(
    order_amount: float | None,
    paid_total: float,
    is_window_expired: bool = False,
    tolerance: float = 0.01,
) -> str:
    """Evaluate order payment status state machine based on collected funds."""
    if order_amount is None or order_amount <= 0:
        return OrderPaymentStatus.UNPAID if paid_total <= 0 else OrderPaymentStatus.OVERPAID

    diff = round(paid_total - order_amount, 2)

    if paid_total <= 0:
        return OrderPaymentStatus.UNPAID

    if abs(diff) <= tolerance:
        return OrderPaymentStatus.PAID

    if diff > tolerance:
        return OrderPaymentStatus.OVERPAID

    # Paid less than order total:
    if is_window_expired:
        return OrderPaymentStatus.UNDERPAID
    return OrderPaymentStatus.PARTIAL


def check_lateness(
    payment_time: datetime,
    order_created_time: datetime | None,
    window_hours: float = 48.0,
) -> bool:
    """Return True if payment arrived after the nominal payment window expired."""
    if order_created_time is None:
        return False
    return payment_time > (order_created_time + timedelta(hours=window_hours))
