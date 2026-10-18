"""Unit tests for order payment reconciliation state machine."""

from datetime import datetime

from streaming.common.status import (
    OrderPaymentStatus,
    check_lateness,
    determine_order_status,
)


def test_order_status_transitions():
    """Verify order reconciliation state machine across all payment conditions."""
    order_amount = 5000.0

    # 1. Zero payment -> UNPAID
    assert determine_order_status(order_amount, paid_total=0.0) == OrderPaymentStatus.UNPAID

    # 2. Less paid within active window -> PARTIAL
    assert (
        determine_order_status(order_amount, paid_total=2500.0, is_window_expired=False)
        == OrderPaymentStatus.PARTIAL
    )

    # 3. Less paid after window closed -> UNDERPAID
    assert (
        determine_order_status(order_amount, paid_total=2500.0, is_window_expired=True)
        == OrderPaymentStatus.UNDERPAID
    )

    # 4. Exact payment -> PAID
    assert determine_order_status(order_amount, paid_total=5000.0) == OrderPaymentStatus.PAID

    # 5. Exact payment with floating point micro-difference within tolerance -> PAID
    assert determine_order_status(order_amount, paid_total=5000.004) == OrderPaymentStatus.PAID

    # 6. Paid greater than order amount -> OVERPAID
    assert determine_order_status(order_amount, paid_total=5500.0) == OrderPaymentStatus.OVERPAID


def test_check_lateness():
    """Verify lateness detector against 48-hour window."""
    order_time = datetime(2026, 10, 18, 8, 0, 0)

    # 24 hours later -> Not late
    pay_ontime = datetime(2026, 10, 19, 8, 0, 0)
    assert not check_lateness(pay_ontime, order_time, window_hours=48.0)

    # 48.5 hours later -> Late
    pay_late = datetime(2026, 10, 20, 8, 30, 0)
    assert check_lateness(pay_late, order_time, window_hours=48.0)
