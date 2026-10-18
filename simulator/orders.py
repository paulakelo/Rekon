"""Order generator for Rekon simulator.

Generates Dukastack-compatible retail order headers:
- order_ref: ORD-00000001, ORD-00000002 (matching dukastack's natural key format)
- shop_id: Kenyan retail shop identifier (SHOP-001 to SHOP-010)
- amount_kes: Order bill amount in Kenyan Shillings
- created_at: Event timestamp from the virtual clock
"""

import json
import random
from typing import Any

from simulator.clock import VirtualClock

KENYAN_SHOPS = [
    "SHOP-001-NAIROBI-CBD",
    "SHOP-002-WESTLANDS",
    "SHOP-003-KILIMANI",
    "SHOP-004-MOMBASA-TOWN",
    "SHOP-005-NYALI",
    "SHOP-006-KISUMU-CENTRAL",
    "SHOP-007-ELDORET-MAIN",
    "SHOP-008-NAKURU-HIGHWAY",
]


class OrderGenerator:
    """Generates synthetic retail POS orders aligned with dukastack."""

    def __init__(self, clock: VirtualClock, seed: int = 42):
        self.clock = clock
        self.rng = random.Random(seed)
        self.order_counter = 0

    def next_order(self, fixed_amount: float | None = None) -> dict[str, Any]:
        """Generate a single order header at the current virtual clock time."""
        self.order_counter += 1
        order_ref = f"ORD-{self.order_counter:08d}"
        shop_id = self.rng.choice(KENYAN_SHOPS)

        if fixed_amount is not None:
            amount = round(fixed_amount, 2)
        else:
            # Typical Kenyan retail duka basket sizes: 350 to 12,500 KES
            amount = round(self.rng.uniform(350.0, 12500.0), 2)

        return {
            "order_ref": order_ref,
            "shop_id": shop_id,
            "amount_kes": amount,
            "currency": "KES",
            "created_at": self.clock.now_iso(),
        }

    def generate_batch(self, count: int) -> list[dict[str, Any]]:
        """Generate a batch of orders, advancing the virtual clock between orders."""
        orders = []
        for _ in range(count):
            order = self.next_order()
            orders.append(order)
            # Advance simulated clock by 2 to 15 minutes between customer orders
            self.clock.advance_minutes(self.rng.uniform(2.0, 15.0))
        return orders

    @staticmethod
    def to_json(order: dict[str, Any]) -> str:
        """Serialize order dict to JSON string."""
        return json.dumps(order)
