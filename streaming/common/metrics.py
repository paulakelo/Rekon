"""Micro-batch telemetry and observability tracking."""

import logging
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger("rekon.streaming.metrics")


class MicrobatchMetricsTracker:
    """Collects and records per-batch pipeline metrics."""

    def __init__(self):
        self.history: list[dict[str, Any]] = []

    def record_batch(
        self,
        batch_id: int,
        layer: str,
        rows_in: int,
        valid_rows: int,
        duplicates_dropped: int,
        rejects: int,
        duration_ms: float,
    ) -> dict[str, Any]:
        """Record batch metrics entry."""
        entry = {
            "batch_id": batch_id,
            "layer": layer,
            "recorded_at": datetime.now(UTC).isoformat(),
            "rows_in": rows_in,
            "valid_rows": valid_rows,
            "duplicates_dropped": duplicates_dropped,
            "rejects": rejects,
            "duration_ms": round(duration_ms, 2),
        }
        self.history.append(entry)
        logger.info(
            "Batch %d [%s] completed: in=%d, valid=%d, dupes=%d, rejects=%d (%.1f ms)",
            batch_id,
            layer,
            rows_in,
            valid_rows,
            duplicates_dropped,
            rejects,
            duration_ms,
        )
        return entry


# Global singleton tracker
metrics_tracker = MicrobatchMetricsTracker()
