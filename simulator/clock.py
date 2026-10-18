"""Virtual accelerated event-time clock for Rekon simulator.

Ensures strict determinism across test runs and replays.
All pipeline decisions evaluate against event time, never wall-clock now().
"""

from datetime import UTC, datetime


class VirtualClock:
    """Manages an accelerated deterministic virtual event timeline."""

    def __init__(self, start_iso: str = "2026-10-18T08:00:00Z", speedup_ratio: float = 600.0):
        """Initialize clock with fixed start timestamp and speedup ratio.

        Default speedup: 1 real second = 600 simulated seconds (10 minutes).
        """
        self.current_time = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
        self.speedup_ratio = speedup_ratio

    def now(self) -> datetime:
        """Return current virtual event time."""
        return self.current_time

    def now_iso(self) -> str:
        """Return current virtual event time in ISO-8601 format."""
        return self.current_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    def now_mpesa(self) -> str:
        """Return current virtual event time in Daraja yyyyMMddHHmmss format."""
        return self.current_time.strftime("%Y%m%d%H%M%S")

    def advance_seconds(self, seconds: float) -> datetime:
        """Advance the virtual clock by a specific number of simulated seconds."""
        from datetime import timedelta

        self.current_time += timedelta(seconds=seconds)
        return self.current_time

    def advance_minutes(self, minutes: float) -> datetime:
        """Advance the virtual clock by simulated minutes."""
        return self.advance_seconds(minutes * 60)

    def advance_hours(self, hours: float) -> datetime:
        """Advance the virtual clock by simulated hours."""
        return self.advance_seconds(hours * 3600)

    def set_time(self, new_time: datetime) -> None:
        """Set explicit virtual event time."""
        if new_time.tzinfo is None:
            new_time = new_time.replace(tzinfo=UTC)
        self.current_time = new_time
