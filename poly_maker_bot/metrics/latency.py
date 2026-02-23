from __future__ import annotations
import time
from collections import deque
from statistics import median

class RollingLatency:
    def __init__(self, maxlen: int = 2000):
        self.samples = deque(maxlen=maxlen)

    def add(self, seconds: float) -> None:
        self.samples.append(seconds)

    def snapshot(self) -> dict:
        if not self.samples:
            return {"n": 0}
        xs = sorted(list(self.samples))
        n = len(xs)
        p50 = xs[int(0.50 * (n - 1))]
        p95 = xs[int(0.95 * (n - 1))]
        p99 = xs[int(0.99 * (n - 1))]
        avg = sum(xs) / n
        return {
            "n": n,
            "avg_ms": avg * 1000,
            "p50_ms": p50 * 1000,
            "p95_ms": p95 * 1000,
            "p99_ms": p99 * 1000,
        }


class RateTracker:
    """Track request rates over time windows."""

    def __init__(self):
        # Store timestamps of requests (keep last 10 minutes)
        self.timestamps = deque(maxlen=10000)

    def record(self) -> None:
        """Record a request at current time."""
        self.timestamps.append(time.time())

    def get_rates(self) -> dict:
        """
        Get request rates for different time windows.

        Returns:
            dict with rates per minute for 1min, 10min windows and total average
        """
        if not self.timestamps:
            return {
                "last_1min": 0.0,
                "last_10min": 0.0,
                "avg_per_min": 0.0,
                "total_requests": 0
            }

        now = time.time()

        # Snapshot the deque to avoid mutation during iteration
        timestamps = list(self.timestamps)

        # Count requests in last 1 minute
        one_min_ago = now - 60
        count_1min = sum(1 for ts in timestamps if ts >= one_min_ago)

        # Count requests in last 10 minutes
        ten_min_ago = now - 600
        count_10min = sum(1 for ts in timestamps if ts >= ten_min_ago)

        # Calculate average rate over entire time window
        total = len(timestamps)
        if total > 0:
            oldest = timestamps[0]
            elapsed_mins = (now - oldest) / 60
            avg_per_min = total / elapsed_mins if elapsed_mins > 0 else 0.0
        else:
            avg_per_min = 0.0

        return {
            "last_1min": count_1min,
            "last_10min": count_10min,
            "avg_per_min": round(avg_per_min, 1),
            "total_requests": total
        }