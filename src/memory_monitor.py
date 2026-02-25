"""Memory monitor for pipeline execution.

Tracks RSS memory usage via psutil and enforces a configurable threshold
to prevent OOM situations during streaming pipeline execution.
"""

import logging
import os
from typing import Optional

import psutil

logger = logging.getLogger(__name__)

_BYTES_PER_MB = 1024 * 1024


class MemoryMonitor:
    """Monitor process RSS memory and enforce a threshold."""

    def __init__(self, threshold_percent: float = 70.0) -> None:
        self._process = psutil.Process(os.getpid())
        self._threshold_percent = threshold_percent
        self._total_ram = psutil.virtual_memory().total
        self._threshold_bytes = int(self._total_ram * threshold_percent / 100)
        logger.info(
            "MemoryMonitor initialised: total_ram=%dMB threshold=%.0f%% (%dMB)",
            self._total_ram // _BYTES_PER_MB,
            threshold_percent,
            self._threshold_bytes // _BYTES_PER_MB,
        )

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    def rss_bytes(self) -> int:
        """Return current RSS in bytes."""
        return self._process.memory_info().rss

    def rss_mb(self) -> float:
        """Return current RSS in megabytes."""
        return self.rss_bytes() / _BYTES_PER_MB

    def usage_percent(self) -> float:
        """Return current RSS as % of total RAM."""
        return self.rss_bytes() / self._total_ram * 100

    def is_within_threshold(self) -> bool:
        """Return True if current RSS is below the configured threshold."""
        return self.rss_bytes() < self._threshold_bytes

    def check_or_abort(self) -> None:
        """Log current memory and raise if threshold exceeded."""
        rss = self.rss_bytes()
        pct = rss / self._total_ram * 100
        logger.info("Memory check: RSS=%.1fMB (%.1f%%)", rss / _BYTES_PER_MB, pct)
        if rss >= self._threshold_bytes:
            msg = (
                f"Memory threshold exceeded: RSS={rss // _BYTES_PER_MB}MB "
                f"({pct:.1f}%) >= {self._threshold_percent:.0f}%"
            )
            logger.critical(msg)
            raise MemoryError(msg)

    def log_status(self, label: str = "") -> None:
        """Log current memory status with an optional label."""
        rss = self.rss_bytes()
        pct = rss / self._total_ram * 100
        prefix = f"[{label}] " if label else ""
        logger.info("%sRSS=%.1fMB (%.1f%%)", prefix, rss / _BYTES_PER_MB, pct)
