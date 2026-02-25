"""Sink queue manager for controlled concurrent writes.

Supports two modes:

1. **Stream mode** (preferred for pipelines)  —  ``open_stream()`` /
   ``submit()`` / ``close_stream()``.  All DataFrames are appended to a
   single CSV file (header written once).  After closing, call
   ``stream_to_parquet()`` for a zero-RAM lazy conversion.

2. **Standalone mode**  —  ``submit(df, output_path)`` writes a separate
   file per call (parquet or csv) with retry logic.
"""

import logging
import threading
from pathlib import Path
from typing import Optional

import polars as pl

from src.memory_monitor import MemoryMonitor

logger = logging.getLogger(__name__)


class SinkManager:
    """FIFO sink queue with configurable concurrency limit.

    In *stream mode*, a single CSV file is opened once and every
    ``submit()`` call appends rows — the header is written only on the
    first call.  The semaphore still governs how many threads may enter
    ``submit()`` concurrently; a ``threading.Lock`` serialises the actual
    file writes so the CSV is never interleaved.
    """

    def __init__(
        self,
        max_concurrent: int = 3,
        output_format: str = "parquet",
        retry_attempts: int = 3,
        memory_monitor: Optional[MemoryMonitor] = None,
    ) -> None:
        self._semaphore = threading.Semaphore(max_concurrent)
        self._output_format = output_format.lower()
        self._retry_attempts = retry_attempts
        self._memory = memory_monitor
        self._lock = threading.Lock()
        self._active_count = 0

        # Stream-mode state
        self._stream_path: Optional[Path] = None
        self._stream_file = None          # TextIO handle
        self._header_written: bool = False
        self._stream_rows: int = 0

        logger.info(
            "SinkManager ready: max_concurrent=%d format=%s retries=%d",
            max_concurrent,
            self._output_format,
            retry_attempts,
        )

    # ------------------------------------------------------------------
    # Stream mode — open / submit / close
    # ------------------------------------------------------------------

    def open_stream(self, output_path: Path) -> None:
        """Create (or truncate) *output_path* for streaming appends.

        Every subsequent ``submit(df)`` will append rows to this file.
        The CSV header is written automatically on the first append.
        Call ``close_stream()`` when all dates have been processed.
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self._stream_path = output_path
        # newline="" prevents Python from doubling \r\n on Windows
        self._stream_file = open(  # noqa: SIM115
            output_path, "w", encoding="utf-8", newline="",
        )
        self._header_written = False
        self._stream_rows = 0
        logger.info("Opened stream → %s", output_path)

    def close_stream(self) -> Optional[Path]:
        """Flush and close the stream.  Returns the CSV path."""
        if self._stream_file is not None:
            self._stream_file.close()
            logger.info(
                "Closed stream (%d total rows) → %s",
                self._stream_rows,
                self._stream_path,
            )
        path = self._stream_path
        self._stream_file = None
        self._stream_path = None
        self._header_written = False
        self._stream_rows = 0
        return path

    @property
    def streaming(self) -> bool:
        """``True`` when a stream file is open."""
        return self._stream_file is not None

    # ------------------------------------------------------------------
    # Public API — submit
    # ------------------------------------------------------------------

    def submit(self, df: pl.DataFrame, output_path: Optional[Path] = None) -> None:
        """Write *df*.  Behaviour depends on the current mode:

        * **Stream mode** (``open_stream`` was called) — *df* is appended
          to the open CSV file.  *output_path* is ignored.
        * **Standalone mode** — *df* is written to *output_path* as a
          self-contained file (parquet or csv) with retry logic.

        In both modes the caller blocks on the semaphore until a slot
        opens, and the write happens on the calling thread.
        """
        self._semaphore.acquire()
        self._inc_active()
        try:
            if self._stream_file is not None:
                self._append_to_stream(df)
            elif output_path is not None:
                self._write_with_retry(df, output_path)
            else:
                raise ValueError(
                    "No stream open and no output_path provided"
                )
        finally:
            self._dec_active()
            self._semaphore.release()

    @property
    def active_count(self) -> int:
        """Number of sinks currently in progress."""
        with self._lock:
            return self._active_count

    # ------------------------------------------------------------------
    # Lazy parquet conversion
    # ------------------------------------------------------------------

    @staticmethod
    def stream_to_parquet(csv_path: Path, parquet_path: Path) -> None:
        """Lazy-scan *csv_path* and sink to *parquet_path* (zero extra RAM)."""
        logger.info("Converting %s → %s (lazy scan + sink)", csv_path, parquet_path)
        pl.scan_csv(str(csv_path)).sink_parquet(str(parquet_path))
        logger.info("[OK] Parquet written → %s", parquet_path)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _inc_active(self) -> None:
        with self._lock:
            self._active_count += 1
            logger.info("Sink slot acquired (%d active)", self._active_count)

    def _dec_active(self) -> None:
        with self._lock:
            self._active_count -= 1
            logger.info("Sink slot released (%d active)", self._active_count)

    def _append_to_stream(self, df: pl.DataFrame) -> None:
        """Append *df* rows to the open CSV stream.

        The header line is written only on the first call.  Subsequent
        calls strip the header so the file is a valid, contiguous CSV.
        Thread-safe via ``self._lock``.
        """
        if df.is_empty():
            logger.debug("Skipping empty DataFrame append")
            return

        if self._memory is not None:
            self._memory.check_or_abort()

        csv_text: str = df.write_csv()

        with self._lock:
            if not self._header_written:
                self._stream_file.write(csv_text)
                self._header_written = True
            else:
                # Skip the first line (column names)
                first_nl = csv_text.index("\n")
                self._stream_file.write(csv_text[first_nl + 1:])

            self._stream_file.flush()
            self._stream_rows += df.height

        logger.info("[OK] Appended %d rows to stream (%d total)", df.height, self._stream_rows)

    def _write_with_retry(self, df: pl.DataFrame, output_path: Path) -> None:
        """Write df with up to N retries (standalone mode)."""
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if self._memory is not None:
            self._memory.check_or_abort()

        last_err: Optional[Exception] = None
        for attempt in range(1, self._retry_attempts + 1):
            try:
                self._do_write(df, output_path)
                logger.info("[OK] Wrote %s", output_path)
                return
            except Exception as exc:  # noqa: BLE001
                last_err = exc
                logger.warning(
                    "Sink attempt %d/%d failed for %s: %s",
                    attempt,
                    self._retry_attempts,
                    output_path,
                    exc,
                )

        logger.error("All %d sink attempts failed for %s", self._retry_attempts, output_path)
        if last_err is not None:
            raise last_err

    def _do_write(self, df: pl.DataFrame, output_path: Path) -> None:
        """Perform the actual write in the requested format (standalone mode)."""
        if self._output_format == "parquet":
            df.write_parquet(str(output_path))
        else:
            df.write_csv(str(output_path))
