"""Unit tests for the moving window OR logic."""
import tempfile
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from src.moving_window import compute_disp


def _write_enriched_csv(tmp_dir: Path, rows: list[dict]) -> str:
    """Write a fake enriched CSV with date-named columns."""
    df = pl.DataFrame(rows)
    path = str(tmp_dir / "enriched.csv")
    df.write_csv(path)
    return path


@pytest.fixture()
def enriched_csv(tmp_path: Path) -> str:
    """Create a minimal enriched CSV for testing.

    Columns: NIO, 20/01/2026, 21/01/2026, 22/01/2026, 23/01/2026, 24/01/2026

    Data (3 meters):
        A   | 1 | 0 | 0 | 0 | 0
        B   | 0 | 0 | 0 | 0 | 0
        C   | 0 | 0 | 1 | 0 | 0
    """
    rows = [
        {"NIO": "A", "20/01/2026": "1", "21/01/2026": "0", "22/01/2026": "0", "23/01/2026": "0", "24/01/2026": "0"},
        {"NIO": "B", "20/01/2026": "0", "21/01/2026": "0", "22/01/2026": "0", "23/01/2026": "0", "24/01/2026": "0"},
        {"NIO": "C", "20/01/2026": "0", "21/01/2026": "0", "22/01/2026": "1", "23/01/2026": "0", "24/01/2026": "0"},
    ]
    return _write_enriched_csv(tmp_path, rows)


class TestMovingWindow:
    """Tests for compute_disp()."""

    def test_binary_or_window_5(self, enriched_csv: str):
        """Target 25/01 with window=5 should look at 20-24/01."""
        target = datetime(2026, 1, 25)
        result = compute_disp(enriched_csv, target, window_days=5)

        assert result.height == 3
        disp = dict(zip(result["NIO"].to_list(), result["DISP"].to_list()))
        # A had a 1 on day 5 (20/01) → DISP=1
        assert disp["A"] == 1
        # B all zeros → DISP=0
        assert disp["B"] == 0
        # C had a 1 on day 3 (22/01) → DISP=1
        assert disp["C"] == 1

    def test_smaller_window(self, enriched_csv: str):
        """Window=2 for target 25/01 should use only 23-24/01 (cols 2,1).
        All are 0 for every meter → all DISP=0."""
        target = datetime(2026, 1, 25)
        result = compute_disp(enriched_csv, target, window_days=2)

        disp = dict(zip(result["NIO"].to_list(), result["DISP"].to_list()))
        assert disp["A"] == 0
        assert disp["B"] == 0
        assert disp["C"] == 0

    def test_no_columns_in_range(self, enriched_csv: str):
        """Target far in the future → no columns match → empty result."""
        target = datetime(2027, 1, 1)
        result = compute_disp(enriched_csv, target, window_days=5)
        assert result.height == 0

    def test_binarize_converts_fractional(self, tmp_path: Path):
        """ORCA-style fractional values (0.25, 0.5, 0.75) should become 1."""
        rows = [
            {"NIO": "X", "23/01/2026": "0,25", "24/01/2026": "0"},
            {"NIO": "Y", "23/01/2026": "0", "24/01/2026": "0,75"},
            {"NIO": "Z", "23/01/2026": "0", "24/01/2026": "0"},
        ]
        path = _write_enriched_csv(tmp_path, rows)

        target = datetime(2026, 1, 25)
        result = compute_disp(path, target, window_days=5, binarize=True)

        disp = dict(zip(result["NIO"].to_list(), result["DISP"].to_list()))
        assert disp["X"] == 1
        assert disp["Y"] == 1
        assert disp["Z"] == 0
