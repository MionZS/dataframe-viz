"""Unit tests for pipeline_orchestrator — date range and aggregation."""
from datetime import datetime

import polars as pl
import pytest

from src.pipeline_orchestrator import aggregate, build_date_range


class TestBuildDateRange:
    """Tests for build_date_range()."""

    def test_january_2026_default_window(self):
        """Jan 2026 with window=5 → 36 dates (27 Dec .. 31 Jan)."""
        dates = build_date_range("2026-01", window_days=5)
        assert len(dates) == 36
        assert dates[0] == datetime(2025, 12, 27)
        assert dates[-1] == datetime(2026, 1, 31)

    def test_february_non_leap(self):
        """Feb 2026 with window=5 → 33 dates (27 Jan .. 28 Feb)."""
        dates = build_date_range("2026-02", window_days=5)
        assert dates[0] == datetime(2026, 1, 27)
        assert dates[-1] == datetime(2026, 2, 28)
        assert len(dates) == 33

    def test_custom_window(self):
        """Window=0 → only the month itself."""
        dates = build_date_range("2026-01", window_days=0)
        assert dates[0] == datetime(2026, 1, 1)
        assert len(dates) == 31


class TestAggregate:
    """Tests for aggregate()."""

    def test_basic_aggregation(self):
        """Group 4 rows into 2 groups."""
        joined = pl.DataFrame({
            "NIO": ["A", "B", "C", "D"],
            "MUNICIPIO": ["CWB", "CWB", "LDA", "LDA"],
            "ORIGEM": ["ORCA", "ORCA", "ORCA", "ORCA"],
            "DISP": [1, 0, 1, 1],
        })
        target = datetime(2026, 1, 25)
        agg = aggregate(joined, target)

        assert agg.height == 2
        assert "CONTAGEM_COMM" in agg.columns
        assert "CONTAGEM_TOT" in agg.columns
        assert "DATA" in agg.columns

        cwb = agg.filter(pl.col("MUNICIPIO") == "CWB")
        assert cwb["CONTAGEM_COMM"][0] == 1
        assert cwb["CONTAGEM_TOT"][0] == 2

        lda = agg.filter(pl.col("MUNICIPIO") == "LDA")
        assert lda["CONTAGEM_COMM"][0] == 2
        assert lda["CONTAGEM_TOT"][0] == 2

    def test_empty_input(self):
        """Empty joined frame should return empty aggregated frame."""
        joined = pl.DataFrame({
            "NIO": [], "MUNICIPIO": [], "ORIGEM": [], "DISP": [],
        }).cast({
            "NIO": pl.Utf8, "MUNICIPIO": pl.Utf8,
            "ORIGEM": pl.Utf8, "DISP": pl.Int8,
        })
        agg = aggregate(joined, datetime(2026, 1, 1))
        assert agg.height == 0
        assert set(agg.columns) == {"MUNICIPIO", "ORIGEM", "CONTAGEM_COMM", "CONTAGEM_TOT", "DATA"}

    def test_mixed_origens(self):
        """Two different origens should produce separate groups."""
        joined = pl.DataFrame({
            "NIO": ["A", "B", "C"],
            "MUNICIPIO": ["CWB", "CWB", "CWB"],
            "ORIGEM": ["ORCA", "SANPLAT", "ORCA"],
            "DISP": [1, 1, 0],
        })
        agg = aggregate(joined, datetime(2026, 1, 25))
        assert agg.height == 2  # CWB+ORCA, CWB+SANPLAT
