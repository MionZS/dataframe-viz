"""Unit tests for pipeline_orchestrator — date range, aggregation, enrichment."""
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from src.pipeline_orchestrator import aggregate, build_date_range, _run_enrichment


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


class TestRunEnrichment:
    """Tests for _run_enrichment()."""

    def test_calls_process_orca_and_sanplat(self, tmp_path: Path):
        """Enrichment phase should call both process_orca and process_sanplat."""
        orca_raw = tmp_path / "orca" / "raw.parquet"
        orca_ref = tmp_path / "orca" / "ref.csv"
        orca_enriched = tmp_path / "trusted" / "ORCA" / "enriched.csv"
        sanplat_raw = tmp_path / "sanplat" / "raw.csv"
        sanplat_ref = tmp_path / "sanplat" / "ref.csv"
        sanplat_enriched = tmp_path / "trusted" / "SANPLAT" / "enriched.csv"

        # Create dummy source files so exists() checks pass
        for f in (orca_raw, orca_ref, sanplat_raw, sanplat_ref):
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_text("x")

        cfg = {
            "paths": {
                "orca_raw": str(orca_raw),
                "orca_ref_date": str(orca_ref),
                "orca_enriched": str(orca_enriched),
                "sanplat_refined": str(sanplat_raw),
                "sanplat_ref_date": str(sanplat_ref),
                "sanplat_enriched": str(sanplat_enriched),
            },
        }

        with patch("src.pipeline_orchestrator.process_orca") as mock_orca, \
             patch("src.pipeline_orchestrator.process_sanplat") as mock_sanplat:
            mock_orca.return_value = orca_enriched
            mock_sanplat.return_value = sanplat_enriched
            _run_enrichment(cfg)

        mock_orca.assert_called_once_with(orca_raw, orca_ref, orca_enriched.parent)
        mock_sanplat.assert_called_once_with(sanplat_raw, sanplat_ref, sanplat_enriched.parent)

    def test_skips_when_files_missing(self, tmp_path: Path):
        """Enrichment should skip gracefully when source files don't exist."""
        cfg = {
            "paths": {
                "orca_raw": str(tmp_path / "missing_orca.parquet"),
                "orca_ref_date": str(tmp_path / "missing_ref.csv"),
                "orca_enriched": str(tmp_path / "out" / "orca.csv"),
                "sanplat_refined": str(tmp_path / "missing_sanplat.csv"),
                "sanplat_ref_date": str(tmp_path / "missing_ref2.csv"),
                "sanplat_enriched": str(tmp_path / "out" / "sanplat.csv"),
            },
        }

        with patch("src.pipeline_orchestrator.process_orca") as mock_orca, \
             patch("src.pipeline_orchestrator.process_sanplat") as mock_sanplat:
            _run_enrichment(cfg)  # should not raise

        mock_orca.assert_not_called()
        mock_sanplat.assert_not_called()
