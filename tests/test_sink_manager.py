"""Unit tests for sink_manager module."""
import threading
from pathlib import Path

import polars as pl
import pytest

from src.sink_manager import SinkManager


@pytest.fixture()
def sample_df() -> pl.DataFrame:
    """Small test DataFrame."""
    return pl.DataFrame({
        "MUNICIPIO": ["CURITIBA", "LONDRINA"],
        "ORIGEM": ["ORCA", "ORCA"],
        "CONTAGEM_COMM": [10, 5],
        "CONTAGEM_TOT": [20, 15],
        "DATA": ["2026-01-25", "2026-01-25"],
    })


@pytest.fixture()
def sample_df_day2() -> pl.DataFrame:
    """Second day for stream testing."""
    return pl.DataFrame({
        "MUNICIPIO": ["CURITIBA", "LONDRINA"],
        "ORIGEM": ["ORCA", "ORCA"],
        "CONTAGEM_COMM": [12, 8],
        "CONTAGEM_TOT": [20, 15],
        "DATA": ["2026-01-26", "2026-01-26"],
    })


class TestSinkManagerStandalone:
    """Standalone-mode tests (write individual files)."""

    def test_write_csv(self, tmp_path: Path, sample_df: pl.DataFrame):
        """CSV sink should produce a readable file."""
        sm = SinkManager(max_concurrent=1, output_format="csv")
        out = tmp_path / "test.csv"
        sm.submit(sample_df, out)

        assert out.exists()
        result = pl.read_csv(str(out))
        assert result.height == 2
        assert "MUNICIPIO" in result.columns

    def test_write_parquet(self, tmp_path: Path, sample_df: pl.DataFrame):
        """Parquet sink should produce a readable file."""
        sm = SinkManager(max_concurrent=1, output_format="parquet")
        out = tmp_path / "test.parquet"
        sm.submit(sample_df, out)

        assert out.exists()
        result = pl.read_parquet(str(out))
        assert result.height == 2

    def test_creates_parent_dirs(self, tmp_path: Path, sample_df: pl.DataFrame):
        """Sink should create parent directories if needed."""
        sm = SinkManager(max_concurrent=1, output_format="csv")
        out = tmp_path / "sub" / "dir" / "test.csv"
        sm.submit(sample_df, out)
        assert out.exists()


class TestSinkManagerStream:
    """Stream-mode tests (single-file append)."""

    def test_stream_single_append(self, tmp_path: Path, sample_df: pl.DataFrame):
        """Streaming a single DataFrame should produce a valid CSV."""
        sm = SinkManager(max_concurrent=1)
        out = tmp_path / "stream.csv"
        sm.open_s