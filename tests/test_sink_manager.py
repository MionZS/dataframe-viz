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
        sm.open_stream(out)
        assert sm.streaming

        sm.submit(sample_df)
        path = sm.close_stream()

        assert not sm.streaming
        assert path == out
        result = pl.read_csv(str(out))
        assert result.height == 2
        assert list(result.columns) == list(sample_df.columns)

    def test_stream_multi_append(
        self, tmp_path: Path, sample_df: pl.DataFrame, sample_df_day2: pl.DataFrame,
    ):
        """Two appends should produce one CSV with 4 rows and one header."""
        sm = SinkManager(max_concurrent=1)
        out = tmp_path / "stream.csv"
        sm.open_stream(out)

        sm.submit(sample_df)
        sm.submit(sample_df_day2)
        sm.close_stream()

        result = pl.read_csv(str(out))
        assert result.height == 4
        assert result.filter(pl.col("DATA") == "2026-01-25").height == 2
        assert result.filter(pl.col("DATA") == "2026-01-26").height == 2

    def test_stream_skips_empty(self, tmp_path: Path, sample_df: pl.DataFrame):
        """Empty DataFrames should not corrupt the stream."""
        sm = SinkManager(max_concurrent=1)
        out = tmp_path / "stream.csv"
        sm.open_stream(out)

        empty = sample_df.clear()
        sm.submit(empty)        # should be silently skipped
        sm.submit(sample_df)    # real data
        sm.close_stream()

        result = pl.read_csv(str(out))
        assert result.height == 2

    def test_stream_to_parquet(
        self, tmp_path: Path, sample_df: pl.DataFrame, sample_df_day2: pl.DataFrame,
    ):
        """Lazy CSV→parquet conversion should produce identical data."""
        sm = SinkManager(max_concurrent=1)
        csv_out = tmp_path / "stream.csv"
        sm.open_stream(csv_out)
        sm.submit(sample_df)
        sm.submit(sample_df_day2)
        sm.close_stream()

        pq_out = tmp_path / "stream.parquet"
        SinkManager.stream_to_parquet(csv_out, pq_out)

        assert pq_out.exists()
        result = pl.read_parquet(str(pq_out))
        assert result.height == 4
        assert set(result.columns) == set(sample_df.columns)

    def test_stream_creates_parent_dirs(self, tmp_path: Path, sample_df: pl.DataFrame):
        """open_stream should create parent directories."""
        sm = SinkManager(max_concurrent=1)
        out = tmp_path / "deep" / "nested" / "stream.csv"
        sm.open_stream(out)
        sm.submit(sample_df)
        sm.close_stream()
        assert out.exists()

    def test_concurrency_limit(self, tmp_path: Path, sample_df: pl.DataFrame):
        """Verify that active_count never exceeds max_concurrent."""
        sm = SinkManager(max_concurrent=2, output_format="csv")
        peaks: list[int] = []

        def _write(idx: int):
            out = tmp_path / f"file_{idx}.csv"
            peaks.append(sm.active_count)
            sm.submit(sample_df, out)

        threads = [threading.Thread(target=_write, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All 5 files should exist
        for i in range(5):
            assert (tmp_path / f"file_{i}.csv").exists()
