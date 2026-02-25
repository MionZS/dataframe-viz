"""Unit tests for join_daily module."""
import tempfile
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from src.join_daily import join_with_diario


@pytest.fixture()
def diario_dir(tmp_path: Path) -> str:
    """Create a temp directory with a fake Diario parquet for 2026-01-25."""
    diario_df = pl.DataFrame({
        "NIO": ["A", "B", "C", "D"],
        "MUNICIPIO": ["CURITIBA", "LONDRINA", "CURITIBA", "MARINGA"],
    })
    path = tmp_path / "Diario_2026-01-25.parquet"
    diario_df.write_parquet(str(path))
    return str(tmp_path)


@pytest.fixture()
def disp_df() -> pl.DataFrame:
    """Fake DISP frame with 3 meters."""
    return pl.DataFrame({
        "NIO": ["A", "B", "Z"],
        "DISP": [1, 0, 1],
    }).cast({"NIO": pl.Utf8, "DISP": pl.Int8})


class TestJoinDaily:
    """Tests for join_with_diario()."""

    def test_left_join_keeps_all_diario(self, disp_df: pl.DataFrame, diario_dir: str):
        """Left join from Diario should keep all 4 Diario meters (A,B,C,D).
        Z (only in DISP) is dropped; C,D get DISP=0."""
        result = join_with_diario(
            disp_df,
            target_date=datetime(2026, 1, 25),
            diario_dir=diario_dir,
            origem="ORCA",
        )

        assert result.height == 4  # A, B, C, D from Diario
        nios = set(result["NIO"].to_list())
        assert nios == {"A", "B", "C", "D"}
        assert all(v == "ORCA" for v in result["ORIGEM"].to_list())

        disp = dict(zip(result["NIO"].to_list(), result["DISP"].to_list()))
        # A matched DISP=1, B matched DISP=0
        assert disp["A"] == 1
        assert disp["B"] == 0
        # C, D have no DISP data → default to 0
        assert disp["C"] == 0
        assert disp["D"] == 0

    def test_missing_diario_returns_empty(self, disp_df: pl.DataFrame, diario_dir: str):
        """Missing Diario file should return empty frame, not raise."""
        result = join_with_diario(
            disp_df,
            target_date=datetime(2099, 1, 1),
            diario_dir=diario_dir,
            origem="SANPLAT",
        )
        assert result.height == 0
        assert "MUNICIPIO" in result.columns
        assert "ORIGEM" in result.columns

    def test_municipio_correct(self, disp_df: pl.DataFrame, diario_dir: str):
        """MUNICIPIO should come from Diario for all meters."""
        result = join_with_diario(
            disp_df,
            target_date=datetime(2026, 1, 25),
            diario_dir=diario_dir,
            origem="TEST",
        )
        muni = dict(zip(result["NIO"].to_list(), result["MUNICIPIO"].to_list()))
        assert muni["A"] == "CURITIBA"
        assert muni["B"] == "LONDRINA"
        assert muni["C"] == "CURITIBA"
        assert muni["D"] == "MARINGA"
