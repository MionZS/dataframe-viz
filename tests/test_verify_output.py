"""Tests for verify_output module."""

import tempfile
from pathlib import Path

import polars as pl
import pytest

from src.verify_output import verify_output_integrity


@pytest.fixture
def valid_output_fixture():
    """Create a valid output file for testing."""
    df = pl.DataFrame(
        {
            "MUNICIPIO": ["MUNICIPALITY_A", "MUNICIPALITY_A", "MUNICIPALITY_B"],
            "INTELIGENTE": ["Hexing", "Nansen", "Hexing"],
            "CONTAGEM_COMM": [850, 450, 920],
            "CONTAGEM_TOT": [1000, 500, 1100],
            "DISP": [0.85, 0.90, 0.8364],
            "DATA": ["2026-03-07", "2026-03-07", "2026-03-07"],
        }
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        df.write_csv(f.name)
        temp_path = f.name

    yield temp_path

    Path(temp_path).unlink(missing_ok=True)


def test_verify_output_valid_file(valid_output_fixture):
    """Test verification of a valid output file."""
    is_valid, report = verify_output_integrity(valid_output_fixture)

    assert is_valid
    assert report["file_exists"]
    assert report["row_count"] == 3
    assert report["column_count"] == 6
    assert not report["missing_columns"]
    assert not report["errors"]


def test_verify_output_missing_file():
    """Test verification of missing file."""
    is_valid, report = verify_output_integrity("/nonexistent/output.csv")

    assert not is_valid
    assert not report["file_exists"]
    assert "File not found" in report["errors"][0]


def test_verify_output_empty_file():
    """Test verification of empty file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        temp_path = f.name

    try:
        is_valid, report = verify_output_integrity(temp_path)
        assert not is_valid
        assert "File is empty" in report["errors"]
    finally:
        Path(temp_path).unlink(missing_ok=True)


def test_verify_output_missing_columns():
    """Test verification with missing expected columns."""
    df = pl.DataFrame(
        {
            "MUNICIPIO": ["A"],
            "INTELIGENTE": ["Hexing"],
            # Missing CONTAGEM_COMM, CONTAGEM_TOT, DISP, DATA
        }
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        df.write_csv(f.name)
        temp_path = f.name

    try:
        is_valid, report = verify_output_integrity(temp_path)
        assert not is_valid
        assert len(report["missing_columns"]) == 4
    finally:
        Path(temp_path).unlink(missing_ok=True)


def test_verify_output_null_values():
    """Test verification detects null values."""
    df = pl.DataFrame(
        {
            "MUNICIPIO": ["A", None],
            "INTELIGENTE": ["Hexing", "Nansen"],
            "CONTAGEM_COMM": [100, 50],
            "CONTAGEM_TOT": [200, None],
            "DISP": [0.5, 0.25],
            "DATA": ["2026-03-07", "2026-03-07"],
        }
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        df.write_csv(f.name)
        temp_path = f.name

    try:
        is_valid, report = verify_output_integrity(temp_path)
        assert not is_valid
        assert "Unexpected nulls" in str(report["errors"])
    finally:
        Path(temp_path).unlink(missing_ok=True)


def test_verify_output_parquet_format(valid_output_fixture):
    """Test verification works with Parquet files."""
    # Convert CSV to Parquet
    df = pl.read_csv(valid_output_fixture)
    parquet_path = valid_output_fixture.replace(".csv", ".parquet")
    df.write_parquet(parquet_path)

    try:
        is_valid, report = verify_output_integrity(parquet_path)
        assert is_valid
        assert report["row_count"] == 3
    finally:
        Path(parquet_path).unlink(missing_ok=True)


def test_verify_output_disp_ratio_within_unit_interval():
    """Test verification accepts DISP ratios within [0,1]."""
    df = pl.DataFrame(
        {
            "MUNICIPIO": ["A"],
            "INTELIGENTE": ["Hexing"],
            "CONTAGEM_COMM": [850],
            "CONTAGEM_TOT": [1000],
            "DISP": [0.85],
            "DATA": ["2026-03-07"],
        }
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        df.write_csv(f.name)
        temp_path = f.name

    try:
        is_valid, report = verify_output_integrity(temp_path)
        assert is_valid
        assert report["disp_in_unit_interval"]
    finally:
        Path(temp_path).unlink(missing_ok=True)


def test_verify_output_disp_out_of_range():
    """Test verification fails when DISP goes outside [0,1]."""
    df = pl.DataFrame(
        {
            "MUNICIPIO": ["A"],
            "INTELIGENTE": ["Hexing"],
            "CONTAGEM_COMM": [1200],
            "CONTAGEM_TOT": [1000],
            "DISP": [1.2],
            "DATA": ["2026-03-07"],
        }
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        df.write_csv(f.name)
        temp_path = f.name

    try:
        is_valid, report = verify_output_integrity(temp_path)
        assert not is_valid
        assert not report["disp_in_unit_interval"]
    finally:
        Path(temp_path).unlink(missing_ok=True)
