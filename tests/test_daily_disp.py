"""Tests for daily_disp module."""

import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from src.daily_disp import compute_disp_single_day


@pytest.fixture
def enriched_csv_fixture():
    """Create a temporary enriched CSV for testing."""
    # Create sample data with absolute date columns
    ref_date = datetime(2026, 3, 6)
    prev_date = ref_date - timedelta(days=1)
    col_name = prev_date.strftime("%d/%m/%Y")  # "05/03/2026"

    df = pl.DataFrame(
        {
            "NIO": ["METER001", "METER002", "METER003"],
            col_name: [1, 0, 1],
        }
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        df.write_csv(f.name)
        temp_path = f.name

    yield temp_path

    # Cleanup
    Path(temp_path).unlink(missing_ok=True)


def test_compute_disp_single_day_basic(enriched_csv_fixture):
    """Test that compute_disp_single_day reads and computes correctly."""
    target_date = datetime(2026, 3, 6)
    result = compute_disp_single_day(enriched_csv_fixture, target_date)

    # Should have 3 rows
    assert result.height == 3

    # Should have NIO and DISP columns
    assert set(result.columns) == {"NIO", "DISP"}

    # Check DISP values
    disp_values = result.select("DISP").to_series().to_list()
    assert disp_values == [1, 0, 1]

    # Check NIOs
    nio_values = result.select("NIO").to_series().to_list()
    assert nio_values == ["METER001", "METER002", "METER003"]


def test_compute_disp_single_day_count_communicating(enriched_csv_fixture):
    """Test counting of communicating meters."""
    target_date = datetime(2026, 3, 6)
    result = compute_disp_single_day(enriched_csv_fixture, target_date)

    communicating = result.filter(pl.col("DISP") == 1).height
    assert communicating == 2  # METER001 and METER003


def test_compute_disp_single_day_missing_file():
    """Test handling of missing file."""
    result = compute_disp_single_day("/nonexistent/file.csv", datetime(2026, 3, 6))

    # Should return empty DataFrame
    assert result.is_empty()
    assert set(result.columns) == {"NIO", "DISP"}


def test_compute_disp_single_day_column_not_found():
    """Test handling when target column doesn't exist."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        df = pl.DataFrame({"NIO": ["METER001"], "99/12/1999": [1]})
        df.write_csv(f.name)
        temp_path = f.name

    try:
        # Use a date that won't match the column
        result = compute_disp_single_day(temp_path, datetime(2026, 3, 6))
        assert result.is_empty()
    finally:
        Path(temp_path).unlink(missing_ok=True)
