"""Verify integrity of pipeline output files."""

import logging
from pathlib import Path
from typing import Any, Dict, Tuple

import polars as pl

logger = logging.getLogger(__name__)


def verify_output_integrity(output_path: str) -> Tuple[bool, Dict[str, Any]]:
    """Verify the integrity of a pipeline output file (CSV or Parquet).

    Parameters
    ----------
    output_path:
        Path to the output file (CSV or Parquet).

    Returns
    -------
    Tuple[bool, dict]
        (is_valid, report) where report contains detailed checks.
    """
    report = {
        "file_exists": False,
        "file_size_bytes": 0,
        "row_count": 0,
        "column_count": 0,
        "expected_columns": [],
        "missing_columns": [],
        "column_types": {},
        "disp_range": None,
        "disp_in_unit_interval": False,
        "null_counts": {},
        "has_duplicates": False,
        "errors": [],
    }

    path = Path(output_path)

    # Check file exists
    if not path.exists():
        report["errors"].append(f"File not found: {output_path}")
        return False, report

    report["file_exists"] = True
    report["file_size_bytes"] = path.stat().st_size

    if report["file_size_bytes"] == 0:
        report["errors"].append("File is empty")
        return False, report

    # Read file
    try:
        if output_path.endswith(".parquet"):
            df = pl.read_parquet(output_path)
        else:
            df = pl.read_csv(output_path)
    except Exception as exc:
        report["errors"].append(f"Failed to read file: {exc}")
        return False, report

    report["row_count"] = df.height
    report["column_count"] = df.width

    # Check expected columns
    expected_columns = [
        "MUNICIPIO",
        "INTELIGENTE",
        "CONTAGEM_COMM",
        "CONTAGEM_TOT",
        "DISP",
        "DATA",
    ]
    report["expected_columns"] = expected_columns
    report["missing_columns"] = [c for c in expected_columns if c not in df.columns]

    if report["missing_columns"]:
        report["errors"].append(
            f"Missing columns: {report['missing_columns']}"
        )

    # Column types
    report["column_types"] = {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)}

    # Check DISP column (aggregated ratio should be in [0,1])
    if "DISP" in df.columns:
        try:
            disp_col = df.select("DISP")
            disp_min = disp_col.select(pl.col("DISP").min()).item()
            disp_max = disp_col.select(pl.col("DISP").max()).item()
            report["disp_range"] = (float(disp_min), float(disp_max))

            report["disp_in_unit_interval"] = disp_min >= 0.0 and disp_max <= 1.0

            if not report["disp_in_unit_interval"]:
                report["errors"].append(
                    f"DISP column out of expected range [0,1]: {disp_min}–{disp_max}"
                )
        except Exception as exc:
            report["errors"].append(f"Failed to check DISP column: {exc}")

    # Check for nulls
    for col in df.columns:
        null_count = df.select(pl.col(col).null_count()).item()
        if null_count > 0:
            report["null_counts"][col] = null_count

    if report["null_counts"]:
        report["errors"].append(f"Unexpected nulls: {report['null_counts']}")

    # Check for duplicates (by MUNICIPIO, INTELIGENTE, DATA)
    if all(c in df.columns for c in ["MUNICIPIO", "INTELIGENTE", "DATA"]):
        try:
            n_unique = df.select(
                pl.struct(["MUNICIPIO", "INTELIGENTE", "DATA"])
            ).n_unique()
            report["has_duplicates"] = n_unique < df.height
            if report["has_duplicates"]:
                report["errors"].append(
                    f"Found duplicate rows by [MUNICIPIO, INTELIGENTE, DATA]: "
                    f"{df.height - n_unique} duplicates"
                )
        except Exception as exc:
            report["errors"].append(f"Failed to check duplicates: {exc}")

    is_valid = len(report["errors"]) == 0

    return is_valid, report


def print_verification_report(report: Dict) -> None:
    """Print verification report in human-readable format."""
    print("\n" + "=" * 70)
    print("OUTPUT FILE VERIFICATION REPORT")
    print("=" * 70)

    print(f"\n✓ File exists: {report['file_exists']}")
    if report["file_exists"]:
        print(f"  Size: {report['file_size_bytes']:,} bytes")
        print(f"  Rows: {report['row_count']:,}")
        print(f"  Columns: {report['column_count']}")

    if report["missing_columns"]:
        print(f"\n✗ Missing columns: {report['missing_columns']}")
    else:
        print("\n✓ All expected columns present")

    if report["column_types"]:
        print("\nColumn types:")
        for col, dtype in report["column_types"].items():
            print(f"  {col:20} → {dtype}")

    if report["disp_range"]:
        print(f"\n✓ DISP range: {report['disp_range'][0]:.4f}–{report['disp_range'][1]:.4f}")
        if report["disp_in_unit_interval"]:
            print("  ✓ All values are within [0,1]")
        else:
            print("  ✗ Values outside [0,1] detected")

    if report["null_counts"]:
        print("\n✗ Null values found:")
        for col, count in report["null_counts"].items():
            print(f"  {col}: {count:,}")
    else:
        print("\n✓ No null values")

    if report["has_duplicates"]:
        print("\n✗ Duplicate rows detected by [MUNICIPIO, INTELIGENTE, DATA]")
    else:
        print("\n✓ No duplicates")

    if report["errors"]:
        print(f"\n✗ VALIDATION FAILED with {len(report['errors'])} error(s):")
        for i, err in enumerate(report["errors"], 1):
            print(f"  {i}. {err}")
    else:
        print("\n✓ VALIDATION PASSED — output file is OK")

    print("=" * 70 + "\n")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m src.verify_output <output_file>")
        sys.exit(1)

    output_file = sys.argv[1]
    is_valid, report = verify_output_integrity(output_file)
    print_verification_report(report)
    sys.exit(0 if is_valid else 1)
