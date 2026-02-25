#!/usr/bin/env python3
"""
Data Reference Date Enrichment Script

Renames relative date columns to absolute dates in communication data files
based on relative date numbering and a reference date.

This script:
1. Reads a data file (CSV or Parquet) with relative date columns (90, 89, ..., 1)
2. Reads a reference date from a CSV file
3. Calculates actual dates for each relative date column
4. Renames relative columns directly to absolute date strings (DD/MM/YYYY)
5. Saves the enriched file to the trusted directory

The output CSV has absolute dates as column names — no secondary header row.

Usage:
    python enrich_dates.py --data-file path/to/data.csv --ref-file path/to/ref.csv
    python enrich_dates.py --data-file path/to/data.parquet --ref-file path/to/ref.csv

AUTHOR: Data Processing Team
VERSION: 1.1.0
"""

import sys
from pathlib import Path
from typing import Optional, Tuple
from datetime import datetime, timedelta
import argparse

import polars as pl


def read_reference_date(ref_csv_path: Path) -> Tuple[str, datetime]:
    """
    Read reference date from CSV file.
    
    Expected format: single row with date value.
    Returns: (header_name, datetime_object)
    """
    try:
        df = pl.read_csv(ref_csv_path, n_rows=1)
        
        # Get column name and value
        col_name = df.columns[0]
        date_str = df[col_name][0]
        
        # Parse date - handle various formats
        date_formats = [
            "%Y-%m-%d",           # 2026-02-24
            "%d-%m-%Y",           # 24-02-2026
            "%d/%m/%Y",           # 24/02/2026
        ]
        
        parsed_date = None
        for fmt in date_formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                break
            except ValueError:
                continue
        
        if parsed_date is None:
            raise ValueError(f"Could not parse date: {date_str}")
        
        print(f"[INFO] Reference date read: {col_name} = {parsed_date.strftime('%d/%m/%Y')}")
        return col_name, parsed_date
        
    except Exception as e:
        print(f"[ERROR] Failed to read reference file: {e}")
        sys.exit(1)


def get_start_date_from_last_column(df: pl.DataFrame, date_reference: datetime) -> datetime:
    """
    Determine the start date based on the last relative day column.
    
    For ORCA (columns 90-1): column 90 = reference date - 1
    For SANPLAT (columns 91-1): column 91 = reference date - 1
    """
    # Find the highest numeric column name (ignoring non-numeric columns)
    numeric_cols = []
    for col in df.columns:
        try:
            numeric_cols.append(int(col))
        except ValueError:
            pass
    
    if not numeric_cols:
        raise ValueError("No numeric columns found in data file")
    
    max_col = max(numeric_cols)
    print(f"[INFO] Highest relative day column: {max_col}")
    
    # If max column is 90 (ORCA) or 91 (SANPLAT), it represents reference date - 1
    # So the start date for column 90/91 is reference date - 1
    start_date = date_reference - timedelta(days=1)
    print(f"[INFO] Start date (for column {max_col}): {start_date.strftime('%d/%m/%Y')}")
    
    return start_date


def build_column_rename_map(df: pl.DataFrame, start_date: datetime) -> dict:
    """
    Build a rename mapping from relative day columns to absolute dates.

    Maps relative day columns (90-1) to actual date strings (DD/MM/YYYY).
    Non-numeric columns are preserved as-is.
    """
    max_relative = max(int(c) for c in df.columns if c.isdigit())
    rename_map: dict = {}

    for col in df.columns:
        try:
            relative_day = int(col)
            days_offset = max_relative - relative_day
            actual_date = start_date - timedelta(days=days_offset)
            rename_map[col] = actual_date.strftime("%d/%m/%Y")
        except ValueError:
            # Non-numeric column — keep original name
            pass

    return rename_map


def process_orca(
    data_file: Path,
    ref_file: Path,
    output_dir: Path
) -> Path:
    """Process ORCA parquet file and add date header."""
    print(f"\n[INFO] Processing ORCA file: {data_file.name}")
    
    # Read reference date
    _, ref_date = read_reference_date(ref_file)
    
    # Load parquet file
    df = pl.scan_parquet(str(data_file)).collect()
    print(f"[INFO] Loaded parquet: {len(df)} rows, {len(df.columns)} columns")
    
    # Get start date
    start_date = get_start_date_from_last_column(df, ref_date)

    # Rename relative columns to absolute dates
    rename_map = build_column_rename_map(df, start_date)
    enriched_df = df.rename(rename_map)

    # Save to trusted directory as CSV
    output_file = output_dir / f"{data_file.stem}_com_datas.csv"
    enriched_df.write_csv(str(output_file))

    print(f"[SUCCESS] Enriched file saved: {output_file}")
    return output_file


def process_sanplat(
    data_file: Path,
    ref_file: Path,
    output_dir: Path
) -> Path:
    """Process SANPLAT CSV file and add date header."""
    print(f"\n[INFO] Processing SANPLAT file: {data_file.name}")
    
    # Read reference date
    _, ref_date = read_reference_date(ref_file)
    
    # Load CSV file
    df = pl.scan_csv(str(data_file)).collect()
    print(f"[INFO] Loaded CSV: {len(df)} rows, {len(df.columns)} columns")
    
    # Get start date
    start_date = get_start_date_from_last_column(df, ref_date)

    # Rename relative columns to absolute dates
    rename_map = build_column_rename_map(df, start_date)
    enriched_df = df.rename(rename_map)

    # Save to trusted directory
    output_file = output_dir / f"{data_file.stem}_com_datas.csv"
    enriched_df.write_csv(str(output_file))

    print(f"[SUCCESS] Enriched file saved: {output_file}")
    return output_file


def enrich_orca():
    """Enrich ORCA data with reference dates."""
    data_file = Path("data/raw/ORCA/Dados_Comunicacao.parquet")
    ref_file = Path("data/trusted/CIS/Data_Referencia.csv")
    output_dir = Path("data/trusted/ORCA")
    
    # Create output directory if needed
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if not data_file.exists():
        print(f"[ERROR] Data file not found: {data_file}")
        return False
    
    if not ref_file.exists():
        print(f"[ERROR] Reference file not found: {ref_file}")
        return False
    
    try:
        process_orca(data_file, ref_file, output_dir)
        return True
    except Exception as e:
        print(f"[ERROR] Failed to process ORCA: {e}")
        import traceback
        traceback.print_exc()
        return False


def enrich_sanplat():
    """Enrich SANPLAT data with reference dates."""
    data_file = Path("data/refined/SANPLAT/Dados_Comunicacao_SANPLAT.csv")
    ref_file = Path("data/trusted/SANPLAT/Data_Referencia_2.csv")
    output_dir = Path("data/trusted/SANPLAT")
    
    # Create output directory if needed
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if not data_file.exists():
        print(f"[ERROR] Data file not found: {data_file}")
        return False
    
    if not ref_file.exists():
        print(f"[ERROR] Reference file not found: {ref_file}")
        return False
    
    try:
        process_sanplat(data_file, ref_file, output_dir)
        return True
    except Exception as e:
        print(f"[ERROR] Failed to process SANPLAT: {e}")
        import traceback
        traceback.print_exc()
        return False


def _build_parser() -> argparse.ArgumentParser:
    """Build the argument parser."""
    parser = argparse.ArgumentParser(
        description="Enrich communication data files with date references"
    )
    parser.add_argument(
        "--orca",
        action="store_true",
        help="Process ORCA data"
    )
    parser.add_argument(
        "--sanplat",
        action="store_true",
        help="Process SANPLAT data"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Process both ORCA and SANPLAT data"
    )
    parser.add_argument(
        "--data-file",
        type=str,
        help="Custom data file path"
    )
    parser.add_argument(
        "--ref-file",
        type=str,
        help="Custom reference file path"
    )
    return parser


def _handle_custom_files(args: argparse.Namespace) -> None:
    """Process custom data and reference files."""
    data_file = Path(args.data_file)
    ref_file = Path(args.ref_file)
    output_dir = data_file.parent.parent / "trusted" / data_file.parent.name
    output_dir.mkdir(parents=True, exist_ok=True)

    if data_file.suffix.lower() == ".parquet":
        process_orca(data_file, ref_file, output_dir)
    else:
        process_sanplat(data_file, ref_file, output_dir)


def _handle_process_all() -> None:
    """Process both ORCA and SANPLAT data."""
    separator = "=" * 60
    print(f"\n{separator}")
    print("Enriching ORCA and SANPLAT communication data")
    print(separator)

    orca_ok = enrich_orca()
    sanplat_ok = enrich_sanplat()

    if orca_ok and sanplat_ok:
        print(f"\n{separator}")
        print("[SUCCESS] All files processed successfully!")
        print(separator)
    else:
        print("\n[WARNING] Some files failed to process")


def main():
    """Main entry point."""
    args = _build_parser().parse_args()

    if args.data_file and args.ref_file:
        _handle_custom_files(args)
        return

    if args.all or (not args.orca and not args.sanplat):
        _handle_process_all()
    else:
        if args.orca:
            enrich_orca()
        if args.sanplat:
            enrich_sanplat()


if __name__ == "__main__":
    main()
