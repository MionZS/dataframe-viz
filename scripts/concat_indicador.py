#!/usr/bin/env python3
"""Concatenate monthly `municipio` final outputs into a single CSV.

Finds files named `municipio_YYYY-MM.csv` or `municipio_YYYY-MM.parquet`
in `data/trusted/municipio_daily/`, concatenates them and writes
`data/trusted/Indicador_comunicacao.csv`.

Usage:
  python scripts/concat_indicador.py
  python scripts/concat_indicador.py --input-dir data/trusted/municipio_daily --output data/trusted/Indicador_comunicacao.csv --drop-duplicates
"""
from pathlib import Path
import re
import argparse
import logging
from typing import List

import polars as pl

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")


def find_monthly_files(input_dir: Path) -> List[Path]:
    pattern = re.compile(r"^municipio_\d{4}-\d{2}\.(csv|parquet)$", re.IGNORECASE)
    files = [p for p in sorted(input_dir.iterdir()) if pattern.match(p.name)]
    return files


def read_table(p: Path) -> pl.DataFrame:
    if p.suffix.lower() == ".csv":
        return pl.read_csv(p)
    if p.suffix.lower() == ".parquet":
        return pl.read_parquet(p)
    raise ValueError(f"Unsupported file type: {p}")


def main(input_dir: str, output: str, drop_duplicates: bool):
    inp = Path(input_dir)
    out = Path(output)

    if not inp.exists():
        logging.error("Input directory not found: %s", inp)
        return 1

    files = find_monthly_files(inp)
    if not files:
        logging.warning("No monthly municipio files found in %s", inp)
        return 0

    logging.info("Found %d monthly files", len(files))
    dfs: List[pl.DataFrame] = []
    for f in files:
        try:
            logging.info("Reading %s", f.name)
            df = read_table(f)
            dfs.append(df)
        except Exception as e:
            logging.error("Failed to read %s: %s", f, e)

    if not dfs:
        logging.error("No dataframes to concatenate")
        return 1

    combined = pl.concat(dfs, how="vertical")

    if drop_duplicates:
        before = combined.height
        combined = combined.unique()
        logging.info("Dropped duplicates: %d -> %d rows", before, combined.height)

    # Reorder common columns if present
    desired = ["MUNICIPIO", "INTELIGENTE", "CONTAGEM_COMM", "CONTAGEM_TOT", "DISP", "DATA"]
    cols = [c for c in desired if c in combined.columns] + [c for c in combined.columns if c not in desired]
    combined = combined.select(cols)

    # Sort for stable output if DATA exists
    if "DATA" in combined.columns:
        combined = combined.sort(["DATA", "MUNICIPIO", "INTELIGENTE"]) if {"MUNICIPIO","INTELIGENTE"}.issubset(set(combined.columns)) else combined.sort("DATA")

    out.parent.mkdir(parents=True, exist_ok=True)
    combined.write_csv(str(out))
    logging.info("Wrote %s (%d rows)", out, combined.height)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Concatenate monthly municipio outputs into Indicador_comunicacao.csv")
    parser.add_argument("--input-dir", default="data/trusted/municipio_daily", help="Directory with municipio_YYYY-MM.* files")
    parser.add_argument("--output", default="data/trusted/Indicador_comunicacao.csv", help="Output CSV path")
    parser.add_argument("--drop-duplicates", action="store_true", help="Drop duplicate rows across source files")
    args = parser.parse_args()
    raise SystemExit(main(args.input_dir, args.output, args.drop_duplicates))
