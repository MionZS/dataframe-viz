#!/usr/bin/env python3
"""Concatenate monthly `municipio` final outputs into a single CSV.

Supports two variants:
- full:   `municipio_YYYY-MM.csv|parquet`
- simple: `municipio_YYYY-MM_1day.csv|parquet`

For each month, selects at most one source file (prefers parquet over csv)
to avoid duplicated rows when both formats exist.

Usage:
  python scripts/concat_indicador.py
    python scripts/concat_indicador.py --variant full
    python scripts/concat_indicador.py --variant simple --output data/trusted/Indicador_comunicacao_simple.csv
"""
from pathlib import Path
import re
import argparse
import logging
from typing import Dict, List

import polars as pl

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

_PARQUET_SUFFIX = ".parquet"


def find_monthly_files(input_dir: Path, variant: str = "full") -> List[Path]:
    if variant == "simple":
        pattern = re.compile(r"^municipio_(\d{4}-\d{2})_1day\.(csv|parquet)$", re.IGNORECASE)
    else:
        pattern = re.compile(r"^municipio_(\d{4}-\d{2})\.(csv|parquet)$", re.IGNORECASE)

    # Keep one file per month, preferring parquet if both exist.
    by_month: Dict[str, Path] = {}
    for p in sorted(input_dir.iterdir()):
        m = pattern.match(p.name)
        if not m:
            continue

        month = m.group(1)
        chosen = by_month.get(month)
        if chosen is None:
            by_month[month] = p
            continue

        # Prefer parquet over csv.
        if chosen.suffix.lower() != _PARQUET_SUFFIX and p.suffix.lower() == _PARQUET_SUFFIX:
            by_month[month] = p

    return [by_month[m] for m in sorted(by_month.keys())]


def read_table(p: Path) -> pl.DataFrame:
    if p.suffix.lower() == ".csv":
        return pl.read_csv(p)
    if p.suffix.lower() == _PARQUET_SUFFIX:
        return pl.read_parquet(p)
    raise ValueError(f"Unsupported file type: {p}")


def main(input_dir: str, output: str, drop_duplicates: bool, variant: str = "full"):
    inp = Path(input_dir)
    out = Path(output)

    if not inp.exists():
        logging.error("Input directory not found: %s", inp)
        return 1

    files = find_monthly_files(inp, variant=variant)
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
    parser.add_argument("--variant", choices=["simple", "full"], default="full", help="Pipeline variant to concatenate")
    parser.add_argument("--drop-duplicates", action="store_true", help="Drop duplicate rows across source files")
    args = parser.parse_args()
    raise SystemExit(main(args.input_dir, args.output, args.drop_duplicates, args.variant))
