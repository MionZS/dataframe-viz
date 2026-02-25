"""Batch export selected date ranges from enriched files.

Config file is a JSON list of objects with keys:
  - input: path to enriched file (CSV or parquet)
  - output: output CSV path
  - from_date: start date (inclusive) in YYYY-MM-DD or DD/MM/YYYY
  - to_date: end date (inclusive)

Example:
[
  {"input": "data/trusted/ORCA/Dados_Comunicacao_com_datas.csv", "output": "data/trusted/ORCA/out.csv", "from_date": "2025-12-27", "to_date": "2026-01-31"}
]
"""
from pathlib import Path
from datetime import datetime, date
import json
import sys
from typing import List, Dict, Optional

import polars as pl

# Helper: parse date string

def parse_date(s: str) -> datetime:
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unrecognized date format: {s}")


def _read_columns(inp: Path) -> List[str]:
    """Return column names from the input file."""
    if inp.suffix.lower() in (".csv", ".txt"):
        return pl.read_csv(str(inp), n_rows=0).columns
    return pl.scan_parquet(str(inp)).collect_schema().names()


def _is_date_column(name: str) -> bool:
    """Check whether a column name is a parseable date string."""
    try:
        parse_date(name)
        return True
    except ValueError:
        return False


def _parse_header_dates(columns: List[str], from_date: datetime, to_date: datetime) -> List[str]:
    """Given column names, return non-date columns + date columns within range."""
    selected_date_cols: List[str] = []
    other_cols: List[str] = []

    for c in columns:
        parsed: Optional[datetime] = None
        try:
            parsed = parse_date(c)
        except ValueError:
            pass

        if parsed is not None:
            if from_date <= parsed <= to_date:
                selected_date_cols.append(c)
        else:
            other_cols.append(c)

    return other_cols + selected_date_cols


def process_task(task: Dict) -> None:
    inp = Path(task["input"])
    out = Path(task["output"])
    from_date = parse_date(task["from_date"]) if isinstance(task.get("from_date"), str) else None
    to_date = parse_date(task["to_date"]) if isinstance(task.get("to_date"), str) else None

    if not inp.exists():
        print(f"[ERROR] Input not found: {inp}")
        return
    if from_date is None or to_date is None:
        print("[ERROR] from_date/to_date must be provided in task")
        return

    print(f"Processing: {inp} -> {out}")

    # Ensure output filename contains provider tag (e.g., _ORCA or _SANPLAT)
    def _detect_provider_tag(p: Path) -> str:
        parts_upper = [part.upper() for part in p.parts]
        for candidate in ("ORCA", "SANPLAT"):
            if candidate in parts_upper:
                return candidate
        # fallback to immediate parent folder name
        return p.parent.name.upper()

    def _ensure_output_has_tag(path: Path, tag: str) -> Path:
        stem = path.stem
        if stem.upper().endswith(f"_{tag}"):
            return path
        new_name = f"{stem}_{tag}{path.suffix}"
        return path.with_name(new_name)

    provider_tag = _detect_provider_tag(inp)
    out = _ensure_output_has_tag(out, provider_tag)
    print(f"Adjusted output path: {out}")
    try:
        columns = _read_columns(inp)
    except Exception as e:
        print(f"[ERROR] Failed to read columns: {e}")
        return

    select_cols = _parse_header_dates(columns, from_date, to_date)
    if not select_cols:
        print("[WARN] No columns selected for this task, skipping")
        return

    n_date = sum(1 for c in select_cols if _is_date_column(c))
    print(f"Selecting {len(select_cols)} columns ({len(select_cols) - n_date} non-date + {n_date} date cols)")

    try:
        if inp.suffix.lower() in (".csv", ".txt"):
            lf = pl.scan_csv(str(inp), ignore_errors=True)
        else:
            lf = pl.scan_parquet(str(inp))
        lf.select(select_cols).sink_csv(str(out))
        print(f"[OK] Wrote {out}")
    except Exception as e:
        print(f"[ERROR] Failed to export: {e}")


def main(config_path: str):
    cfg = Path(config_path)
    if not cfg.exists():
        print(f"Config not found: {cfg}")
        sys.exit(1)
    try:
        tasks = json.loads(cfg.read_text())
    except Exception as e:
        print(f"Failed to load JSON config: {e}")
        sys.exit(1)

    if not isinstance(tasks, list):
        print("Config must be a list of task objects")
        sys.exit(1)

    for task in tasks:
        try:
            process_task(task)
        except Exception as e:
            print(f"Task failed: {e}")


if __name__ == "__main__":
    cfg = sys.argv[1] if len(sys.argv) > 1 else "scripts/export_config.json"
    main(cfg)
