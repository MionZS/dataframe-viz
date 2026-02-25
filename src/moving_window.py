"""Moving window OR logic for communication availability.

For each target date D the availability flag is:

    DISP(D) = max( col[D-5], col[D-4], col[D-3], col[D-2], col[D-1] )

Because the source data is binary (0/1) after binarisation, `max` over
the window is equivalent to a logical OR: if *any* of the preceding 5
days has a non-zero value the meter is considered "communicating".

The enriched CSV uses absolute date strings (DD/MM/YYYY) as column names.
Non-date columns (e.g. NIO) keep their original names.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import polars as pl

logger = logging.getLogger(__name__)

# Date formats used in the secondary header row
_DATE_FORMATS = ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d")


def _parse_date(s: str) -> Optional[datetime]:
    """Try multiple date formats; return None on failure."""
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _build_col_date_map(columns: List[str]) -> Dict[str, datetime]:
    """Return {column_name: parsed_date} for every date-named column."""
    mapping: Dict[str, datetime] = {}
    for col in columns:
        parsed = _parse_date(col)
        if parsed is not None:
            mapping[col] = parsed
    return mapping


def _build_col_date_map_from_row(header_row: pl.DataFrame) -> Dict[str, datetime]:
    """Return {column_name: parsed_date} by parsing the first-row values.

    This supports older enriched files that kept relative column names
    ("1","2",...) and included a secondary date row as the first
    data row.
    """
    mapping: Dict[str, datetime] = {}
    for col in header_row.columns:
        # Only consider numeric column names as date-mapped
        if not col.isdigit():
            continue
        val = header_row[col][0]
        if isinstance(val, str):
            parsed = _parse_date(val)
            if parsed is not None:
                mapping[col] = parsed
        elif isinstance(val, datetime):
            mapping[col] = val
    return mapping


def _resolve_window_columns(
    col_date_map: Dict[str, datetime],
    target_date: datetime,
    window_days: int,
) -> List[str]:
    """Return column names whose dates fall in [D - window_days, D - 1]."""
    start = target_date - timedelta(days=window_days)
    end = target_date - timedelta(days=1)
    return [col for col, d in col_date_map.items() if start <= d <= end]


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------


def compute_disp(
    enriched_path: str,
    target_date: datetime,
    window_days: int = 5,
    binarize: bool = False,
    nio_col: str = "NIO",
) -> pl.DataFrame:
    """Compute DISP(target_date) from an enriched CSV.

    Parameters
    ----------
    enriched_path:
        Path to the enriched CSV (with secondary date-header row).
    target_date:
        The date D for which to compute availability.
    window_days:
        Number of preceding days to examine (default 5).
    binarize:
        If True, convert all non-zero values to 1 before applying
        the window (required for ORCA).
    nio_col:
        Name of the meter-identifier column.

    Returns
    -------
    pl.DataFrame with columns [NIO, DISP] where DISP is 0 or 1.
    """
    # 1. Read column names to build column→date map (new format)
    schema_cols = pl.read_csv(enriched_path, n_rows=0).columns
    col_date_map = _build_col_date_map(schema_cols)

    # Fallback for legacy files that kept numeric column names and
    # inserted a secondary date header as the first data row.
    if not col_date_map:
        header_row = pl.read_csv(enriched_path, n_rows=1)
        col_date_map = _build_col_date_map_from_row(header_row)

    if not col_date_map:
        raise ValueError(f"No date columns found in {enriched_path}")

    # 2. Find columns inside the window
    window_cols = _resolve_window_columns(col_date_map, target_date, window_days)
    if not window_cols:
        _fmt = _DATE_FORMATS[0]
        logger.warning(
            "No columns in window [%s .. %s] for target %s",
            (target_date - timedelta(days=window_days)).strftime(_fmt),
            (target_date - timedelta(days=1)).strftime(_fmt),
            target_date.strftime(_fmt),
        )
        # Return an empty frame so the caller can skip gracefully
        return pl.DataFrame({nio_col: [], "DISP": []}).cast(
            {nio_col: pl.Utf8, "DISP": pl.Int8}
        )

    logger.info(
        "Moving window for %s: using %d columns %s",
        target_date.strftime("%Y-%m-%d"),
        len(window_cols),
        window_cols,
    )

    # 3. Read only NIO + window columns
    all_cols = [nio_col] + window_cols
    df = pl.read_csv(enriched_path, columns=all_cols)

    # 4. Binarize if needed (ORCA has 0/0.25/0.5/0.75/1)
    if binarize:
        for c in window_cols:
            # Coerce to string, replace comma with dot (European decimals),
            # cast to float and consider any value > 0 as 1.
            num_expr = pl.col(c).cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64)
            df = df.with_columns(
                pl.when(num_expr > 0).then(1).otherwise(0).alias(c)
            )

    # 5. DISP = max across window columns (binary OR)
    disp_expr = pl.max_horizontal(*[pl.col(c) for c in window_cols])
    result = df.select(
        pl.col(nio_col),
        disp_expr.cast(pl.Int8).alias("DISP"),
    )

    logger.info(
        "DISP computed: %d meters, %d communicating",
        result.height,
        result.filter(pl.col("DISP") == 1).height,
    )
    return result
