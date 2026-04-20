"""Single-day DISP extraction (no moving window).

Reads enriched CSV with absolute date columns and extracts DISP
directly from the previous day's column for the target date.
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import polars as pl

logger = logging.getLogger(__name__)

# Date format used in enriched CSV column names
_DATE_FORMAT = "%d/%m/%Y"


def compute_disp_single_day(
    enriched_path: str, target_date: datetime, nio_col: str = "NIO"
) -> pl.DataFrame:
    """Extract DISP from previous day column only (no moving window).

    For target_date, reads the column corresponding to target_date - 1
    and uses its values directly as DISP.

    Parameters
    ----------
    enriched_path:
        Path to the enriched CSV (with absolute date column names).
    target_date:
        The date D — we extract data from D-1 (previous day).
    nio_col:
        Name of the meter-identifier column.

    Returns
    -------
    pl.DataFrame with columns [NIO, DISP] where DISP is 0 or 1.
    """
    # Determine column to read: target_date - 1
    prev_date = target_date - timedelta(days=1)
    target_col = prev_date.strftime(_DATE_FORMAT)

    # Read only NIO + target column
    try:
        df = pl.read_csv(enriched_path, columns=[nio_col, target_col])
    except Exception as exc:
        logger.warning(
            "Column %s not found in %s: %s", target_col, enriched_path, exc
        )
        return pl.DataFrame({nio_col: [], "DISP": []}).cast(
            {nio_col: pl.Utf8, "DISP": pl.Int8}
        )

    # Convert target column to numeric, coerce any non-zero to 1
    disp_col_expr = (
        pl.col(target_col).cast(pl.Utf8).str.replace(",", ".").cast(pl.Float64)
    )
    result = df.select(
        pl.col(nio_col),
        pl.when(disp_col_expr > 0).then(1).otherwise(0).cast(pl.Int8).alias("DISP"),
    )

    logger.info(
        "DISP extracted from %s: %d meters, %d communicating",
        target_col,
        result.height,
        result.filter(pl.col("DISP") == 1).height,
    )
    return result
