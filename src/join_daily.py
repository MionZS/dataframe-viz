"""Join communication availability with the daily Diario file.

For a given date D, loads the Diario parquet (Diario_YYYY-MM-DD.parquet),
selects [NIO, MUNICIPIO], and performs a LEFT JOIN from Diario onto the
DISP frame produced by moving_window.compute_disp().

The Diario is the source of truth for the meter universe: every meter in
the Diario appears in the output.  Meters without a match in DISP default
to DISP=0 (not communicating).
"""

import logging
from datetime import datetime
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)


def _diario_path(diario_dir: str, target_date: datetime) -> Path:
    """Build the path for the Diario parquet of a given date."""
    filename = f"Diario_{target_date.strftime('%Y-%m-%d')}.parquet"
    return Path(diario_dir) / filename


def join_with_diario(
    disp_df: pl.DataFrame,
    target_date: datetime,
    diario_dir: str,
    origem: str,
    nio_col: str = "NIO",
    municipio_col: str = "MUNICIPIO",
) -> pl.DataFrame:
    """Left-join Diario with DISP frame to add MUNICIPIO.

    The Diario is the left side (source of truth for the meter universe).
    Meters not found in disp_df receive DISP=0.

    Parameters
    ----------
    disp_df:
        DataFrame with [NIO, DISP] from moving_window.compute_disp().
    target_date:
        Date D (used to locate the correct Diario file).
    diario_dir:
        Directory containing Diario_YYYY-MM-DD.parquet files.
    origem:
        Source tag to add ("ORCA" or "SANPLAT").
    nio_col / municipio_col:
        Column names in the Diario file.

    Returns
    -------
    DataFrame [NIO, MUNICIPIO, ORIGEM, DISP] after LEFT JOIN from Diario.
    Returns empty frame (correct schema) if Diario file is missing.
    """
    path = _diario_path(diario_dir, target_date)

    if not path.exists():
        logger.warning("Diario not found for %s: %s — skipping", target_date.strftime("%Y-%m-%d"), path)
        return _empty_result(nio_col, municipio_col)

    logger.info("Loading Diario: %s", path)
    diario = pl.scan_parquet(str(path)).select([nio_col, municipio_col]).collect()

    # Normalize NIO: cast to string and strip leading zeros so that
    # "0043138963" and "43138963" match.  Non-numeric NIOs pass through.
    def _normalize_nio(df: pl.DataFrame, col: str) -> pl.DataFrame:
        if col not in df.columns:
            return df
        return df.with_columns(
            pl.col(col).cast(pl.Utf8)
            .str.replace(r"^0+(.)", r"$1")
            .alias(col)
        )

    disp_norm = _normalize_nio(disp_df, nio_col)
    diario_norm = _normalize_nio(diario, nio_col)

    # Diario is the left side: keeps ALL meters from the universe.
    # Meters not found in DISP get null → fill with 0.
    joined = diario_norm.join(disp_norm, on=nio_col, how="left")
    joined = joined.with_columns(
        pl.col("DISP").fill_null(0).cast(pl.Int8),
        pl.lit(origem).alias("ORIGEM"),
    )

    logger.info(
        "Join result for %s [%s]: %d rows (from %d DISP × %d Diario)",
        target_date.strftime("%Y-%m-%d"),
        origem,
        joined.height,
        disp_df.height,
        diario.height,
    )

    # Free diario eagerly
    del diario, diario_norm, disp_norm

    return joined


def _empty_result(nio_col: str, municipio_col: str) -> pl.DataFrame:
    """Return an empty DataFrame with the expected post-join schema."""
    return pl.DataFrame({
        nio_col: [],
        municipio_col: [],
        "ORIGEM": [],
        "DISP": [],
    }).cast({
        nio_col: pl.Utf8,
        municipio_col: pl.Utf8,
        "ORIGEM": pl.Utf8,
        "DISP": pl.Int8,
    })
