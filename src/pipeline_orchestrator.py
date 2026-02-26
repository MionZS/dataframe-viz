"""Pipeline orchestrator for Smart Meter Communication Consolidation.

Reads config/pipeline.yaml and executes the full pipeline:

    Phase 1 — Enrichment:
        1a. Enrich ORCA raw data with absolute date column headers
        1b. Enrich SANPLAT refined data with absolute date column headers

    Phase 2 — Daily processing (for each date D in the target month):
        2a. Compute DISP(D) via moving window (ORCA + SANPLAT)
        2b. Join with Diario to add MUNICIPIO
        2c. Group by [MUNICIPIO, ORIGEM]
        2d. Sink to single stream file via SinkManager

See docs/prd/PRD_smart_meter_pipeline_v1.0.0.md for specification.
"""

import logging
import sys
import time
from calendar import monthrange
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

import polars as pl
import yaml

from src.enrich_dates import process_orca, process_sanplat
from src.join_daily import join_with_diario
from src.memory_monitor import MemoryMonitor
from src.moving_window import compute_disp
from src.sink_manager import SinkManager

logger = logging.getLogger(__name__)

DEFAULT_CONFIG = "config/pipeline.yaml"


# ------------------------------------------------------------------
# Config loader
# ------------------------------------------------------------------


def load_config(config_path: str = DEFAULT_CONFIG) -> Dict[str, Any]:
    """Load and validate pipeline YAML config."""
    path = Path(config_path)
    if not path.exists():
        logger.error("Config not found: %s", path)
        sys.exit(1)

    with open(path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    # Validate required keys
    required = ["target_month", "paths"]
    for key in required:
        if key not in cfg:
            logger.error("Missing required config key: %s", key)
            sys.exit(1)

    return cfg


# ------------------------------------------------------------------
# Enrichment phase
# ------------------------------------------------------------------


def _run_enrichment(cfg: Dict[str, Any]) -> None:
    """Phase 1: enrich raw/refined data with absolute date headers.

    Uses ``process_orca`` and ``process_sanplat`` from *enrich_dates*
    to regenerate the enriched CSV files that the moving-window step
    reads from.  Paths are taken from the pipeline config.
    """
    paths = cfg["paths"]
    t0 = time.perf_counter()
    logger.info("=" * 60)
    logger.info("Phase 1: Enrichment — generating date-headed CSVs")

    # --- ORCA ---
    orca_raw = Path(paths["orca_raw"])
    orca_ref = Path(paths["orca_ref_date"])
    orca_out_dir = Path(paths["orca_enriched"]).parent

    if orca_raw.exists() and orca_ref.exists():
        logger.info("Enriching ORCA: %s", orca_raw)
        orca_out_dir.mkdir(parents=True, exist_ok=True)
        orca_enriched = process_orca(orca_raw, orca_ref, orca_out_dir)
        logger.info("[OK] ORCA enriched → %s", orca_enriched)
    else:
        logger.warning(
            "Skipping ORCA enrichment — missing file(s): raw=%s ref=%s",
            orca_raw.exists(),
            orca_ref.exists(),
        )

    # --- SANPLAT ---
    sanplat_raw = Path(paths["sanplat_refined"])
    sanplat_ref = Path(paths["sanplat_ref_date"])
    sanplat_out_dir = Path(paths["sanplat_enriched"]).parent

    if sanplat_raw.exists() and sanplat_ref.exists():
        logger.info("Enriching SANPLAT: %s", sanplat_raw)
        sanplat_out_dir.mkdir(parents=True, exist_ok=True)
        sanplat_enriched = process_sanplat(sanplat_raw, sanplat_ref, sanplat_out_dir)
        logger.info("[OK] SANPLAT enriched → %s", sanplat_enriched)
    else:
        logger.warning(
            "Skipping SANPLAT enrichment — missing file(s): raw=%s ref=%s",
            sanplat_raw.exists(),
            sanplat_ref.exists(),
        )

    elapsed = time.perf_counter() - t0
    logger.info("Enrichment phase complete in %.1fs", elapsed)


# ------------------------------------------------------------------
# Date range builder
# ------------------------------------------------------------------


def build_date_range(target_month: str, window_days: int = 5) -> List[datetime]:
    """Build the list of dates to process.

    Returns dates from (first_of_month - window_days) through end of month.
    The first `window_days` dates are needed so the moving window has
    look-back data for the first day of the month.
    """
    year, month = map(int, target_month.split("-"))
    first_day = datetime(year, month, 1)
    _, last_day_num = monthrange(year, month)
    last_day = datetime(year, month, last_day_num)

    start = first_day - timedelta(days=window_days)
    dates: List[datetime] = []
    current = start
    while current <= last_day:
        dates.append(current)
        current += timedelta(days=1)

    logger.info(
        "Date range: %s -> %s (%d dates)",
        dates[0].strftime("%Y-%m-%d"),
        dates[-1].strftime("%Y-%m-%d"),
        len(dates),
    )
    return dates


# ------------------------------------------------------------------
# Aggregation
# ------------------------------------------------------------------


def aggregate(joined_df: pl.DataFrame, target_date: datetime) -> pl.DataFrame:
    """Group by [MUNICIPIO, ORIGEM] and compute counts.

    Returns DataFrame [MUNICIPIO, ORIGEM, CONTAGEM_COMM, CONTAGEM_TOT, DATA].
    """
    if joined_df.is_empty():
        return pl.DataFrame({
            "MUNICIPIO": [],
            "ORIGEM": [],
            "CONTAGEM_COMM": [],
            "CONTAGEM_TOT": [],
            "DATA": [],
        }).cast({
            "MUNICIPIO": pl.Utf8,
            "ORIGEM": pl.Utf8,
            "CONTAGEM_COMM": pl.Int64,
            "CONTAGEM_TOT": pl.Int64,
            "DATA": pl.Utf8,
        })

    agg = joined_df.group_by(["MUNICIPIO", "ORIGEM"]).agg(
        pl.col("DISP").sum().alias("CONTAGEM_COMM"),
        pl.col("NIO").count().alias("CONTAGEM_TOT"),
    )

    agg = agg.with_columns(
        pl.lit(target_date.strftime("%Y-%m-%d")).alias("DATA"),
    )

    logger.info(
        "Aggregated %s: %d groups, total_comm=%d, total_meters=%d",
        target_date.strftime("%Y-%m-%d"),
        agg.height,
        agg["CONTAGEM_COMM"].sum(),
        agg["CONTAGEM_TOT"].sum(),
    )
    return agg


# ------------------------------------------------------------------
# Process one date
# ------------------------------------------------------------------


def _process_date(
    target_date: datetime,
    cfg: Dict[str, Any],
    sink: SinkManager,
    memory: MemoryMonitor,
) -> None:
    """Process a single date: window → join → aggregate → sink."""
    paths = cfg["paths"]
    window_days: int = cfg.get("moving_window_days", 5)
    diario_dir: str = paths["diario_dir"]
    binarize_orca: bool = cfg.get("orca_binarize", True)

    date_str = target_date.strftime("%Y-%m-%d")
    logger.info("=" * 60)
    logger.info("Processing date: %s", date_str)

    memory.log_status(f"start {date_str}")

    # --- ORCA ---
    orca_disp = _safe_compute_disp(
        paths["orca_enriched"], target_date, window_days, binarize_orca, "ORCA"
    )

    # --- SANPLAT ---
    sanplat_disp = _safe_compute_disp(
        paths["sanplat_enriched"], target_date, window_days, False, "SANPLAT"
    )

    # --- Join with Diario ---
    orca_joined = _safe_join(orca_disp, target_date, diario_dir, "ORCA")
    sanplat_joined = _safe_join(sanplat_disp, target_date, diario_dir, "SANPLAT")

    # Free DISP frames
    del orca_disp, sanplat_disp

    # --- Concat ---
    joined = pl.concat([orca_joined, sanplat_joined])
    del orca_joined, sanplat_joined

    if joined.is_empty():
        logger.warning("No data after join for %s — skipping", date_str)
        return

    # --- Aggregate ---
    agg = aggregate(joined, target_date)
    del joined

    # --- Sink (appends to the single stream file) ---
    sink.submit(agg)
    del agg

    memory.log_status(f"end {date_str}")


def _safe_compute_disp(
    enriched_path: str,
    target_date: datetime,
    window_days: int,
    binarize: bool,
    label: str,
) -> pl.DataFrame:
    """Compute DISP with error handling."""
    try:
        return compute_disp(
            enriched_path=enriched_path,
            target_date=target_date,
            window_days=window_days,
            binarize=binarize,
        )
    except Exception as exc:
        logger.error("Failed to compute DISP [%s] for %s: %s", label, target_date, exc)
        return pl.DataFrame({"NIO": [], "DISP": []}).cast(
            {"NIO": pl.Utf8, "DISP": pl.Int8}
        )


def _safe_join(
    disp_df: pl.DataFrame,
    target_date: datetime,
    diario_dir: str,
    origem: str,
) -> pl.DataFrame:
    """Join with error handling."""
    try:
        return join_with_diario(disp_df, target_date, diario_dir, origem)
    except Exception as exc:
        logger.error("Failed to join [%s] for %s: %s", origem, target_date, exc)
        return pl.DataFrame({
            "NIO": [], "MUNICIPIO": [], "ORIGEM": [], "DISP": [],
        }).cast({
            "NIO": pl.Utf8, "MUNICIPIO": pl.Utf8, "ORIGEM": pl.Utf8, "DISP": pl.Int8,
        })


# ------------------------------------------------------------------
# Main entry point
# ------------------------------------------------------------------


def run(config_path: str = DEFAULT_CONFIG) -> None:
    """Run the full pipeline."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    logger.info("Pipeline starting — config: %s", config_path)
    t0 = time.perf_counter()

    cfg = load_config(config_path)

    # --- Phase 1: Enrichment ---
    _run_enrichment(cfg)

    # --- Phase 2: Daily processing ---
    memory = MemoryMonitor(
        threshold_percent=cfg.get("memory_threshold_percent", 70)
    )
    sink = SinkManager(
        max_concurrent=cfg.get("sink_queue_limit", 3),
        output_format=cfg.get("output_format", "parquet"),
        retry_attempts=cfg.get("sink_retry_attempts", 3),
        memory_monitor=memory,
    )

    # Ensure output dirs exist
    for dir_key in ("mixed_output", "municipio_daily_output"):
        Path(cfg["paths"][dir_key]).mkdir(parents=True, exist_ok=True)

    dates = build_date_range(
        cfg["target_month"],
        window_days=cfg.get("moving_window_days", 5),
    )

    # --- Open single stream file for all dates ---
    output_dir = Path(cfg["paths"]["municipio_daily_output"])
    target_month = cfg["target_month"]
    csv_stream_path = output_dir / f"municipio_{target_month}.csv"
    sink.open_stream(csv_stream_path)

    success = 0
    failures = 0
    for d in dates:
        try:
            _process_date(d, cfg, sink, memory)
            success += 1
        except MemoryError:
            logger.critical("Aborting pipeline — memory threshold exceeded")
            break
        except Exception as exc:
            logger.error("Date %s failed: %s", d.strftime("%Y-%m-%d"), exc)
            failures += 1

    # --- Close stream and optionally convert to parquet ---
    stream_csv = sink.close_stream()
    fmt = cfg.get("output_format", "parquet")
    if fmt == "parquet" and stream_csv is not None and stream_csv.stat().st_size > 0:
        parquet_path = stream_csv.with_suffix(".parquet")
        SinkManager.stream_to_parquet(stream_csv, parquet_path)

    elapsed = time.perf_counter() - t0
    logger.info(
        "Pipeline finished in %.1fs — %d ok, %d failed out of %d dates",
        elapsed,
        success,
        failures,
        len(dates),
    )
    memory.log_status("pipeline_end")


if __name__ == "__main__":
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_CONFIG
    run(cfg_path)
