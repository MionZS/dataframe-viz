"""Simplified pipeline orchestrator — Phase 1 + 3 only (single-day lookback, no moving window).

Reads config/pipeline.yaml and executes two phases:

    Phase 1 — Enrichment  (intermediate deliverables)
        Rename relative date columns to absolute dates.
        Output: trusted/ORCA/…_com_datas.csv, trusted/SANPLAT/…_com_datas.csv

    Phase 3 — Join & Aggregate  (final deliverable)
        For each day:
          - Extract DISP from the previous day column (target_date - 1) only
          - No moving window; direct single-day lookback
          - Merge ORCA+SANPLAT DISP by NIO
          - Left-join once with Diario (MUNICIPIO)
          - Inner-join with MEDIDORES (smart brands only)
          - Aggregate by [MUNICIPIO, INTELIGENTE]
          - DISP = CONTAGEM_COMM / CONTAGEM_TOT
          - Stream-sink into a single output file
        Output: municipio_daily/municipio_YYYY-MM.csv (.parquet)

This uses single-day (yesterday) lookback instead of 5-day moving window.
"""

import logging
import sys
import time
from calendar import monthrange
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import polars as pl
import yaml

from src.daily_disp import compute_disp_single_day
from src.enrich_dates import process_orca, process_sanplat
from src.join_daily import join_with_diario, join_with_medidores
from src.memory_monitor import MemoryMonitor
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


def _run_enrichment(cfg: Dict[str, Any]) -> Tuple[Optional[Path], Optional[Path]]:
    """Phase 1: enrich raw/refined data with absolute date headers.

    Outputs to separate _1day directories to avoid conflicts with the
    standard pipeline. Returns paths to enriched files in:
      trusted_1day/ORCA/…_com_datas.csv
      trusted_1day/SANPLAT/…_com_datas.csv
    """
    paths = cfg["paths"]
    t0 = time.perf_counter()
    logger.info("=" * 60)
    logger.info("Phase 1: Enrichment — generating date-headed CSVs (to _1day dirs)")

    orca_enriched_1day = None
    sanplat_enriched_1day = None

    # --- ORCA (to trusted_1day/) ---
    orca_raw = Path(paths["orca_raw"])
    orca_ref = Path(paths["orca_ref_date"])
    orca_out_dir_1day = Path(paths["orca_enriched"]).parent.parent / "trusted_1day" / "ORCA"

    if orca_raw.exists() and orca_ref.exists():
        logger.info("Enriching ORCA: %s → %s", orca_raw, orca_out_dir_1day)
        orca_out_dir_1day.mkdir(parents=True, exist_ok=True)
        orca_enriched_1day = process_orca(orca_raw, orca_ref, orca_out_dir_1day)
        logger.info("[OK] ORCA enriched → %s", orca_enriched_1day)
    else:
        logger.warning(
            "Skipping ORCA enrichment — missing file(s): raw=%s ref=%s",
            orca_raw.exists(),
            orca_ref.exists(),
        )

    # --- SANPLAT (to trusted_1day/) ---
    sanplat_raw = Path(paths["sanplat_refined"])
    sanplat_ref = Path(paths["sanplat_ref_date"])
    sanplat_out_dir_1day = Path(paths["sanplat_enriched"]).parent.parent / "trusted_1day" / "SANPLAT"

    if sanplat_raw.exists() and sanplat_ref.exists():
        logger.info("Enriching SANPLAT: %s → %s", sanplat_raw, sanplat_out_dir_1day)
        sanplat_out_dir_1day.mkdir(parents=True, exist_ok=True)
        sanplat_enriched_1day = process_sanplat(sanplat_raw, sanplat_ref, sanplat_out_dir_1day)
        logger.info("[OK] SANPLAT enriched → %s", sanplat_enriched_1day)
    else:
        logger.warning(
            "Skipping SANPLAT enrichment — missing file(s): raw=%s ref=%s",
            sanplat_raw.exists(),
            sanplat_ref.exists(),
        )

    elapsed = time.perf_counter() - t0
    logger.info("Enrichment phase complete in %.1fs", elapsed)
    return orca_enriched_1day, sanplat_enriched_1day


# ------------------------------------------------------------------
# Date range builder
# ------------------------------------------------------------------


def build_month_dates(target_month: str) -> List[datetime]:
    """Return only the days that belong to *target_month* (no look-back)."""
    year, month = map(int, target_month.split("-"))
    _, last_day_num = monthrange(year, month)
    return [datetime(year, month, d) for d in range(1, last_day_num + 1)]


# ------------------------------------------------------------------
# Aggregation
# ------------------------------------------------------------------


def aggregate(joined_df: pl.DataFrame, target_date: datetime) -> pl.DataFrame:
    """Group by [MUNICIPIO, INTELIGENTE] and compute counts + availability.

    Returns DataFrame with columns:
        MUNICIPIO, INTELIGENTE, CONTAGEM_COMM, CONTAGEM_TOT, DISP, DATA

    DISP is computed as ``CONTAGEM_COMM / CONTAGEM_TOT`` (rounded to 4
    decimal places). Each municipality can have up to 3 groups — one per
    smart-meter brand (Hexing, Nansen, Nansen Ipiranga).
    """
    if joined_df.is_empty():
        return pl.DataFrame({
            "MUNICIPIO": [],
            "INTELIGENTE": [],
            "CONTAGEM_COMM": [],
            "CONTAGEM_TOT": [],
            "DISP": [],
            "DATA": [],
        }).cast({
            "MUNICIPIO": pl.Utf8,
            "INTELIGENTE": pl.Utf8,
            "CONTAGEM_COMM": pl.Int64,
            "CONTAGEM_TOT": pl.Int64,
            "DISP": pl.Float64,
            "DATA": pl.Utf8,
        })

    agg = joined_df.group_by(["MUNICIPIO", "INTELIGENTE"]).agg(
        pl.col("DISP").sum().alias("CONTAGEM_COMM"),
        pl.col("NIO").count().alias("CONTAGEM_TOT"),
    )

    agg = agg.with_columns(
        (pl.col("CONTAGEM_COMM") / pl.col("CONTAGEM_TOT"))
        .round(4)
        .alias("DISP"),
        pl.lit(target_date.strftime("%Y-%m-%d")).alias("DATA"),
    )

    logger.info(
        "Aggregated %s: %d groups, comm=%d, total=%d",
        target_date.strftime("%Y-%m-%d"),
        agg.height,
        agg["CONTAGEM_COMM"].sum(),
        agg["CONTAGEM_TOT"].sum(),
    )
    return agg


# ------------------------------------------------------------------
# Phase 3 — Join & Aggregate (final deliverable only)
# ------------------------------------------------------------------


def _compute_combined_disp(
    target_date: datetime,
    orca_enriched: Optional[str],
    sanplat_enriched: Optional[str],
) -> pl.DataFrame:
    """Extract DISP from both ORCA and SANPLAT (single-day lookback), then merge by NIO.

    Returns a DataFrame with [NIO, DISP] (combined from both sources).
    """
    orca_disp = pl.DataFrame({"NIO": [], "DISP": []})
    sanplat_disp = pl.DataFrame({"NIO": [], "DISP": []})

    # Extract ORCA DISP from previous day column
    if orca_enriched and Path(orca_enriched).exists():
        try:
            orca_disp = compute_disp_single_day(
                enriched_path=orca_enriched,
                target_date=target_date,
            )
            logger.info("ORCA DISP extracted: %d rows", orca_disp.height)
        except Exception as exc:
            logger.error("ORCA DISP extraction failed: %s", exc)

    # Extract SANPLAT DISP from previous day column
    if sanplat_enriched and Path(sanplat_enriched).exists():
        try:
            sanplat_disp = compute_disp_single_day(
                enriched_path=sanplat_enriched,
                target_date=target_date,
            )
            logger.info("SANPLAT DISP extracted: %d rows", sanplat_disp.height)
        except Exception as exc:
            logger.error("SANPLAT DISP extraction failed: %s", exc)

    # Merge: take union, then unique by NIO (safety for overlap)
    if not orca_disp.is_empty() and not sanplat_disp.is_empty():
        combined = pl.concat([orca_disp, sanplat_disp])
    elif not orca_disp.is_empty():
        combined = orca_disp
    elif not sanplat_disp.is_empty():
        combined = sanplat_disp
    else:
        combined = pl.DataFrame({"NIO": [], "DISP": []})

    if not combined.is_empty():
        combined = combined.unique(subset=["NIO"], keep="first")

    return combined


def _process_date(
    target_date: datetime,
    orca_enriched_1day: Optional[Path],
    sanplat_enriched_1day: Optional[Path],
    cfg: Dict[str, Any],
    sink: SinkManager,
    memory: MemoryMonitor,
) -> None:
    """Process one date: compute DISP → join Diario → join MEDIDORES → aggregate → sink."""
    paths = cfg["paths"]
    diario_dir: str = paths["diario_dir"]
    medidores_path: str = paths["medidores"]
    date_str = target_date.strftime("%Y-%m-%d")

    logger.info("=" * 60)
    logger.info("Processing date: %s", date_str)
    memory.log_status(f"start {date_str}")

    # Extract DISP from previous day only (no moving window)
    combined_disp = _compute_combined_disp(
        target_date=target_date,
        orca_enriched=str(orca_enriched_1day) if orca_enriched_1day else None,
        sanplat_enriched=str(sanplat_enriched_1day) if sanplat_enriched_1day else None,
    )

    if combined_disp.is_empty():
        logger.warning("No DISP data computed for %s — skipping", date_str)
        return

    # Join with Diario
    joined = _safe_join(combined_disp, target_date, diario_dir, "MIXED")
    del combined_disp

    if joined.is_empty():
        logger.warning("No data after Diario join for %s — skipping", date_str)
        return

    # Join with MEDIDORES to add INTELIGENTE
    joined = join_with_medidores(joined, medidores_path)

    # Aggregate
    agg = aggregate(joined, target_date)
    del joined

    # Sink (stream append)
    sink.submit(agg)
    del agg
    memory.log_status(f"end {date_str}")


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
    """Run simplified pipeline (Phase 1 + 3, skip Phase 2)."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    logger.info("Simplified pipeline starting — config: %s", config_path)
    t0 = time.perf_counter()

    cfg = load_config(config_path)

    # ── Phase 1: Enrichment ──────────────────────────────────────
    orca_enriched_1day, sanplat_enriched_1day = _run_enrichment(cfg)

    # ── Phase 3: Join & Aggregate (final deliverable) ────────────
    # (Phase 2 is skipped — DISP computed on-demand per date)
    logger.info("=" * 60)
    logger.info("Phase 3: Join & Aggregate (DISP computed on-demand)")

    memory = MemoryMonitor(
        threshold_percent=cfg.get("memory_threshold_percent", 70)
    )
    sink = SinkManager(
        max_concurrent=cfg.get("sink_queue_limit", 3),
        output_format=cfg.get("output_format", "parquet"),
        retry_attempts=cfg.get("sink_retry_attempts", 3),
        memory_monitor=memory,
    )

    output_dir = Path(cfg["paths"]["municipio_daily_output"])
    output_dir.mkdir(parents=True, exist_ok=True)
    target_month = cfg["target_month"]
    csv_stream_path = output_dir / f"municipio_{target_month}_1day.csv"
    sink.open_stream(csv_stream_path)

    month_dates = build_month_dates(target_month)
    success = 0
    failures = 0
    for d in month_dates:
        try:
            _process_date(d, orca_enriched_1day, sanplat_enriched_1day, cfg, sink, memory)
            success += 1
        except MemoryError:
            logger.critical("Aborting pipeline — memory threshold exceeded")
            break
        except Exception as exc:
            logger.error("Date %s failed: %s", d.strftime("%Y-%m-%d"), exc)
            failures += 1

    # Close stream and optionally convert to parquet
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
        len(month_dates),
    )
    memory.log_status("pipeline_end")


if __name__ == "__main__":
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_CONFIG
    run(cfg_path)
