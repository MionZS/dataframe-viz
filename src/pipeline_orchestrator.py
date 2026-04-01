"""Pipeline orchestrator for Smart Meter Communication Consolidation.

Reads config/pipeline.yaml and executes three phases:

    Phase 1 — Enrichment  (intermediate deliverables)
        Rename relative date columns to absolute dates.
        Output: trusted/ORCA/…_com_datas.csv, trusted/SANPLAT/…_com_datas.csv

    Phase 2 — Moving Window  (intermediate deliverables)
        Compute per-NIO DISP flag (0/1) for each day using 5-day OR window.
        Output per origin: trusted/ORCA/disp_YYYY-MM.csv,
                           trusted/SANPLAT/disp_YYYY-MM.csv
        Concatenated:      mixed/disp_YYYY-MM.csv

    Phase 3 — Join & Aggregate  (final deliverable)
        Inner-join with MEDIDORES (filter to smart brands only),
        left-join with Diario (MUNICIPIO), aggregate by
        [MUNICIPIO, INTELIGENTE].  DISP = CONTAGEM_COMM / CONTAGEM_TOT.
        Stream-sink into a single output file.
        Output: municipio_daily/municipio_YYYY-MM.csv (.parquet)

See docs/prd/PRD_smart_meter_pipeline_v1.0.0.md for specification.
"""

import logging
import sys
import time
from calendar import monthrange
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import polars as pl
import yaml

from src.enrich_dates import process_orca, process_sanplat
from src.join_daily import join_with_diario, join_with_medidores
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

    DISP is computed here as ``CONTAGEM_COMM / CONTAGEM_TOT`` (rounded to
    4 decimal places).  Each municipality can have up to 3 groups — one
    per smart-meter brand (Hexing, Nansen, Nansen Ipiranga).
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
# Phase 2 — Moving Window (intermediate deliverables)
# ------------------------------------------------------------------


def _run_moving_window(cfg: Dict[str, Any]) -> Optional[Path]:
    """Phase 2: compute DISP for each target-month day, save intermediates.

    For each origin (ORCA, SANPLAT) produces a long-format CSV:
        [NIO, DISP, DATA]
    saved to trusted/<ORIGIN>/disp_<YYYY-MM>.csv.

    Then concatenates both into mixed/disp_<YYYY-MM>.csv (adds ORIGEM).

    Returns the path to the mixed CSV, or None on total failure.
    """
    paths = cfg["paths"]
    target_month: str = cfg["target_month"]
    window_days: int = cfg.get("moving_window_days", 5)
    binarize_orca: bool = cfg.get("orca_binarize", True)

    month_dates = build_month_dates(target_month)
    t0 = time.perf_counter()
    logger.info("=" * 60)
    logger.info(
        "Phase 2: Moving Window — %d dates (%s)",
        len(month_dates),
        target_month,
    )

    origin_configs = [
        ("ORCA", paths["orca_enriched"], binarize_orca),
        ("SANPLAT", paths["sanplat_enriched"], False),
    ]

    mixed_frames: List[pl.DataFrame] = []

    for origem, enriched_path, binarize in origin_configs:
        frames: List[pl.DataFrame] = []
        for d in month_dates:
            try:
                disp = compute_disp(
                    enriched_path=enriched_path,
                    target_date=d,
                    window_days=window_days,
                    binarize=binarize,
                )
                if not disp.is_empty():
                    disp = disp.with_columns(
                        pl.lit(d.strftime("%Y-%m-%d")).alias("DATA"),
                    )
                    frames.append(disp)
            except Exception as exc:
                logger.error(
                    "DISP failed [%s] %s: %s",
                    origem,
                    d.strftime("%Y-%m-%d"),
                    exc,
                )

        if not frames:
            logger.warning("No DISP frames produced for %s — skipping", origem)
            continue

        origin_df = pl.concat(frames)
        del frames

        # Save per-origin intermediate
        out_dir = Path(enriched_path).parent
        out_dir.mkdir(parents=True, exist_ok=True)
        origin_csv = out_dir / f"disp_{target_month}.csv"
        origin_df.write_csv(str(origin_csv))
        logger.info(
            "[OK] %s DISP saved → %s (%d rows)",
            origem,
            origin_csv,
            origin_df.height,
        )

        # Tag with ORIGEM for mixed concat
        origin_df = origin_df.with_columns(
            pl.lit(origem).alias("ORIGEM"),
        )
        mixed_frames.append(origin_df)
        del origin_df

    if not mixed_frames:
        logger.error("No DISP data produced — cannot continue")
        return None

    mixed_df = pl.concat(mixed_frames)
    del mixed_frames

    mixed_dir = Path(paths["mixed_output"])
    mixed_dir.mkdir(parents=True, exist_ok=True)
    mixed_csv = mixed_dir / f"disp_{target_month}.csv"
    mixed_df.write_csv(str(mixed_csv))
    logger.info(
        "[OK] Mixed DISP saved → %s (%d rows)",
        mixed_csv,
        mixed_df.height,
    )
    del mixed_df

    elapsed = time.perf_counter() - t0
    logger.info("Moving window phase complete in %.1fs", elapsed)
    return mixed_csv


# ------------------------------------------------------------------
# Phase 3 — Join & Aggregate (final deliverable only)
# ------------------------------------------------------------------


def _process_date(
    target_date: datetime,
    mixed_disp: pl.LazyFrame,
    cfg: Dict[str, Any],
    sink: SinkManager,
    memory: MemoryMonitor,
) -> None:
    """Process one date: filter DISP → join Diario → join MEDIDORES → aggregate → sink."""
    paths = cfg["paths"]
    diario_dir: str = paths["diario_dir"]
    medidores_path: str = paths["medidores"]
    date_str = target_date.strftime("%Y-%m-%d")

    logger.info("=" * 60)
    logger.info("Processing date: %s", date_str)
    memory.log_status(f"start {date_str}")

    # Collect this date's rows from mixed DISP
    day_disp = (
        mixed_disp
        .filter(pl.col("DATA") == date_str)
        .collect()
    )

    if day_disp.is_empty():
        logger.warning("No DISP data for %s — skipping", date_str)
        return

    # Combine ORCA and SANPLAT into a single DISP frame.
    # ORCA and SANPLAT cover disjoint meter populations but BOTH originate
    # from the same Diário universe.  Joining Diário LEFT per-origin and then
    # concatenating means every NIO in Diário appears TWICE, doubling
    # CONTAGEM_TOT.  Instead we merge first, then do exactly ONE Diário join.
    combined_disp = (
        day_disp
        .select(["NIO", "DISP"])
        .unique(subset=["NIO"], keep="first")  # safety: ORCA/SANPLAT overlap
    )
    del day_disp

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
    """Run the full pipeline (3 phases)."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    logger.info("Pipeline starting — config: %s", config_path)
    t0 = time.perf_counter()

    cfg = load_config(config_path)

    # ── Phase 1: Enrichment ──────────────────────────────────────
    _run_enrichment(cfg)

    # ── Phase 2: Moving Window (intermediates) ───────────────────
    mixed_csv = _run_moving_window(cfg)
    if mixed_csv is None:
        logger.error("Pipeline aborted — no moving-window output")
        return

    # ── Phase 3: Join & Aggregate (final deliverable) ────────────
    logger.info("=" * 60)
    logger.info("Phase 3: Join & Aggregate")

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
    csv_stream_path = output_dir / f"municipio_{target_month}.csv"
    sink.open_stream(csv_stream_path)

    # Lazy-scan the mixed DISP CSV — rows are pulled per-date, not all at once
    mixed_lazy = pl.scan_csv(str(mixed_csv))

    month_dates = build_month_dates(target_month)
    success = 0
    failures = 0
    for d in month_dates:
        try:
            _process_date(d, mixed_lazy, cfg, sink, memory)
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
