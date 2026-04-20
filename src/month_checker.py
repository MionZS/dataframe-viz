"""Monthly availability checker and computation orchestrator.

Detects available Diario data by month, checks which months have been
computed, and determines what needs to be generated (for both full and
simplified pipelines).

Can be run daily to incrementally compute months as data becomes available.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set

import polars as pl

logger = logging.getLogger(__name__)


def get_available_months(diario_dir: str) -> List[str]:
    """Get list of months for which complete Diario data exists.

    A month is considered complete if it has all expected days
    (28/29/30/31 depending on the month).

    Parameters
    ----------
    diario_dir:
        Path to directory containing Diario_YYYY-MM-DD.parquet files.

    Returns
    -------
    List[str]
        Sorted list of available months as "YYYY-MM".
    """
    diario_path = Path(diario_dir)
    if not diario_path.exists():
        logger.warning("Diario directory not found: %s", diario_dir)
        return []

    # Group files by month
    month_days: Dict[str, Set[int]] = {}
    for file in diario_path.glob("Diario_*.parquet"):
        try:
            # Extract date from filename: Diario_YYYY-MM-DD.parquet
            date_str = file.stem.split("_")[1]  # "2026-03-07"
            year_month = date_str[:7]  # "2026-03"
            day = int(date_str.split("-")[2])
            if year_month not in month_days:
                month_days[year_month] = set()
            month_days[year_month].add(day)
        except Exception as exc:
            logger.warning("Failed to parse file %s: %s", file, exc)

    # Check which months are complete
    available = []
    for year_month in sorted(month_days.keys()):
        # Determine expected days in month
        year, month = map(int, year_month.split("-"))
        if month == 2:
            expected_days = 29 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 28
        elif month in [4, 6, 9, 11]:
            expected_days = 30
        else:
            expected_days = 31

        actual_days = len(month_days[year_month])
        if actual_days == expected_days:
            available.append(year_month)
            logger.info(
                "Month %s is complete: %d/%d days",
                year_month,
                actual_days,
                expected_days,
            )
        else:
            logger.info(
                "Month %s is incomplete: %d/%d days",
                year_month,
                actual_days,
                expected_days,
            )

    return available


def get_computed_months(output_dir: str, pipeline_name: str) -> List[str]:
    """Get list of months that have been computed for a given pipeline.

    Parameters
    ----------
    output_dir:
        Path to municipio_daily_output directory.
    pipeline_name:
        Pipeline identifier: "simple" or "full" (used in filename suffixes).

    Returns
    -------
    List[str]
        Sorted list of computed months as "YYYY-MM".
    """
    output_path = Path(output_dir)
    if not output_path.exists():
        return []

    computed = set()
    suffix = f"_1day.csv" if pipeline_name == "simple" else ".csv"
    pattern = f"municipio_*{suffix}*"

    for file in output_path.glob(pattern):
        try:
            # Extract month from filename: municipio_YYYY-MM_1day.csv
            parts = file.stem.split("_")
            if len(parts) >= 2:
                year_month = parts[1]
                if len(year_month) == 7 and year_month[4] == "-":  # "2026-03" format
                    computed.add(year_month)
        except Exception as exc:
            logger.warning("Failed to parse file %s: %s", file, exc)

    return sorted(list(computed))


def get_months_to_compute(
    diario_dir: str, output_dir: str, pipeline_name: str
) -> List[str]:
    """Determine which months need to be computed.

    Compares available months (from Diario data) with computed months
    (from output directory).

    Parameters
    ----------
    diario_dir:
        Path to Diario directory.
    output_dir:
        Path to municipio_daily_output directory.
    pipeline_name:
        Pipeline identifier: "simple" or "full".

    Returns
    -------
    List[str]
        Sorted list of months to compute as "YYYY-MM".
    """
    available = get_available_months(diario_dir)
    computed = get_computed_months(output_dir, pipeline_name)

    to_compute = sorted(list(set(available) - set(computed)))

    logger.info(
        "Pipeline %s: available=%s, computed=%s, to_compute=%s",
        pipeline_name,
        available,
        computed,
        to_compute,
    )

    return to_compute


def print_plan(
    diario_dir: str, output_dir: str
) -> Dict[str, List[str]]:
    """Analyze and print the computation plan for both pipelines.

    Returns
    -------
    Dict[str, List[str]]
        Plan with keys "simple" and "full", each mapping to list of months to compute.
    """
    plan = {
        "simple": get_months_to_compute(diario_dir, output_dir, "simple"),
        "full": get_months_to_compute(diario_dir, output_dir, "full"),
    }

    print("\n" + "=" * 70)
    print("COMPUTATION PLAN")
    print("=" * 70)

    available = get_available_months(diario_dir)
    print(f"\nAvailable data (Diario):")
    print(f"  Months: {available}")

    for pipeline in ["simple", "full"]:
        computed = get_computed_months(output_dir, pipeline)
        to_compute = plan[pipeline]

        print(f"\nPipeline '{pipeline}':")
        print(f"  Computed:   {computed}")
        print(f"  To compute: {to_compute}")

        if not to_compute:
            print(f"  Status: ✓ All available months are up-to-date")
        else:
            print(f"  Status: Need to compute {len(to_compute)} month(s)")

    print("=" * 70 + "\n")

    return plan


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    # Default paths (can be overridden via args)
    diario_dir = sys.argv[1] if len(sys.argv) > 1 else "D:/Projects/visualizer-tuis/data/raw/CIS/Diario"
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "data/trusted/municipio_daily"

    plan = print_plan(diario_dir, output_dir)

    # Exit with status 0 if all up-to-date, 1 if computations needed
    needs_computation = any(plan.values())
    sys.exit(1 if needs_computation else 0)
