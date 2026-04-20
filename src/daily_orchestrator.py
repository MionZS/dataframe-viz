"""Daily orchestrator for incremental pipeline computation.

Runs month availability checks and computes only missing months,
then concatenates all results. Designed for daily execution.
"""

import logging
import sys
import time
from pathlib import Path
from typing import Dict, List

import yaml

from src.month_checker import get_months_to_compute, print_plan

logger = logging.getLogger(__name__)


def run_pipeline_for_month(config_path: str, target_month: str, pipeline_name: str) -> bool:
    """Run pipeline orchestrator for a specific month.

    Parameters
    ----------
    config_path:
        Path to pipeline.yaml config.
    target_month:
        Target month as "YYYY-MM".
    pipeline_name:
        Pipeline module name: "pipeline_orchestrator_simple" or "pipeline_orchestrator".

    Returns
    -------
    bool
        True if successful, False if failed.
    """
    try:
        # Update config with target month
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)

        cfg["target_month"] = target_month

        with open(config_path, "w", encoding="utf-8") as f:
            yaml.dump(cfg, f)

        logger.info(
            "Updated config: target_month = %s", target_month
        )

        # Import and run pipeline
        if pipeline_name == "simple":
            from src.pipeline_orchestrator_simple import run as run_pipeline
        else:
            from src.pipeline_orchestrator import run as run_pipeline

        logger.info("Starting %s pipeline for %s...", pipeline_name, target_month)
        t0 = time.perf_counter()

        run_pipeline(config_path)

        elapsed = time.perf_counter() - t0
        logger.info(
            "✓ %s pipeline completed in %.1fs for %s",
            pipeline_name,
            elapsed,
            target_month,
        )
        return True

    except Exception as exc:
        logger.error("✗ %s pipeline failed for %s: %s", pipeline_name, target_month, exc)
        return False


def concatenate_outputs(output_dir: str = "data/trusted/municipio_daily", pipeline_name: str = "simple") -> bool:
    """Concatenate all monthly outputs into single file.

    Parameters
    ----------
    output_dir:
        Path to municipio_daily_output directory.
    pipeline_name:
        Pipeline identifier: "simple" or "full" (determines output filename suffix).

    Returns
    -------
    bool
        True if successful, False if failed.
    """
    try:
        from scripts.concat_indicador import main as concat_main

        # Determine output filename based on pipeline
        suffix = "_simple" if pipeline_name == "simple" else "_full"
        output_file = Path(output_dir).parent / f"Indicador_comunicacao{suffix}.csv"

        logger.info("Concatenating all monthly outputs → %s", output_file)
        result = concat_main(
            input_dir=output_dir,
            output=str(output_file),
            drop_duplicates=False,
        )
        
        if result == 0:
            logger.info("✓ Concatenation complete: %s", output_file)
            return True
        else:
            logger.error("✗ Concatenation returned error code %d", result)
            return False

    except Exception as exc:
        logger.error("✗ Concatenation failed: %s", exc)
        return False


def verify_output(output_file: str) -> bool:
    """Verify integrity of output file.

    Returns
    -------
    bool
        True if valid, False if not.
    """
    try:
        from src.verify_output import verify_output_integrity, print_verification_report

        is_valid, report = verify_output_integrity(output_file)
        print_verification_report(report)
        return is_valid

    except Exception as exc:
        logger.error("✗ Verification failed: %s", exc)
        return False


def run_daily_orchestrator(
    config_path: str = "config/pipeline.yaml",
    diario_dir: str = "D:/Projects/visualizer-tuis/data/raw/CIS/Diario",
    output_dir: str = "data/trusted/municipio_daily",
    pipelines: List[str] = None,
) -> Dict[str, bool]:
    """Run full daily orchestration workflow.

    Parameters
    ----------
    config_path:
        Path to pipeline.yaml.
    diario_dir:
        Path to Diario data directory.
    output_dir:
        Path to output directory.
    pipelines:
        List of pipelines to run: ["simple", "full"] or subset.

    Returns
    -------
    Dict[str, bool]
        Status of each operation: {"simple": bool, "full": bool, "concat": bool, "verify": bool}.
    """
    if pipelines is None:
        pipelines = ["simple", "full"]

    status = {
        "simple": False,
        "full": False,
        "concat": False,
        "verify": False,
    }

    # Step 1: Analyze availability
    logger.info("=" * 70)
    logger.info("DAILY ORCHESTRATOR START")
    logger.info("=" * 70)

    plan = print_plan(diario_dir, output_dir)

    # Step 2: Run computations for missing months
    for pipeline in pipelines:
        months_to_compute = plan[pipeline]

        if not months_to_compute:
            logger.info("✓ %s pipeline: all available months already computed", pipeline)
            status[pipeline] = True
            continue

        logger.info(
            "→ %s pipeline: computing %d month(s)...",
            pipeline,
            len(months_to_compute),
        )

        pipeline_name = "pipeline_orchestrator_simple" if pipeline == "simple" else "pipeline_orchestrator"
        all_ok = True

        for month in months_to_compute:
            success = run_pipeline_for_month(config_path, month, pipeline)
            if not success:
                all_ok = False

        status[pipeline] = all_ok

    # Step 3: Concatenate results (one per pipeline)
    concat_results = {}
    for pipeline in pipelines:
        if status.get(pipeline):
            logger.info("→ Concatenating outputs for %s pipeline...", pipeline)
            concat_ok = concatenate_outputs(output_dir, pipeline)
            concat_results[pipeline] = concat_ok
        else:
            concat_results[pipeline] = False

    status["concat"] = any(concat_results.values())

    # Step 4: Verify final outputs
    for pipeline in pipelines:
        suffix = "_simple" if pipeline == "simple" else "_full"
        final_output = Path(output_dir).parent / f"Indicador_comunicacao{suffix}.csv"
        
        if concat_results.get(pipeline):
            logger.info("→ Verifying %s output integrity...", pipeline)
            verify_ok = verify_output(str(final_output))
            status["verify"] = verify_ok and status.get("verify", True)
        else:
            logger.warning("Skipping verification for %s (not computed)", pipeline)

    # Summary
    logger.info("=" * 70)
    logger.info("DAILY ORCHESTRATOR SUMMARY")
    logger.info("=" * 70)
    logger.info("  Simple pipeline:  %s", "✓" if status["simple"] else "✗")
    logger.info("  Full pipeline:    %s", "✓" if status["full"] else "✗")
    logger.info("  Concatenation:    %s", "✓" if status["concat"] else "✗")
    logger.info("  Verification:     %s", "✓" if status["verify"] else "✗")
    
    # Print output files
    if status["simple"]:
        simple_out = Path(output_dir).parent / "Indicador_comunicacao_simple.csv"
        logger.info("  → Simple output: %s", simple_out)
    if status["full"]:
        full_out = Path(output_dir).parent / "Indicador_comunicacao_full.csv"
        logger.info("  → Full output:   %s", full_out)
    
    logger.info("=" * 70)

    return status


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    config_path = sys.argv[1] if len(sys.argv) > 1 else "config/pipeline.yaml"
    diario_dir = sys.argv[2] if len(sys.argv) > 2 else "D:/Projects/visualizer-tuis/data/raw/CIS/Diario"
    output_dir = sys.argv[3] if len(sys.argv) > 3 else "data/trusted/municipio_daily"
    pipelines = sys.argv[4].split(",") if len(sys.argv) > 4 else ["simple", "full"]

    status = run_daily_orchestrator(config_path, diario_dir, output_dir, pipelines)

    # Exit 0 if all OK, 1 if any failed
    all_ok = all(status.values())
    sys.exit(0 if all_ok else 1)
