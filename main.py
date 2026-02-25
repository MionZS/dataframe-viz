#!/usr/bin/env python3
"""
Main entry point for visualizer-tuis project.

Provides quick access to:
- Lazy Frame Viewer: Interactive data file viewer
- Data Inspector: Quick schema and data inspection  
- Enrich Dates: Add date references to communication data
"""

import sys
import argparse

SCRIPT_NAME = "main.py"


def run_viewer():
    """Launch the lazy frame viewer."""
    from src.tui.lazy_frame_viewer import main
    main()


def run_inspector():
    """Launch the data inspector."""
    from src.tui.data_inspector import main
    main()


def run_enrichment():
    """Run the date enrichment script."""
    from src.enrich_dates import main
    main()


def run_pipeline(config_path: str = "config/pipeline.yaml"):
    """Run the smart-meter consolidation pipeline."""
    from src.pipeline_orchestrator import run
    run(config_path)


def _build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser with subcommands."""
    parser = argparse.ArgumentParser(
        description="Visualizer TUIs - Data visualization and inspection tools"
    )
    subparsers = parser.add_subparsers(
        title="Commands", dest="command", help="Available commands"
    )

    viewer_parser = subparsers.add_parser("view", help="Launch the lazy frame viewer")
    viewer_parser.add_argument("--file", "-f", help="Data file to view")
    viewer_parser.add_argument("--dir-as-file", help="Directory to view as single file")

    inspector_parser = subparsers.add_parser("inspect", help="Launch the data inspector")
    inspector_parser.add_argument("--file", "-f", help="Data file to inspect")

    enrich_parser = subparsers.add_parser("enrich", help="Enrich data with date references")
    enrich_parser.add_argument("--orca", action="store_true", help="Process ORCA data")
    enrich_parser.add_argument("--sanplat", action="store_true", help="Process SANPLAT data")
    enrich_parser.add_argument("--all", action="store_true", help="Process all data")
    enrich_parser.add_argument("--data-file", help="Custom data file path")
    enrich_parser.add_argument("--ref-file", help="Custom reference file path")

    pipeline_parser = subparsers.add_parser("pipeline", help="Run smart-meter consolidation pipeline")
    pipeline_parser.add_argument(
        "--config", "-c",
        default="config/pipeline.yaml",
        help="Path to pipeline YAML config (default: config/pipeline.yaml)",
    )

    return parser


def _handle_view(args: argparse.Namespace) -> None:
    """Handle the 'view' subcommand."""
    if args.file or args.dir_as_file:
        flag = "--file" if args.file else "--dir-as-file"
        sys.argv = [SCRIPT_NAME, flag, args.file or args.dir_as_file]
    run_viewer()


def _handle_inspect(args: argparse.Namespace) -> None:
    """Handle the 'inspect' subcommand."""
    if args.file:
        sys.argv = [SCRIPT_NAME, "--file", args.file]
    run_inspector()


def _handle_enrich(args: argparse.Namespace) -> None:
    """Handle the 'enrich' subcommand."""
    sys.argv = [SCRIPT_NAME]
    if args.orca:
        sys.argv.append("--orca")
    if args.sanplat:
        sys.argv.append("--sanplat")
    if args.all:
        sys.argv.append("--all")
    if args.data_file:
        sys.argv.extend(["--data-file", args.data_file])
    if args.ref_file:
        sys.argv.extend(["--ref-file", args.ref_file])
    run_enrichment()


def _print_usage() -> None:
    """Print usage information when no subcommand is given."""
    print("Visualizer TUIs - Data visualization and inspection tools")
    print("\nAvailable commands:")
    print("  view      - Interactive file viewer")
    print("  inspect   - Quick data inspection")
    print("  enrich    - Add date references to data")
    print("  pipeline  - Run consolidation pipeline")
    print(f"\nUsage: python {SCRIPT_NAME} [command] [options]")
    print(f"       python {SCRIPT_NAME} view --help")
    print(f"       python {SCRIPT_NAME} inspect --help")
    print(f"       python {SCRIPT_NAME} enrich --help")
    print(f"       python {SCRIPT_NAME} pipeline --help")


def _handle_pipeline(args: argparse.Namespace) -> None:
    """Handle the 'pipeline' subcommand."""
    run_pipeline(args.config)


_COMMAND_HANDLERS = {
    "view": _handle_view,
    "inspect": _handle_inspect,
    "enrich": _handle_enrich,
    "pipeline": _handle_pipeline,
}


def main():
    args = _build_parser().parse_args()

    handler = _COMMAND_HANDLERS.get(args.command)
    if handler:
        handler(args)
    else:
        _print_usage()

if __name__ == "__main__":
    main()
