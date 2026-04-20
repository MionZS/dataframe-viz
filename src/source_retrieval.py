"""Retrieve latest source ZIP and refresh Diario files.

Flow:
1. Find newest .zip in the source folder.
2. Validate that its modification date is today.
3. Copy ZIP into project local staging folder.
4. Extract ZIP and copy Diario_YYYY-MM-DD.parquet files into target Diario dir,
   replacing existing files and adding new ones.
"""

import logging
import shutil
import zipfile
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)


def find_latest_zip(source_dir: str) -> Optional[Path]:
    """Return newest .zip file in source_dir or None if no zip exists."""
    src = Path(source_dir)
    if not src.exists():
        logger.error("Source directory not found: %s", source_dir)
        return None

    zips = [p for p in src.iterdir() if p.is_file() and p.suffix.lower() == ".zip"]
    if not zips:
        return None

    return max(zips, key=lambda p: p.stat().st_mtime)


def is_file_modified_today(path: Path) -> bool:
    """Check if file modification date is today's local date."""
    mtime = datetime.fromtimestamp(path.stat().st_mtime).date()
    return mtime == date.today()


def _extract_and_sync_diario(zip_path: Path, diario_dir: Path, extract_dir: Path) -> Tuple[int, int]:
    """Extract ZIP and sync Diario parquet files into diario_dir.

    Returns (new_files, replaced_files).
    """
    if extract_dir.exists():
        shutil.rmtree(extract_dir)
    extract_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)

    diario_dir.mkdir(parents=True, exist_ok=True)

    new_files = 0
    replaced_files = 0

    for src_file in extract_dir.rglob("Diario_*.parquet"):
        dst = diario_dir / src_file.name
        if dst.exists():
            replaced_files += 1
        else:
            new_files += 1
        shutil.copy2(src_file, dst)

    return new_files, replaced_files


def retrieve_and_refresh_diario(
    source_dir: str,
    project_root: str,
    diario_dir: str,
) -> Tuple[bool, Dict[str, object]]:
    """Retrieve current ZIP and refresh Diario files.

    Returns
    -------
    (ok, details) where details includes status/message and counters.
    """
    details: Dict[str, object] = {
        "status": "failed",
        "message": "",
        "zip_path": None,
        "zip_mtime": None,
        "local_zip": None,
        "new_files": 0,
        "replaced_files": 0,
    }

    latest = find_latest_zip(source_dir)
    if latest is None:
        details["message"] = "No ZIP files found in source folder"
        logger.warning(details["message"])
        return False, details

    details["zip_path"] = str(latest)
    details["zip_mtime"] = datetime.fromtimestamp(latest.stat().st_mtime).isoformat()

    if not is_file_modified_today(latest):
        details["message"] = (
            f"Latest ZIP is not from today: {latest.name} "
            f"({details['zip_mtime']})"
        )
        logger.warning(details["message"])
        return False, details

    project = Path(project_root)
    incoming_dir = project / "incoming_data"
    incoming_dir.mkdir(parents=True, exist_ok=True)

    local_zip = incoming_dir / latest.name
    shutil.copy2(latest, local_zip)
    details["local_zip"] = str(local_zip)

    extracted_dir = incoming_dir / "_extracted"
    new_files, replaced_files = _extract_and_sync_diario(
        zip_path=local_zip,
        diario_dir=Path(diario_dir),
        extract_dir=extracted_dir,
    )

    details["new_files"] = new_files
    details["replaced_files"] = replaced_files
    details["status"] = "ok"
    details["message"] = (
        f"ZIP synced successfully: {local_zip.name} | "
        f"new={new_files}, replaced={replaced_files}"
    )

    logger.info(details["message"])
    return True, details
