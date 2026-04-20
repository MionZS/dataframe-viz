"""Tests for source_retrieval module."""

import os
import tempfile
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

from src.source_retrieval import (
    find_latest_zip,
    is_file_modified_today,
    retrieve_and_refresh_diario,
)


def _set_mtime(path: Path, dt: datetime) -> None:
    ts = dt.timestamp()
    os.utime(path, (ts, ts))


def test_find_latest_zip_returns_most_recent():
    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp)
        z1 = src / "older.zip"
        z2 = src / "newer.zip"
        z1.write_bytes(b"a")
        time.sleep(0.01)
        z2.write_bytes(b"b")

        latest = find_latest_zip(str(src))
        assert latest is not None
        assert latest.name == "newer.zip"


def test_is_file_modified_today_false_for_old_file():
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "x.zip"
        p.write_bytes(b"x")
        _set_mtime(p, datetime.now() - timedelta(days=1))
        assert not is_file_modified_today(p)


def test_retrieve_and_refresh_diario_syncs_files_when_zip_is_today():
    with tempfile.TemporaryDirectory() as src_tmp, tempfile.TemporaryDirectory() as proj_tmp:
        src_dir = Path(src_tmp)
        project_root = Path(proj_tmp)
        diario_dir = project_root / "data" / "raw" / "CIS" / "Diario"
        diario_dir.mkdir(parents=True, exist_ok=True)

        zip_path = src_dir / "diario_update.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("foo/Diario_2026-04-01.parquet", b"dummy")
            zf.writestr("foo/Diario_2026-04-02.parquet", b"dummy")

        ok, details = retrieve_and_refresh_diario(
            source_dir=str(src_dir),
            project_root=str(project_root),
            diario_dir=str(diario_dir),
        )

        assert ok
        assert details["status"] == "ok"
        assert (diario_dir / "Diario_2026-04-01.parquet").exists()
        assert (diario_dir / "Diario_2026-04-02.parquet").exists()
