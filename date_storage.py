from __future__ import annotations

import logging
import shutil
from collections.abc import Callable
from datetime import date, timedelta
from pathlib import Path

from radar_core.dates import daily_partition_date


logger = logging.getLogger(__name__)


def snapshot_database(
    db_path: Path,
    *,
    snapshot_date: date | None = None,
    snapshot_root: Path | None = None,
) -> Path | None:
    if not db_path.exists():
        return None

    target_date = snapshot_date or daily_partition_date()
    target_root = snapshot_root or db_path.parent / "snapshots"
    target_dir = target_root / target_date.isoformat()
    target_dir.mkdir(parents=True, exist_ok=True)

    target_path = target_dir / db_path.name
    shutil.copy2(db_path, target_path)
    return target_path


def cleanup_date_directories(base_dir: Path, *, keep_days: int, today: date | None = None) -> int:
    if keep_days < 0 or not base_dir.exists():
        return 0

    cutoff = (today or daily_partition_date()) - timedelta(days=keep_days)
    removed = 0
    for child in base_dir.iterdir():
        if not child.is_dir():
            continue
        try:
            child_date = date.fromisoformat(child.name)
        except ValueError:
            continue

        if child_date < cutoff:
            shutil.rmtree(child)
            removed += 1
    return removed


def cleanup_dated_reports(report_dir: Path, *, keep_days: int, today: date | None = None) -> int:
    if keep_days < 0 or not report_dir.exists():
        return 0

    cutoff = (today or daily_partition_date()) - timedelta(days=keep_days)
    removed = 0
    for html_file in report_dir.glob("*.html"):
        if html_file.name == "index.html":
            continue

        stamp: date | None = None
        stem = html_file.stem
        if len(stem) >= 8 and stem[-8:].isdigit():
            try:
                stamp = date.fromisoformat(f"{stem[-8:-4]}-{stem[-4:-2]}-{stem[-2:]}")
            except ValueError:
                stamp = None
        elif len(stem) == 10:
            try:
                stamp = date.fromisoformat(stem)
            except ValueError:
                stamp = None

        if stamp is not None and stamp < cutoff:
            html_file.unlink()
            removed += 1
    return removed


def _load_compactor() -> Callable[..., dict[str, object]]:
    """Return radar-core's DuckDB compactor.

    HomeRadar writes through GraphStore, then copies the file in
    ``snapshot_database`` before git add. ``compact_duckdb_file`` has to run
    on that path first. ``RADAR_DUCKDB_MAX_BYTES`` and ``RADAR_DUCKDB_COMPACT``
    are read inside radar-core (default budget 90 MiB).
    """
    from radar_core.db_compact import compact_duckdb_file

    return compact_duckdb_file


def _compact_database(database_path: Path) -> dict[str, object]:
    """Shrink ``database_path`` before it is copied or committed.

    Compaction failure must not block the report or the snapshot. The file
    already on disk is left in place.
    """
    bytes_now = database_path.stat().st_size if database_path.exists() else 0
    fallback: dict[str, object] = {
        "compacted": False,
        "reason": "missing" if not database_path.exists() else "skipped",
        "bytes": bytes_now,
        "bytes_before": bytes_now,
        "rows_deleted": 0,
    }
    if not database_path.exists():
        return fallback
    try:
        compact_duckdb_file = _load_compactor()
    except ImportError as exc:
        logger.warning("duckdb_compact_unavailable path=%s error=%s", database_path, exc)
        fallback["reason"] = "unavailable"
        return fallback
    try:
        result = compact_duckdb_file(database_path)
    except Exception as exc:
        logger.warning("duckdb_compact_failed path=%s error=%s", database_path, exc)
        fallback["reason"] = "failed"
        fallback["bytes"] = database_path.stat().st_size if database_path.exists() else 0
        return fallback
    if isinstance(result, dict):
        return result
    fallback["reason"] = "invalid_result"
    return fallback


def apply_date_storage_policy(
    *,
    database_path: Path,
    raw_data_dir: Path,
    report_dir: Path,
    keep_raw_days: int,
    keep_report_days: int,
    snapshot_db: bool,
) -> dict[str, object]:
    # Compact first so the snapshot is a copy of the file that will be committed.
    compact = _compact_database(database_path)
    logger.info(
        "duckdb_compact path=%s reason=%s bytes_before=%s bytes=%s rows_deleted=%s",
        database_path,
        compact.get("reason"),
        compact.get("bytes_before"),
        compact.get("bytes"),
        compact.get("rows_deleted"),
    )
    snapshot_path = snapshot_database(database_path) if snapshot_db else None
    raw_removed = cleanup_date_directories(raw_data_dir, keep_days=keep_raw_days)
    report_removed = cleanup_dated_reports(report_dir, keep_days=keep_report_days)
    return {
        "snapshot_path": str(snapshot_path) if snapshot_path is not None else None,
        "raw_removed": raw_removed,
        "report_removed": report_removed,
        "db_bytes": compact.get("bytes"),
        "db_bytes_before": compact.get("bytes_before"),
        "db_compacted": compact.get("compacted"),
        "db_rows_deleted": compact.get("rows_deleted"),
        "db_reason": compact.get("reason"),
    }
