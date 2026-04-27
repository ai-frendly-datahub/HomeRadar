#!/usr/bin/env python3
"""Run DuckDB data quality checks."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import duckdb
import yaml


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from homeradar.common.quality_checks import run_all_checks  # noqa: E402
from homeradar.quality_report import build_quality_report, write_quality_report  # noqa: E402
from graph.graph_store import GraphStore  # noqa: E402


def _project_path(project_root: Path, raw_path: str | Path) -> Path:
    path = Path(raw_path)
    return path if path.is_absolute() else project_root / path


def _load_runtime_config(project_root: Path) -> dict[str, Any]:
    config_file = project_root / "config" / "config.yaml"
    if not config_file.exists():
        return {}
    raw = yaml.safe_load(config_file.read_text(encoding="utf-8")) or {}
    return raw if isinstance(raw, dict) else {}


def _load_source_quality_config(project_root: Path) -> dict[str, Any]:
    config_file = project_root / "config" / "sources.yaml"
    raw = yaml.safe_load(config_file.read_text(encoding="utf-8")) or {}
    return raw if isinstance(raw, dict) else {}


def _table_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    return {str(row[1]) for row in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()}


def generate_quality_artifacts(
    project_root: Path = PROJECT_ROOT,
    *,
    category_name: str = "home",
) -> tuple[dict[str, Path], dict[str, Any]]:
    runtime_config = _load_runtime_config(project_root)
    db_path = _project_path(
        project_root,
        str(runtime_config.get("database_path", "data/homeradar.duckdb")),
    )
    report_dir = _project_path(
        project_root,
        str(runtime_config.get("report_dir", "reports")),
    )
    metadata = _load_source_quality_config(project_root)
    store = GraphStore(db_path)
    report = build_quality_report(
        sources=metadata.get("sources", []),
        store=store,
        quality_config=metadata,
    )
    paths = write_quality_report(
        report,
        output_dir=report_dir,
        category_name=category_name,
    )
    return paths, report


def main() -> None:
    runtime_config = _load_runtime_config(PROJECT_ROOT)
    db_path = _project_path(
        PROJECT_ROOT,
        str(runtime_config.get("database_path", "data/homeradar.duckdb")),
    )
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        sys.exit(1)

    with duckdb.connect(str(db_path), read_only=True) as con:
        columns = _table_columns(con, "urls")
        run_all_checks(
            con,
            table_name="urls",
            null_conditions={
                "url": "url IS NULL OR url = ''",
                "title": "title IS NULL OR title = ''",
                "published_at": "published_at IS NULL",
            },
            text_columns=[column for column in ["title", "summary"] if column in columns],
            language_column="language" if "language" in columns else None,
            url_column="url",
            date_column="published_at",
        )

    paths, report = generate_quality_artifacts(PROJECT_ROOT)
    summary = report["summary"]
    print(f"quality_report={paths['latest']}")
    print(f"tracked_sources={summary['tracked_sources']}")
    print(f"fresh_sources={summary['fresh_sources']}")
    print(f"stale_sources={summary['stale_sources']}")
    print(f"missing_sources={summary['missing_sources']}")
    print(f"official_primary_status={summary['official_primary_status']}")


if __name__ == "__main__":
    main()
