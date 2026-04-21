#!/usr/bin/env python3
"""Run DuckDB data quality checks."""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from homeradar.common.quality_checks import run_all_checks  # noqa: E402
from homeradar.config_loader import load_settings  # noqa: E402


def _table_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    return {str(row[1]) for row in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()}


def main() -> None:
    db_path = load_settings().database_path
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


if __name__ == "__main__":
    main()
