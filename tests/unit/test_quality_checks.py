from __future__ import annotations

import duckdb

from homeradar.common.quality_checks import run_all_checks


def test_run_all_checks_allows_missing_language_column() -> None:
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            """
            CREATE TABLE urls (
                url TEXT,
                title TEXT,
                summary TEXT,
                published_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            INSERT INTO urls VALUES
            ('https://example.com/1', 'title', 'summary', CURRENT_TIMESTAMP)
            """
        )

        run_all_checks(
            conn,
            table_name="urls",
            null_conditions={
                "url": "url IS NULL OR url = ''",
                "title": "title IS NULL OR title = ''",
            },
            text_columns=["title", "summary"],
            language_column=None,
            url_column="url",
            date_column="published_at",
        )
