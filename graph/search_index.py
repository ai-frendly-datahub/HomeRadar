from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SearchResult:
    link: str
    title: str
    body: str
    rank: float


class SearchIndex:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _init_schema(self) -> None:
        with closing(self._connect()) as conn:
            conn.execute(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts
                USING fts5(link UNINDEXED, title, body, tokenize='unicode61')
                """
            )
            conn.commit()

    def upsert(self, link: str, title: str, body: str) -> None:
        with closing(self._connect()) as conn:
            conn.execute("DELETE FROM documents_fts WHERE link = ?", (link,))
            conn.execute(
                "INSERT INTO documents_fts(link, title, body) VALUES (?, ?, ?)",
                (link, title, body),
            )
            conn.commit()

    def search(self, query: str, *, limit: int = 20) -> list[SearchResult]:
        if limit <= 0:
            return []

        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT link, title, body, rank
                FROM documents_fts
                WHERE documents_fts MATCH ?
                ORDER BY rank
                LIMIT ?
                """,
                (query, limit),
            ).fetchall()

        return [
            SearchResult(link=str(row[0]), title=str(row[1]), body=str(row[2]), rank=float(row[3]))
            for row in rows
        ]
