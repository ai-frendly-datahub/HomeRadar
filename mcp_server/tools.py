from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

from graph.graph_queries import get_trending_entities
from graph.graph_store import GraphStore
from graph.search_index import SearchIndex, SearchResult
from nl_query import parse_query

_READ_ONLY_SQL = re.compile(r"^\s*(SELECT|WITH|EXPLAIN)\b", re.IGNORECASE)


def handle_search(*, search_db_path: Path, db_path: Path, query: str, limit: int = 20) -> str:
    parsed = parse_query(query)
    search_text = parsed.search_text or parsed.original_query
    effective_limit = parsed.limit if parsed.limit > 0 else limit

    if not search_text.strip() or effective_limit <= 0:
        return json.dumps({"ok": True, "results": []}, ensure_ascii=False)

    index = SearchIndex(search_db_path)
    results = index.search(search_text, limit=effective_limit)

    if parsed.days is not None:
        results = _filter_results_by_days(db_path=db_path, results=results, days=parsed.days)

    payload = {
        "ok": True,
        "query": search_text,
        "days": parsed.days,
        "limit": effective_limit,
        "results": [
            {
                "url": item.link,
                "title": item.title,
                "summary": item.body,
                "rank": item.rank,
            }
            for item in results
        ],
    }
    return json.dumps(payload, ensure_ascii=False, default=str)


def handle_recent_updates(*, db_path: Path, days: int = 7, limit: int = 20) -> str:
    cutoff = datetime.now() - timedelta(days=days)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT url, title, source_id, published_at, region, property_type, price, area
            FROM urls
            WHERE published_at >= ?
            ORDER BY published_at DESC
            LIMIT ?
            """,
            [cutoff, limit],
        ).fetchall()

    payload = {
        "ok": True,
        "days": days,
        "limit": limit,
        "results": [
            {
                "url": row[0],
                "title": row[1],
                "source_id": row[2],
                "published_at": row[3],
                "region": row[4],
                "property_type": row[5],
                "price": row[6],
                "area": row[7],
            }
            for row in rows
        ],
    }
    return json.dumps(payload, ensure_ascii=False, default=str)


def handle_sql(*, db_path: Path, query: str) -> str:
    sql = query.strip()
    if not _is_read_only_query(sql):
        return json.dumps(
            {
                "ok": False,
                "error": "Only SELECT/WITH/EXPLAIN single-statement queries are allowed.",
            },
            ensure_ascii=False,
        )

    try:
        with duckdb.connect(str(db_path), read_only=True) as conn:
            cursor = conn.execute(sql)
            rows = cursor.fetchall()
            description = cursor.description
            columns = [str(col[0]) for col in description] if description else []
    except Exception as exc:  # noqa: BLE001
        return json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False)

    return json.dumps({"ok": True, "columns": columns, "rows": rows}, ensure_ascii=False, default=str)


def handle_price_watch(
    *,
    db_path: Path,
    region: str | None = None,
    min_price: float | None = None,
    max_price: float | None = None,
    limit: int = 20,
) -> str:
    with duckdb.connect(str(db_path), read_only=True) as conn:
        tx_columns = _table_columns(conn, "transactions")
        if {"url", "deal_date"}.issubset(tx_columns):
            rows = conn.execute(
                """
                SELECT t.complex_name, t.district, t.price, t.area, t.floor, t.deal_date,
                       u.title, u.url
                FROM transactions t
                JOIN urls u ON u.url = t.url
                WHERE (?1 IS NULL OR t.district LIKE '%' || ?1 || '%')
                  AND (?2 IS NULL OR t.price >= ?2)
                  AND (?3 IS NULL OR t.price <= ?3)
                ORDER BY t.deal_date DESC
                LIMIT ?4
                """,
                [region, min_price, max_price, limit],
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT t.complex_name, t.district, t.price, t.area, t.floor,
                       CAST(t.transaction_date AS VARCHAR) AS deal_date,
                       NULL AS title,
                       NULL AS url
                FROM transactions t
                WHERE (?1 IS NULL OR t.district LIKE '%' || ?1 || '%')
                  AND (?2 IS NULL OR t.price >= ?2)
                  AND (?3 IS NULL OR t.price <= ?3)
                ORDER BY t.transaction_date DESC
                LIMIT ?4
                """,
                [region, min_price, max_price, limit],
            ).fetchall()

    payload = {
        "ok": True,
        "region": region,
        "min_price": min_price,
        "max_price": max_price,
        "limit": limit,
        "results": [
            {
                "complex_name": row[0],
                "district": row[1],
                "price": row[2],
                "area": row[3],
                "floor": row[4],
                "deal_date": row[5],
                "title": row[6],
                "url": row[7],
            }
            for row in rows
        ],
    }
    return json.dumps(payload, ensure_ascii=False, default=str)


def handle_top_trends(
    *, db_path: Path, entity_type: str = "district", days: int = 7, limit: int = 10
) -> str:
    _ = days
    store = GraphStore(db_path)
    rows = get_trending_entities(store, entity_type=entity_type, limit=limit)

    payload = {
        "ok": True,
        "entity_type": entity_type,
        "days": days,
        "limit": limit,
        "results": [
            {"entity_value": row[0], "mention_count": row[1]}
            for row in rows
        ],
    }
    return json.dumps(payload, ensure_ascii=False, default=str)


def _is_read_only_query(query: str) -> bool:
    if not query or not _READ_ONLY_SQL.match(query):
        return False
    if ";" in query.rstrip(";"):
        return False
    return True


def _table_columns(conn: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    return {str(row[1]) for row in rows}


def _filter_results_by_days(
    *, db_path: Path, results: list[SearchResult], days: int
) -> list[SearchResult]:
    if not results:
        return []

    cutoff = datetime.now() - timedelta(days=days)
    links = [result.link for result in results]
    placeholders = ", ".join("?" for _ in links)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        rows = conn.execute(
            f"""
            SELECT url
            FROM urls
            WHERE published_at >= ? AND url IN ({placeholders})
            """,
            [cutoff, *links],
        ).fetchall()
    allowed = {str(row[0]) for row in rows}
    return [result for result in results if result.link in allowed]
