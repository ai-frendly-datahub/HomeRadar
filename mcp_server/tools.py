from __future__ import annotations

import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import duckdb

from graph.graph_queries import get_trending_entities
from graph.graph_store import GraphStore
from homeradar.config_loader import load_source_quality_config
from homeradar.quality_report import build_quality_report
from homeradar.search_index import SearchIndex, SearchResult
from nl_query import parse_query


_READ_ONLY_SQL = re.compile(r"^\s*(SELECT|WITH|EXPLAIN)\b", re.IGNORECASE)


def handle_search(*, search_db_path: Path, db_path: Path, query: str, limit: int = 20) -> str:
    parsed = parse_query(query)
    search_text = parsed.search_text or parsed.original_query
    effective_limit = parsed.limit if parsed.limit is not None and parsed.limit > 0 else limit

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
                "summary": item.snippet,
                "rank": item.rank,
            }
            for item in results
        ],
    }
    return json.dumps(payload, ensure_ascii=False, default=str)


def handle_recent_updates(*, db_path: Path, days: int = 7, limit: int = 20) -> str:
    rows = _recent_updates_rows(db_path=db_path, days=days, limit=limit)

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
                "verification_state": row[8],
                "verification_role": row[9],
                "merge_policy": row[10],
                "event_model": row[11],
            }
            for row in rows
        ],
    }
    return json.dumps(payload, ensure_ascii=False, default=str)


def handle_quality_report(*, db_path: Path, sources_path: Path | None = None) -> str:
    metadata = load_source_quality_config(sources_path)
    store = GraphStore(db_path)
    report = build_quality_report(
        sources=metadata.get("sources", []),
        store=store,
        quality_config=metadata,
    )
    return json.dumps({"ok": True, "quality_report": report}, ensure_ascii=False, default=str)


def handle_sql(*, db_path: Path, query: str) -> str:
    sql = query.strip()
    if not _is_read_only_query(sql):
        return json.dumps(
            {
                "ok": False,
                "error": "Only SELECT/WITH/EXPLAIN queries are allowed.",
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

    return json.dumps(
        {"ok": True, "columns": columns, "rows": rows}, ensure_ascii=False, default=str
    )


def handle_price_watch(
    *,
    db_path: Path | None = None,
    region: str | None = None,
    min_price: float | None = None,
    max_price: float | None = None,
    threshold: float | None = None,
    limit: int = 20,
) -> str:
    _ = threshold
    if db_path is None:
        return "Not available in template project"

    with duckdb.connect(str(db_path), read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT e.entity_value AS complex_name, u.district, u.price, u.area, NULL AS floor,
                   CAST(u.published_at AS VARCHAR) AS deal_date,
                    u.title, u.url
            FROM urls u
            LEFT JOIN url_entities e
              ON e.url = u.url AND e.entity_type = 'complex'
            WHERE u.price IS NOT NULL
              AND (?1 IS NULL OR u.district LIKE '%' || ?1 || '%')
              AND (?2 IS NULL OR u.price >= ?2)
              AND (?3 IS NULL OR u.price <= ?3)
            ORDER BY u.published_at DESC
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
    rows = _top_trends_rows(
        db_path=db_path,
        entity_type=entity_type,
        days=days,
        limit=limit,
    )

    payload = {
        "ok": True,
        "entity_type": entity_type,
        "days": days,
        "limit": limit,
        "results": [{"entity_value": row[0], "mention_count": row[1]} for row in rows],
    }
    return json.dumps(payload, ensure_ascii=False, default=str)


def _is_read_only_query(query: str) -> bool:
    if not query or not _READ_ONLY_SQL.match(query):
        return False
    if ";" in query.rstrip(";"):
        return False
    return True


def _filter_results_by_days(
    *, db_path: Path, results: list[SearchResult], days: int
) -> list[SearchResult]:
    if not results:
        return []

    links = [result.link for result in results]
    placeholders = ", ".join("?" for _ in links)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        try:
            latest_row = conn.execute("SELECT MAX(published_at) FROM urls").fetchone()
            latest_published = latest_row[0] if latest_row else None
            if latest_published is None or not isinstance(latest_published, datetime):
                return []
            cutoff = latest_published - timedelta(days=days)
            rows = conn.execute(
                f"""
                SELECT url
                FROM urls
                WHERE published_at >= ? AND url IN ({placeholders})
                """,
                [cutoff, *links],
            ).fetchall()
        except duckdb.Error:
            latest_row = conn.execute("SELECT MAX(collected_at) FROM articles").fetchone()
            latest_collected = latest_row[0] if latest_row else None
            if latest_collected is None or not isinstance(latest_collected, datetime):
                return []
            cutoff = latest_collected - timedelta(days=days)
            rows = conn.execute(
                f"""
                SELECT link
                FROM articles
                WHERE collected_at >= ? AND link IN ({placeholders})
                """,
                [cutoff, *links],
            ).fetchall()
    allowed = {str(cast(tuple[object, ...], row)[0]) for row in rows}
    return [result for result in results if result.link in allowed]


def _recent_updates_rows(*, db_path: Path, days: int, limit: int) -> list[tuple[object, ...]]:
    cutoff = datetime.now(tz=UTC) - timedelta(days=days)
    with duckdb.connect(str(db_path), read_only=True) as conn:
        try:
            return conn.execute(
                """
                SELECT url, title, source_id, published_at, region, property_type, price, area,
                       verification_state, verification_role, merge_policy, event_model
                FROM urls
                WHERE published_at >= ?
                ORDER BY published_at DESC
                LIMIT ?
                """,
                [cutoff, limit],
            ).fetchall()
        except duckdb.Error:
            try:
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
            except duckdb.Error:
                rows = conn.execute(
                    """
                    SELECT link, title, source, collected_at, NULL, NULL, NULL, NULL
                    FROM articles
                    WHERE collected_at >= ?
                    ORDER BY collected_at DESC
                    LIMIT ?
                    """,
                    [cutoff, limit],
                ).fetchall()

            return [tuple(row) + (None, None, None, None) for row in rows]


def _top_trends_rows(
    *,
    db_path: Path,
    entity_type: str,
    days: int,
    limit: int,
) -> list[tuple[object, ...]]:
    with duckdb.connect(str(db_path), read_only=True) as conn:
        try:
            store = GraphStore(db_path)
            return get_trending_entities(store, entity_type=entity_type, limit=limit)
        except duckdb.Error:
            cutoff = datetime.now(tz=UTC) - timedelta(days=days)
            rows = conn.execute(
                """
                SELECT entities_json
                FROM articles
                WHERE collected_at >= ? AND entities_json IS NOT NULL AND entities_json != ''
                """,
                [cutoff],
            ).fetchall()

    counts: dict[str, int] = {}
    target_entity_type = entity_type.strip().lower()
    for row in rows:
        raw = cast(tuple[object, ...], row)[0]
        if not isinstance(raw, str) or not raw:
            continue
        try:
            payload = cast(object, json.loads(raw))
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        for key, values in cast(dict[object, object], payload).items():
            key_name = str(key).strip().lower()
            if key_name != target_entity_type:
                continue
            if isinstance(values, list):
                for value in values:
                    value_name = str(value).strip()
                    if value_name:
                        counts[value_name] = counts.get(value_name, 0) + 1

    return sorted(counts.items(), key=lambda item: item[1], reverse=True)[:limit]
