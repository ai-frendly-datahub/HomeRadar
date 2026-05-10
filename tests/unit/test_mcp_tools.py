from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

from collectors.base import RawItem
from graph.graph_store import GraphStore
from homeradar.search_index import SearchIndex


def _init_articles_table(db_path: Path) -> None:
    conn = duckdb.connect(str(db_path))
    try:
        _ = conn.execute(
            """
            CREATE TABLE articles (
                id BIGINT PRIMARY KEY,
                category TEXT NOT NULL,
                source TEXT NOT NULL,
                title TEXT NOT NULL,
                link TEXT NOT NULL UNIQUE,
                summary TEXT,
                published TIMESTAMP,
                collected_at TIMESTAMP NOT NULL,
                entities_json TEXT
            )
            """
        )
    finally:
        conn.close()


def _seed_article(
    *,
    db_path: Path,
    article_id: int,
    title: str,
    link: str,
    collected_at: datetime,
    entities: dict[str, list[str]] | None = None,
) -> None:
    conn = duckdb.connect(str(db_path))
    try:
        _ = conn.execute(
            """
            INSERT INTO articles (id, category, source, title, link, summary, published, collected_at, entities_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                article_id,
                "coffee",
                "Test Source",
                title,
                link,
                "summary",
                None,
                collected_at,
                json.dumps(entities or {}, ensure_ascii=False),
            ],
        )
    finally:
        conn.close()


def test_handle_search(tmp_path: Path) -> None:
    from mcp_server.tools import handle_search

    db_path = tmp_path / "radar.duckdb"
    search_db_path = tmp_path / "search.db"
    _init_articles_table(db_path)

    now = datetime.now(UTC)
    recent_link = "https://example.com/recent"
    old_link = "https://example.com/old"

    _seed_article(
        db_path=db_path,
        article_id=1,
        title="Recent coffee demand",
        link=recent_link,
        collected_at=now - timedelta(days=2),
    )
    _seed_article(
        db_path=db_path,
        article_id=2,
        title="Old coffee demand",
        link=old_link,
        collected_at=now - timedelta(days=20),
    )

    with SearchIndex(search_db_path) as idx:
        idx.upsert(recent_link, "Recent coffee demand", "Demand is rising")
        idx.upsert(old_link, "Old coffee demand", "Demand was low")

    output = handle_search(
        search_db_path=search_db_path,
        db_path=db_path,
        query="last 7 days coffee",
        limit=10,
    )

    assert "Recent coffee demand" in output
    assert "Old coffee demand" not in output


def test_handle_recent_updates(tmp_path: Path) -> None:
    from mcp_server.tools import handle_recent_updates

    db_path = tmp_path / "radar.duckdb"
    _init_articles_table(db_path)
    now = datetime.now(UTC)

    _seed_article(
        db_path=db_path,
        article_id=1,
        title="Most recent",
        link="https://example.com/1",
        collected_at=now - timedelta(hours=1),
    )
    _seed_article(
        db_path=db_path,
        article_id=2,
        title="Older",
        link="https://example.com/2",
        collected_at=now - timedelta(days=2),
    )

    output = handle_recent_updates(db_path=db_path, days=1, limit=10)

    assert "Most recent" in output
    assert "Older" not in output


def test_handle_recent_updates_includes_home_verification_fields(tmp_path: Path) -> None:
    from mcp_server.tools import handle_recent_updates

    db_path = tmp_path / "homeradar.duckdb"
    store = GraphStore(db_path)
    store.add_items(
        [
            RawItem(
                url="https://example.com/molit/transaction/1",
                title="Official transaction update",
                summary="MOLIT transaction record.",
                source_id="molit_apt_transaction",
                published_at=datetime.now(UTC) - timedelta(hours=1),
                region="서울",
                raw_data={
                    "district": "강남구",
                    "home_quality": {
                        "verification_state": "official_primary",
                        "verification_role": "official_primary_transaction",
                        "merge_policy": "authoritative_source",
                        "event_model": "transaction_price",
                    },
                },
            )
        ]
    )

    output = handle_recent_updates(db_path=db_path, days=1, limit=10)
    payload = json.loads(output)

    assert payload["results"][0]["verification_state"] == "official_primary"
    assert payload["results"][0]["verification_role"] == "official_primary_transaction"
    assert payload["results"][0]["merge_policy"] == "authoritative_source"
    assert payload["results"][0]["event_model"] == "transaction_price"


def test_handle_quality_report_returns_freshness_and_verification_summary(
    tmp_path: Path, monkeypatch
) -> None:
    from mcp_server.tools import handle_quality_report

    monkeypatch.setenv("MOLIT_SERVICE_KEY", "test-key")
    db_path = tmp_path / "homeradar.duckdb"
    store = GraphStore(db_path)
    store.add_items(
        [
            RawItem(
                url="https://example.com/molit/transaction/2",
                title="Official transaction update",
                summary="MOLIT transaction record.",
                source_id="molit_apt_transaction",
                published_at=datetime.now(UTC) - timedelta(hours=1),
                region="서울",
                raw_data={
                    "home_quality": {
                        "verification_state": "official_primary",
                        "verification_role": "official_primary_transaction",
                        "merge_policy": "authoritative_source",
                        "event_model": "transaction_price",
                    },
                },
            ),
            RawItem(
                url="https://example.com/market/transaction/1",
                title="Market article mentions apartment transaction price",
                summary="Market corroboration needs an official primary transaction source.",
                source_id="market_metric",
                published_at=datetime.now(UTC) - timedelta(hours=2),
                region="서울",
                raw_data={
                    "home_quality": {
                        "verification_state": "market_corroboration_requires_official_source",
                        "verification_role": "market_corroboration",
                        "merge_policy": "cannot_override_official_transaction",
                        "event_model": "market_context",
                    },
                },
            ),
        ]
    )
    sources_path = tmp_path / "sources.yaml"
    sources_path.write_text(
        """
data_quality:
  freshness_sla:
    molit_apt_transaction:
      max_age_days: 2
sources:
  - id: molit_apt_transaction
    name: MOLIT Transaction
    type: api
    enabled: true
    freshness_sla_days: 2
    event_model: transaction_price
    verification_role: official_primary_transaction
  - id: market_metric
    name: Market Metric
    type: rss
    enabled: true
    freshness_sla_days: 2
    event_model: market_context
    verification_role: market_corroboration
""",
        encoding="utf-8",
    )

    output = handle_quality_report(db_path=db_path, sources_path=sources_path)
    payload = json.loads(output)

    assert payload["ok"] is True
    report = payload["quality_report"]
    assert report["summary"]["fresh_sources"] == 2
    assert report["summary"]["daily_review_item_count"] == 1
    assert report["verification_states"] == {
        "market_corroboration_requires_official_source": 1,
        "official_primary": 1,
    }
    assert report["daily_review_items"][0]["reason"] == (
        "home_verification_requires_official_primary"
    )


def test_handle_sql_select(tmp_path: Path) -> None:
    from mcp_server.tools import handle_sql

    db_path = tmp_path / "radar.duckdb"
    _init_articles_table(db_path)

    output = handle_sql(db_path=db_path, query="SELECT COUNT(*) AS total FROM articles")

    assert "total" in output
    assert "0" in output


def test_handle_sql_blocked(tmp_path: Path) -> None:
    from mcp_server.tools import handle_sql

    db_path = tmp_path / "radar.duckdb"
    _init_articles_table(db_path)

    output = handle_sql(db_path=db_path, query="DROP TABLE articles")

    assert "Only SELECT/WITH/EXPLAIN queries are allowed" in output


def test_handle_top_trends(tmp_path: Path) -> None:
    from mcp_server.tools import handle_top_trends

    db_path = tmp_path / "radar.duckdb"
    _init_articles_table(db_path)
    now = datetime.now(UTC)

    _seed_article(
        db_path=db_path,
        article_id=1,
        title="a",
        link="https://example.com/a",
        collected_at=now - timedelta(days=1),
        entities={"district": ["강남구", "서초구"], "complex": ["래미안"]},
    )
    _seed_article(
        db_path=db_path,
        article_id=2,
        title="b",
        link="https://example.com/b",
        collected_at=now - timedelta(days=1),
        entities={"district": ["강남구"]},
    )

    output = handle_top_trends(db_path=db_path, days=7, limit=10)
    payload = json.loads(output)

    assert payload["results"] == [
        {"entity_value": "강남구", "mention_count": 2},
        {"entity_value": "서초구", "mention_count": 1},
    ]
    assert "래미안" not in output


def test_handle_price_watch_stub() -> None:
    from mcp_server.tools import handle_price_watch

    output = handle_price_watch(threshold=10.0)

    assert "Not available in template project" in output
