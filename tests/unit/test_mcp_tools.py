from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from collectors.base import RawItem
from graph.graph_store import GraphStore
from graph.search_index import SearchIndex
from mcp_server.tools import (
    handle_price_watch,
    handle_recent_updates,
    handle_search,
    handle_sql,
    handle_top_trends,
)


def _seed_homeradar_db(db_path: Path) -> GraphStore:
    store = GraphStore(db_path)
    item = RawItem(
        url="https://example.com/news-1",
        title="강남구 아파트 거래 증가",
        summary="실거래가 상승",
        source_id="molit_apt_transaction",
        published_at=datetime(2026, 3, 4, 10, 0, 0),
        region="강남구",
        property_type="아파트",
        price=150000.0,
        area=84.9,
        raw_data={"district": "강남구"},
    )
    store.add_items([item])
    store.add_entities(
        item.url, {"district": ["강남구"], "complex": ["래미안"], "project": ["재건축"]}
    )

    return store


@pytest.mark.unit
def test_handle_search_parses_query_and_returns_fts_results(tmp_path: Path) -> None:
    db_path = tmp_path / "homeradar.duckdb"
    _seed_homeradar_db(db_path)

    search_db = tmp_path / "search.db"
    index = SearchIndex(search_db)
    index.upsert("https://example.com/news-1", "강남구 아파트 거래 증가", "실거래가 상승")

    result = handle_search(
        search_db_path=search_db,
        db_path=db_path,
        query="최근 7일 강남구 5개",
        limit=20,
    )
    payload = json.loads(result)

    assert payload["ok"] is True
    assert payload["days"] == 7
    assert payload["limit"] == 5
    assert payload["results"][0]["url"] == "https://example.com/news-1"


@pytest.mark.unit
def test_handle_recent_updates_returns_latest_urls(tmp_path: Path) -> None:
    db_path = tmp_path / "homeradar.duckdb"
    _seed_homeradar_db(db_path)

    payload = json.loads(handle_recent_updates(db_path=db_path, days=30, limit=10))

    assert payload["ok"] is True
    assert payload["results"]
    assert payload["results"][0]["url"] == "https://example.com/news-1"


@pytest.mark.unit
def test_handle_sql_allows_select_only(tmp_path: Path) -> None:
    db_path = tmp_path / "homeradar.duckdb"
    _seed_homeradar_db(db_path)

    blocked = json.loads(handle_sql(db_path=db_path, query="DELETE FROM urls"))
    allowed = json.loads(handle_sql(db_path=db_path, query="SELECT COUNT(*) AS cnt FROM urls"))

    assert blocked["ok"] is False
    assert allowed["ok"] is True
    assert allowed["rows"][0][0] == 1


@pytest.mark.unit
def test_handle_price_watch_filters_by_region_and_price(tmp_path: Path) -> None:
    db_path = tmp_path / "homeradar.duckdb"
    _seed_homeradar_db(db_path)

    payload = json.loads(
        handle_price_watch(
            db_path=db_path,
            region="강남",
            min_price=140000,
            max_price=160000,
            limit=10,
        )
    )

    assert payload["ok"] is True
    assert len(payload["results"]) == 1
    assert payload["results"][0]["complex_name"] == "래미안"


@pytest.mark.unit
def test_handle_top_trends_uses_trending_entities(tmp_path: Path) -> None:
    db_path = tmp_path / "homeradar.duckdb"
    _seed_homeradar_db(db_path)

    payload = json.loads(
        handle_top_trends(db_path=db_path, entity_type="district", days=7, limit=5)
    )

    assert payload["ok"] is True
    assert payload["results"]
    assert payload["results"][0]["entity_value"] == "강남구"
