from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from collectors.base import RawItem
from graph.graph_queries import search_by_keyword
from graph.graph_store import GraphStore
from graph.search_index import SearchIndex


@pytest.mark.unit
def test_search_index_upsert_and_search(tmp_path: Path) -> None:
    index = SearchIndex(tmp_path / "search.db")
    index.upsert(
        "https://example.com/listing-1",
        "강남구 래미안",
        "서울 강남구 아파트 거래 상승",
    )

    results = index.search("래미안", limit=5)

    assert len(results) == 1
    assert results[0].link == "https://example.com/listing-1"
    assert results[0].title == "강남구 래미안"


@pytest.mark.unit
def test_search_index_upsert_replaces_previous_document(tmp_path: Path) -> None:
    index = SearchIndex(tmp_path / "search.db")
    link = "https://example.com/listing-2"

    index.upsert(link, "송파구 리센츠", "송파 거래")
    index.upsert(link, "송파구 리센츠", "송파 거래 급증")

    results = index.search("급증", limit=5)
    assert len(results) == 1
    assert results[0].body == "송파 거래 급증"


@pytest.mark.unit
def test_search_by_keyword_uses_fts_index(tmp_path: Path) -> None:
    store = GraphStore(tmp_path / "homeradar.duckdb")
    item = RawItem(
        url="https://example.com/article-1",
        title="테스트 기사",
        summary="요약",
        source_id="rss_source",
        published_at=datetime(2026, 3, 4, 11, 0, 0, tzinfo=UTC),
        region="서울",
        property_type="아파트",
    )
    store.add_items([item])

    search_db = tmp_path / "search.db"
    index = SearchIndex(search_db)
    index.upsert(item.url, item.title, "강남 재건축 이슈")

    rows = search_by_keyword(store, "재건축", limit=10, search_db_path=search_db)

    assert len(rows) == 1
    assert rows[0]["url"] == item.url
    assert rows[0]["title"] == item.title
