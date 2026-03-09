from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from collectors.base import RawItem
from graph.graph_store import GraphStore
from reporters.html_reporter import HtmlReporter


@pytest.fixture
def tmp_store(tmp_path: Path) -> GraphStore:
    return GraphStore(db_path=tmp_path / "reporter_test.duckdb")


@pytest.fixture
def sample_region_items() -> list[RawItem]:
    published_at = datetime(2026, 3, 1, tzinfo=timezone.utc)
    return [
        RawItem(
            url="https://example.com/region/seoul-1",
            title="Seoul market update",
            summary="Gangnam district apartment demand increases.",
            source_id="rss_news",
            published_at=published_at,
            region="서울",
            property_type=None,
            price=None,
            area=None,
            raw_data={"district": "강남구"},
        ),
        RawItem(
            url="https://example.com/region/gyeonggi-1",
            title="Gyeonggi market update",
            summary="Bundang district transaction volume rises.",
            source_id="rss_news",
            published_at=published_at,
            region="경기",
            property_type=None,
            price=None,
            area=None,
            raw_data={"district": "분당구"},
        ),
        RawItem(
            url="https://example.com/region/busan-1",
            title="Busan market update",
            summary="Busan apartment supply outlook.",
            source_id="rss_news",
            published_at=published_at,
            region="부산",
            property_type=None,
            price=None,
            area=None,
            raw_data={"district": "해운대구"},
        ),
    ]


def test_get_sido_distribution_from_region_nodes(
    tmp_store: GraphStore,
    sample_region_items: list[RawItem],
) -> None:
    tmp_store.add_items(sample_region_items)
    tmp_store.add_entities(sample_region_items[0].url, {"district": ["강남구"]})
    tmp_store.add_entities(sample_region_items[1].url, {"district": ["분당구"]})

    reporter = HtmlReporter()
    distribution = reporter._get_sido_distribution(tmp_store)

    counts = {item["sido"]: item["count"] for item in distribution}
    assert counts["서울"] == 1
    assert counts["경기"] == 1


def test_get_sido_distribution_falls_back_to_urls(
    tmp_store: GraphStore,
    sample_region_items: list[RawItem],
) -> None:
    tmp_store.add_items(sample_region_items)

    reporter = HtmlReporter()
    distribution = reporter._get_sido_distribution(tmp_store)

    counts = {item["sido"]: item["count"] for item in distribution}
    assert counts["서울"] == 1
    assert counts["경기"] == 1
    assert counts["부산"] == 1


def test_render_region_distribution_returns_table_when_geojson_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reporter = HtmlReporter()

    def _raise_geojson_error() -> dict[str, object]:
        raise RuntimeError("geojson unavailable")

    monkeypatch.setattr(reporter, "_load_korea_geojson", _raise_geojson_error)

    html, used_fallback = reporter._render_region_distribution(
        [
            {
                "sido": "서울",
                "sido_full": "서울특별시",
                "count": 5,
            }
        ]
    )

    assert used_fallback is True
    assert "<table" in html
    assert "서울" in html
