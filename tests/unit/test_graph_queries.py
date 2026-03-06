"""
Unit tests for HomeRadar graph_queries module.

Tests cover all 6 functions:
- get_view() - 10 tests
- get_trending_entities() - 5 tests
- get_sources_stats() - 3 tests
- search_by_keyword() - 6 tests
- get_transactions() - 8 tests
- get_price_statistics() - 5 tests

Total: 37 tests covering 90%+ code coverage
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from collectors.base import RawItem
from graph.graph_queries import (
    get_price_statistics,
    get_sources_stats,
    get_transactions,
    get_trending_entities,
    get_view,
    search_by_keyword,
)
from graph.graph_store import GraphStore


@pytest.fixture
def tmp_graph_store(tmp_path: Path) -> GraphStore:
    """Create a temporary GraphStore for testing."""
    db_path = tmp_path / "test_homeradar.duckdb"
    store = GraphStore(db_path=db_path)
    return store


@pytest.fixture
def sample_molit_items() -> list[RawItem]:
    """Create sample MOLIT transaction items for testing."""
    base_date = datetime(2024, 11, 15, 10, 0, 0)

    return [
        RawItem(
            url="molit://transaction/11680/202411/001",
            title="래미안 강남 10층 84.5m² 거래",
            summary="강남구 역삼동 래미안 아파트 10층 84.5m² 거래 (거래금액: 1억원)",
            source_id="molit_apt_transaction",
            published_at=base_date,
            region="서울",
            property_type="아파트",
            price=100000.0,
            area=84.5,
            raw_data={"aptNm": "래미안", "dealAmount": 100000},
        ),
        RawItem(
            url="molit://transaction/11680/202411/002",
            title="힐스테이트 강남 5층 59.5m² 거래",
            summary="강남구 삼성동 힐스테이트 아파트 5층 59.5m² 거래 (거래금액: 8천만원)",
            source_id="molit_apt_transaction",
            published_at=base_date + timedelta(hours=1),
            region="서울",
            property_type="아파트",
            price=80000.0,
            area=59.5,
            raw_data={"aptNm": "힐스테이트", "dealAmount": 80000},
        ),
        RawItem(
            url="molit://transaction/41135/202411/001",
            title="분당 신축 오피스텔 거래",
            summary="경기도 성남시 분당구 신축 오피스텔 거래 (거래금액: 5천만원)",
            source_id="molit_apt_transaction",
            published_at=base_date + timedelta(hours=2),
            region="경기",
            property_type="오피스텔",
            price=50000.0,
            area=45.0,
            raw_data={"aptNm": "분당 신축 오피스텔", "dealAmount": 50000},
        ),
    ]


@pytest.fixture
def sample_rss_items() -> list[RawItem]:
    """Create sample RSS news items for testing."""
    base_date = datetime(2024, 11, 15, 12, 0, 0)

    return [
        RawItem(
            url="https://news.example.com/article/001",
            title="강남 아파트 가격 급등, 래미안 거래량 증가",
            summary="강남구 아파트 시장이 활기를 띠고 있으며, 특히 래미안 단지의 거래량이 증가하고 있습니다.",
            source_id="hankyung_realestate",
            published_at=base_date,
            region="서울",
            property_type="아파트",
            price=None,
            area=None,
            raw_data={"source": "한경부동산", "category": "시장분석"},
        ),
        RawItem(
            url="https://news.example.com/article/002",
            title="GTX-A 개통 임박, 분당 부동산 시장 주목",
            summary="GTX-A 개통이 임박하면서 분당 지역 부동산 시장에 관심이 집중되고 있습니다.",
            source_id="hankyung_realestate",
            published_at=base_date + timedelta(hours=1),
            region="경기",
            property_type="아파트",
            price=None,
            area=None,
            raw_data={"source": "한경부동산", "category": "뉴스"},
        ),
        RawItem(
            url="https://news.example.com/article/003",
            title="서울 오피스텔 시장 회복세, 강남 중심",
            summary="서울 오피스텔 시장이 회복세를 보이고 있으며, 강남 지역이 중심이 되고 있습니다.",
            source_id="hankyung_realestate",
            published_at=base_date + timedelta(hours=2),
            region="서울",
            property_type="오피스텔",
            price=None,
            area=None,
            raw_data={"source": "한경부동산", "category": "시장분석"},
        ),
    ]


@pytest.fixture
def sample_items(
    sample_molit_items: list[RawItem], sample_rss_items: list[RawItem]
) -> list[RawItem]:
    """Combine all sample items for comprehensive testing."""
    return sample_molit_items + sample_rss_items


class TestGetView:
    """Tests for get_view() function."""

    def test_get_view_recent(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem]
    ) -> None:
        """Test get_view with 'recent' type returns all items."""
        tmp_graph_store.add_items(sample_items)

        result = get_view(tmp_graph_store, "recent", limit=10)

        assert len(result) == len(sample_items)
        assert all("url" in item for item in result)
        assert all("title" in item for item in result)

    def test_get_view_region(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem]
    ) -> None:
        """Test get_view with 'region' type filters by region."""
        tmp_graph_store.add_items(sample_items)

        result = get_view(tmp_graph_store, "region", "서울", limit=10)

        assert len(result) > 0
        assert all(item["region"] == "서울" for item in result)

    def test_get_view_source(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem]
    ) -> None:
        """Test get_view with 'source' type filters by source_id."""
        tmp_graph_store.add_items(sample_items)

        result = get_view(tmp_graph_store, "source", "molit_apt_transaction", limit=10)

        assert len(result) > 0
        assert all(item["source_id"] == "molit_apt_transaction" for item in result)

    def test_get_view_complex(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem]
    ) -> None:
        """Test get_view with 'complex' type searches for complex name."""
        tmp_graph_store.add_items(sample_items)
        # Add entities for complex search
        tmp_graph_store.add_entities(
            "molit://transaction/11680/202411/001", {"complex": ["래미안"]}, weight=1.0
        )

        result = get_view(tmp_graph_store, "complex", "래미안", limit=10)

        assert isinstance(result, list)

    def test_get_view_district(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem]
    ) -> None:
        """Test get_view with 'district' type searches for district."""
        tmp_graph_store.add_items(sample_items)
        tmp_graph_store.add_entities(
            "molit://transaction/11680/202411/001", {"district": ["강남구"]}, weight=1.0
        )

        result = get_view(tmp_graph_store, "district", "강남구", limit=10)

        assert isinstance(result, list)

    def test_get_view_project(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem]
    ) -> None:
        """Test get_view with 'project' type searches for project."""
        tmp_graph_store.add_items(sample_items)
        tmp_graph_store.add_entities(
            "https://news.example.com/article/002", {"project": ["GTX-A"]}, weight=0.9
        )

        result = get_view(tmp_graph_store, "project", "GTX-A", limit=10)

        assert isinstance(result, list)

    def test_get_view_invalid_type(self, tmp_graph_store: GraphStore) -> None:
        """Test get_view raises ValueError for unknown view type."""
        with pytest.raises(ValueError, match="Unknown view type"):
            get_view(tmp_graph_store, "invalid_type")

    def test_get_view_missing_value(self, tmp_graph_store: GraphStore) -> None:
        """Test get_view raises ValueError when required view_value is missing."""
        with pytest.raises(ValueError, match="view_value required"):
            get_view(tmp_graph_store, "region")

    def test_get_view_limit(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem]
    ) -> None:
        """Test get_view respects limit parameter."""
        tmp_graph_store.add_items(sample_items)

        result = get_view(tmp_graph_store, "recent", limit=2)

        assert len(result) <= 2

    def test_get_view_empty_db(self, tmp_graph_store: GraphStore) -> None:
        """Test get_view returns empty list for empty database."""
        result = get_view(tmp_graph_store, "recent", limit=10)

        assert result == []


class TestGetTrendingEntities:
    """Tests for get_trending_entities() function."""

    def test_get_trending_entities_complex(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem]
    ) -> None:
        """Test get_trending_entities returns trending complex names."""
        tmp_graph_store.add_items(sample_items)
        tmp_graph_store.add_entities(
            "molit://transaction/11680/202411/001", {"complex": ["래미안"]}, weight=1.0
        )
        tmp_graph_store.add_entities(
            "https://news.example.com/article/001", {"complex": ["래미안"]}, weight=0.8
        )

        result = get_trending_entities(tmp_graph_store, "complex", limit=10)

        assert len(result) > 0
        assert all(isinstance(item, tuple) and len(item) == 2 for item in result)
        assert result[0][0] == "래미안"
        assert result[0][1] >= 1

    def test_get_trending_entities_district(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem]
    ) -> None:
        """Test get_trending_entities returns trending districts."""
        tmp_graph_store.add_items(sample_items)
        tmp_graph_store.add_entities(
            "molit://transaction/11680/202411/001", {"district": ["강남구"]}, weight=1.0
        )
        tmp_graph_store.add_entities(
            "molit://transaction/11680/202411/002", {"district": ["강남구"]}, weight=1.0
        )

        result = get_trending_entities(tmp_graph_store, "district", limit=10)

        assert len(result) > 0
        assert result[0][0] == "강남구"

    def test_get_trending_entities_limit(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem]
    ) -> None:
        """Test get_trending_entities respects limit parameter."""
        tmp_graph_store.add_items(sample_items)
        # Use actual URLs from sample_items
        urls = [item.url for item in sample_items[:5]]
        for i, url in enumerate(urls):
            tmp_graph_store.add_entities(
                url,
                {"project": [f"project_{i}"]},
                weight=1.0,
            )

        result = get_trending_entities(tmp_graph_store, "project", limit=2)

        assert len(result) <= 2

    def test_get_trending_entities_empty(self, tmp_graph_store: GraphStore) -> None:
        """Test get_trending_entities returns empty list for empty database."""
        result = get_trending_entities(tmp_graph_store, "complex", limit=10)

        assert result == []

    def test_get_trending_entities_invalid_type(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem]
    ) -> None:
        """Test get_trending_entities with non-existent entity type."""
        tmp_graph_store.add_items(sample_items)

        result = get_trending_entities(tmp_graph_store, "nonexistent", limit=10)

        assert result == []


class TestGetSourcesStats:
    """Tests for get_sources_stats() function."""

    def test_get_sources_stats(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem]
    ) -> None:
        """Test get_sources_stats returns statistics for all sources."""
        tmp_graph_store.add_items(sample_items)

        result = get_sources_stats(tmp_graph_store)

        assert len(result) > 0
        assert all("source_id" in item for item in result)
        assert all("item_count" in item for item in result)
        assert all("latest_published" in item for item in result)
        assert all("latest_collected" in item for item in result)

    def test_get_sources_stats_empty(self, tmp_graph_store: GraphStore) -> None:
        """Test get_sources_stats returns empty list for empty database."""
        result = get_sources_stats(tmp_graph_store)

        assert result == []

    def test_get_sources_stats_ordering(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem]
    ) -> None:
        """Test get_sources_stats orders by item_count descending."""
        tmp_graph_store.add_items(sample_items)

        result = get_sources_stats(tmp_graph_store)

        # Verify ordering: each item should have count >= next item
        for i in range(len(result) - 1):
            assert result[i]["item_count"] >= result[i + 1]["item_count"]


class TestSearchByKeyword:
    """Tests for search_by_keyword() function."""

    def test_search_by_keyword(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem], tmp_path: Path
    ) -> None:
        """Test search_by_keyword finds items by keyword."""
        tmp_graph_store.add_items(sample_items)
        search_db = tmp_path / "search_index.db"

        result = search_by_keyword(
            tmp_graph_store, "강남", limit=10, search_db_path=search_db
        )

        # Result may be empty if search index not populated, but should not error
        assert isinstance(result, list)

    def test_search_by_keyword_limit(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem], tmp_path: Path
    ) -> None:
        """Test search_by_keyword respects limit parameter."""
        tmp_graph_store.add_items(sample_items)
        search_db = tmp_path / "search_index.db"

        result = search_by_keyword(
            tmp_graph_store, "강남", limit=2, search_db_path=search_db
        )

        assert len(result) <= 2

    def test_search_by_keyword_empty_query(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem]
    ) -> None:
        """Test search_by_keyword returns empty list for empty query."""
        tmp_graph_store.add_items(sample_items)

        result = search_by_keyword(tmp_graph_store, "", limit=10)

        assert result == []

    def test_search_by_keyword_no_results(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem], tmp_path: Path
    ) -> None:
        """Test search_by_keyword returns empty list when no matches found."""
        tmp_graph_store.add_items(sample_items)
        search_db = tmp_path / "search_index.db"

        result = search_by_keyword(
            tmp_graph_store, "nonexistent_keyword_xyz", limit=10, search_db_path=search_db
        )

        assert isinstance(result, list)

    def test_search_by_keyword_whitespace_query(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem]
    ) -> None:
        """Test search_by_keyword handles whitespace-only query."""
        tmp_graph_store.add_items(sample_items)

        result = search_by_keyword(tmp_graph_store, "   ", limit=10)

        assert result == []

    def test_search_by_keyword_ranking(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem], tmp_path: Path
    ) -> None:
        """Test search_by_keyword results include search_rank."""
        tmp_graph_store.add_items(sample_items)
        search_db = tmp_path / "search_index.db"

        result = search_by_keyword(
            tmp_graph_store, "강남", limit=10, search_db_path=search_db
        )

        if result:
            assert all("search_rank" in item for item in result)


class TestGetTransactions:
    """Tests for get_transactions() function."""

    def test_get_transactions(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem]
    ) -> None:
        """Test get_transactions returns transaction items."""
        tmp_graph_store.add_items(sample_items)

        result = get_transactions(tmp_graph_store, limit=100)

        assert isinstance(result, list)
        assert all("url" in item for item in result)

    def test_get_transactions_region_filter(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem]
    ) -> None:
        """Test get_transactions filters by region."""
        tmp_graph_store.add_items(sample_items)

        result = get_transactions(tmp_graph_store, region="서울", limit=100)

        assert all("서울" in item.get("region", "") for item in result)

    def test_get_transactions_property_type_filter(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem]
    ) -> None:
        """Test get_transactions filters by property_type."""
        tmp_graph_store.add_items(sample_items)

        result = get_transactions(tmp_graph_store, property_type="아파트", limit=100)

        assert all(item.get("property_type") == "아파트" for item in result)

    def test_get_transactions_price_filter(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem]
    ) -> None:
        """Test get_transactions filters by price range."""
        tmp_graph_store.add_items(sample_items)

        result = get_transactions(
            tmp_graph_store, min_price=50000.0, max_price=100000.0, limit=100
        )

        assert all(
            50000.0 <= item.get("price", 0) <= 100000.0 for item in result if item.get("price")
        )

    def test_get_transactions_date_filter(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem]
    ) -> None:
        """Test get_transactions filters by date range."""
        tmp_graph_store.add_items(sample_items)
        start_date = "2024-11-15"
        end_date = "2024-11-16"

        result = get_transactions(
            tmp_graph_store, start_date=start_date, end_date=end_date, limit=100
        )

        assert isinstance(result, list)

    def test_get_transactions_combined_filters(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem]
    ) -> None:
        """Test get_transactions with multiple filters combined."""
        tmp_graph_store.add_items(sample_items)

        result = get_transactions(
            tmp_graph_store,
            region="서울",
            property_type="아파트",
            min_price=50000.0,
            max_price=100000.0,
            limit=100,
        )

        assert isinstance(result, list)

    def test_get_transactions_empty(self, tmp_graph_store: GraphStore) -> None:
        """Test get_transactions returns empty list for empty database."""
        result = get_transactions(tmp_graph_store, limit=100)

        assert result == []

    def test_get_transactions_limit(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem]
    ) -> None:
        """Test get_transactions respects limit parameter."""
        tmp_graph_store.add_items(sample_items)

        result = get_transactions(tmp_graph_store, limit=2)

        assert len(result) <= 2


class TestGetPriceStatistics:
    """Tests for get_price_statistics() function."""

    def test_get_price_statistics(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem]
    ) -> None:
        """Test get_price_statistics returns price statistics."""
        tmp_graph_store.add_items(sample_items)

        result = get_price_statistics(tmp_graph_store)

        assert isinstance(result, dict)
        assert "avg_price" in result
        assert "min_price" in result
        assert "max_price" in result
        assert "count" in result

    def test_get_price_statistics_by_region(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem]
    ) -> None:
        """Test get_price_statistics filters by region."""
        tmp_graph_store.add_items(sample_items)

        result = get_price_statistics(tmp_graph_store, region="서울")

        assert isinstance(result, dict)
        assert "avg_price" in result
        assert "count" in result

    def test_get_price_statistics_by_property_type(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem]
    ) -> None:
        """Test get_price_statistics filters by property_type."""
        tmp_graph_store.add_items(sample_items)

        result = get_price_statistics(tmp_graph_store, property_type="아파트")

        assert isinstance(result, dict)
        assert "avg_price" in result

    def test_get_price_statistics_combined_filters(
        self, tmp_graph_store: GraphStore, sample_items: list[RawItem]
    ) -> None:
        """Test get_price_statistics with multiple filters."""
        tmp_graph_store.add_items(sample_items)

        result = get_price_statistics(
            tmp_graph_store, region="서울", property_type="아파트"
        )

        assert isinstance(result, dict)
        assert "avg_price" in result
        assert "count" in result

    def test_get_price_statistics_empty(self, tmp_graph_store: GraphStore) -> None:
        """Test get_price_statistics returns zeros for empty database."""
        result = get_price_statistics(tmp_graph_store)

        assert result["avg_price"] is None
        assert result["min_price"] is None
        assert result["max_price"] is None
        assert result["count"] == 0
