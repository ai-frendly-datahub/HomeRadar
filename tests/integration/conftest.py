"""
Fixtures for HomeRadar integration tests.

Provides:
- tmp_graph_store: Temporary DuckDB instance for testing
- sample_items: Test data (MOLIT transactions, RSS articles)
- sample_entities: Entity extraction test data
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest

from collectors.base import RawItem
from graph.graph_store import GraphStore


@pytest.fixture
def tmp_graph_store(tmp_path: Path) -> GraphStore:
    """
    Create a temporary GraphStore for testing.

    Args:
        tmp_path: pytest temporary directory

    Returns:
        GraphStore instance with temporary DuckDB
    """
    db_path = tmp_path / "test_homeradar.duckdb"
    store = GraphStore(db_path=db_path)
    return store


@pytest.fixture
def sample_molit_items() -> list[RawItem]:
    """
    Create sample MOLIT transaction items for testing.

    Returns:
        List of RawItem objects representing real estate transactions
    """
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
            raw_data={
                "aptNm": "래미안",
                "dealAmount": 100000,
                "excluUseAr": 84.5,
                "floor": 10,
                "buildYear": 2010,
                "umdNm": "역삼동",
                "jibun": "123-45",
                "sggCd": "11680",
                "lawd_cd": "11680",
                "deal_ymd": "202411",
            },
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
            raw_data={
                "aptNm": "힐스테이트",
                "dealAmount": 80000,
                "excluUseAr": 59.5,
                "floor": 5,
                "buildYear": 2015,
                "umdNm": "삼성동",
                "jibun": "456-78",
                "sggCd": "11680",
                "lawd_cd": "11680",
                "deal_ymd": "202411",
            },
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
            raw_data={
                "aptNm": "분당 신축 오피스텔",
                "dealAmount": 50000,
                "excluUseAr": 45.0,
                "floor": 15,
                "buildYear": 2024,
                "umdNm": "분당동",
                "jibun": "789-01",
                "sggCd": "41135",
                "lawd_cd": "41135",
                "deal_ymd": "202411",
            },
        ),
    ]


@pytest.fixture
def sample_rss_items() -> list[RawItem]:
    """
    Create sample RSS news items for testing.

    Returns:
        List of RawItem objects representing real estate news
    """
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
            raw_data={
                "source": "한경부동산",
                "category": "시장분석",
                "keywords": ["강남", "아파트", "래미안"],
            },
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
            raw_data={
                "source": "한경부동산",
                "category": "뉴스",
                "keywords": ["GTX", "분당", "교통"],
            },
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
            raw_data={
                "source": "한경부동산",
                "category": "시장분석",
                "keywords": ["오피스텔", "강남", "회복"],
            },
        ),
    ]


@pytest.fixture
def sample_items(
    sample_molit_items: list[RawItem], sample_rss_items: list[RawItem]
) -> list[RawItem]:
    """
    Combine all sample items for comprehensive testing.

    Args:
        sample_molit_items: MOLIT transaction items
        sample_rss_items: RSS news items

    Returns:
        Combined list of all sample items
    """
    return sample_molit_items + sample_rss_items


@pytest.fixture
def sample_entities() -> dict[str, list[tuple[str, str, float]]]:
    """
    Create sample entity extraction data for testing.

    Returns:
        Dictionary mapping entity types to (url, entity_value, weight) tuples
    """
    return {
        "complex": [
            ("molit://transaction/11680/202411/001", "래미안", 1.0),
            ("molit://transaction/11680/202411/002", "힐스테이트", 1.0),
            ("https://news.example.com/article/001", "래미안", 0.8),
        ],
        "district": [
            ("molit://transaction/11680/202411/001", "강남구", 1.0),
            ("molit://transaction/11680/202411/002", "강남구", 1.0),
            ("molit://transaction/41135/202411/001", "분당구", 1.0),
        ],
        "project": [
            ("https://news.example.com/article/002", "GTX-A", 0.9),
            ("https://news.example.com/article/002", "분당", 0.8),
        ],
        "keyword": [
            ("https://news.example.com/article/001", "가격급등", 0.7),
            ("https://news.example.com/article/003", "회복세", 0.6),
        ],
    }
