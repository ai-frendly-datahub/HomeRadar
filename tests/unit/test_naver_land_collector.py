"""Unit tests for NaverLandCollector."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from collectors.naver_land_collector import NaverLandCollector


@pytest.mark.unit
def test_naver_land_collector_initializes_with_config() -> None:
    """NaverLandCollector는 설정으로 초기화된다"""
    config = {
        "base_url": "https://land.naver.com",
        "search_url": "https://land.naver.com/search/result",
        "timeout": 15,
        "max_items": 30,
        "max_pages": 2,
        "request_delay_min": 1.0,
        "request_delay_max": 3.0,
    }

    collector = NaverLandCollector("naver_land_test", config)

    assert collector.source_id == "naver_land_test"
    assert collector.base_url == "https://land.naver.com"
    assert collector.timeout == 15
    assert collector.max_items == 30
    assert collector.max_pages == 2
    assert collector.request_delay_min == 1.0
    assert collector.request_delay_max == 3.0


@pytest.mark.unit
def test_naver_land_collector_uses_default_values() -> None:
    """NaverLandCollector는 기본값을 사용한다"""
    config: dict[str, Any] = {}

    collector = NaverLandCollector("naver_land_main", config)

    assert collector.base_url == "https://land.naver.com"
    assert "land.naver.com" in collector.search_url
    assert collector.timeout == 30
    assert collector.max_items == 50
    assert collector.max_pages == 3
    assert collector.request_delay_min == 2.0
    assert collector.request_delay_max == 5.0


@pytest.mark.unit
def test_naver_land_collector_parses_price() -> None:
    """NaverLandCollector는 가격을 파싱한다"""
    config: dict[str, Any] = {}
    collector = NaverLandCollector("naver_land_test", config)

    # 억 단위만
    assert collector._parse_price("5억원") == pytest.approx(500_000_000, rel=0.01)

    # 만 단위
    assert collector._parse_price("월 50만원") == pytest.approx(500_000, rel=0.01)

    # 직접 숫자
    assert collector._parse_price("1,000,000") == 1_000_000

    # 없는 값
    assert collector._parse_price("") is None


@pytest.mark.unit
def test_naver_land_collector_parses_area() -> None:
    """NaverLandCollector는 면적을 파싱한다"""
    config: dict[str, Any] = {}
    collector = NaverLandCollector("naver_land_test", config)

    # 평 단위
    assert collector._parse_area("25.5평") == pytest.approx(84.3775, rel=0.01)

    # 제곱미터
    assert collector._parse_area("84.5㎡") == 84.5
    assert collector._parse_area("84.5m²") == 84.5

    # 없는 값
    assert collector._parse_area("") is None


@pytest.mark.unit
@patch("collectors.naver_land_collector.requests.get")
def test_naver_land_collector_uses_random_user_agent(mock_get: MagicMock) -> None:
    """NaverLandCollector는 랜덤 User-Agent를 사용한다"""
    mock_response = MagicMock()
    mock_response.text = "<html></html>"
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    config: dict[str, Any] = {}
    collector = NaverLandCollector("naver_land_test", config)

    collector._fetch_html("https://land.naver.com/search/result?page=1")

    # User-Agent가 설정되었는지 확인
    call_args = mock_get.call_args
    headers = call_args.kwargs.get("headers", {})
    user_agent = headers.get("User-Agent", "")

    assert user_agent in collector.USER_AGENTS


@pytest.mark.unit
@patch("collectors.naver_land_collector.requests.get")
def test_naver_land_collector_retries_on_failure(mock_get: MagicMock) -> None:
    """NaverLandCollector는 실패 시 재시도한다"""
    mock_get.side_effect = Exception("Network error")

    config: dict[str, Any] = {}
    collector = NaverLandCollector("naver_land_test", config)

    with pytest.raises(Exception):  # noqa: B017
        collector._fetch_html("https://land.naver.com/search/result?page=1")


@pytest.mark.unit
def test_naver_land_collector_creates_raw_item() -> None:
    """NaverLandCollector는 RawItem을 생성한다"""
    from bs4 import BeautifulSoup

    config: dict[str, Any] = {}
    collector = NaverLandCollector("naver_land_test", config)

    html = """
    <div class="item_list">
        <div class="item">
            <a class="item_link" href="/search/result?id=123">
                <span class="item_title">강남 아파트</span>
            </a>
            <span class="item_price">5억원</span>
            <span class="item_area">84.5㎡</span>
            <span class="item_region">서울 강남구</span>
            <span class="item_type">아파트</span>
            <span class="item_desc">신축 아파트</span>
        </div>
    </div>
    """

    soup = BeautifulSoup(html, "html.parser")
    prop_elem = soup.select_one("div.item")

    item = collector._parse_property(prop_elem)

    assert item is not None
    assert item.title == "강남 아파트"
    assert item.region == "서울 강남구"
    assert item.property_type == "아파트"
    assert item.price == pytest.approx(500_000_000, rel=0.01)
    assert item.area == 84.5
