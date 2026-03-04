from __future__ import annotations

import pytest

from nl_query import parse_query


@pytest.mark.unit
def test_parse_query_extracts_korean_days_and_count() -> None:
    parsed = parse_query("최근 7일 강남 아파트 5개")

    assert parsed.days == 7
    assert parsed.limit == 5
    assert parsed.search_text == "강남 아파트"


@pytest.mark.unit
def test_parse_query_extracts_english_days_and_limit() -> None:
    parsed = parse_query("last 2 weeks 재건축 top 8")

    assert parsed.days == 14
    assert parsed.limit == 8
    assert parsed.search_text == "재건축"


@pytest.mark.unit
def test_parse_query_defaults_limit_when_not_provided() -> None:
    parsed = parse_query("송파 실거래")

    assert parsed.days is None
    assert parsed.limit == 20
    assert parsed.search_text == "송파 실거래"
