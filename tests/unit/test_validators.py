from __future__ import annotations

from datetime import datetime

import pytest

from collectors.base import RawItem
from homeradar.common.validators import (
    detect_duplicate_articles,
    is_similar_url,
    normalize_title,
    validate_area,
    validate_article,
    validate_location,
    validate_price,
    validate_url_format,
)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("raw_title", "expected"),
    [
        ("Breaking News", "breaking news"),
        ("  Breaking   News  ", "breaking news"),
        ("Title (Updated)", "title updated"),
        ("A-B Test", "a-b test"),
        ("", ""),
        ("   ", ""),
        ("서울 아파트", "서울 아파트"),
        ("House@#1", "house1"),
    ],
)
def test_normalize_title(raw_title: str, expected: str) -> None:
    assert normalize_title(raw_title) == expected


@pytest.mark.unit
@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://example.com/a", True),
        ("http://example.com/a", True),
        ("ftp://example.com/a", True),
        ("example.com/a", False),
        ("not-a-url", False),
        ("", False),
        (None, False),
    ],
)
def test_validate_url_format(url: str, expected: bool) -> None:
    assert validate_url_format(url) is expected


@pytest.mark.unit
@pytest.mark.parametrize(
    ("url1", "url2", "threshold", "expected"),
    [
        ("https://example.com/a/1", "https://example.com/a/1", 0.8, True),
        ("https://example.com/a/1", "https://example.com/a/1?x=1", 0.8, True),
        ("https://example.com/a/1", "https://other.com/a/1", 0.8, False),
        ("https://example.com/a/1", "https://example.com/a/2", 0.95, False),
        ("https://example.com/a/1", "https://example.com/a/2", 0.5, True),
        ("", "", 0.8, True),
        ("not-url", "https://example.com/a/1", 0.8, False),
    ],
)
def test_is_similar_url(url1: str, url2: str, threshold: float, expected: bool) -> None:
    assert is_similar_url(url1, url2, threshold=threshold) is expected


@pytest.mark.unit
@pytest.mark.parametrize(
    ("title1", "url1", "title2", "url2", "expected"),
    [
        (
            "서울 아파트 급등",
            "https://example.com/a/1",
            "서울 아파트 급등",
            "https://example.com/a/1?x=1",
            True,
        ),
        (
            "서울 아파트 급등",
            "https://example.com/a/1",
            "서울 빌라 급등",
            "https://example.com/a/1?x=1",
            False,
        ),
        (
            "서울 아파트 급등",
            "https://example.com/a/1",
            "서울 아파트 급등",
            "https://other.com/a/1",
            False,
        ),
    ],
)
def test_detect_duplicate_articles(
    title1: str,
    url1: str,
    title2: str,
    url2: str,
    expected: bool,
) -> None:
    assert detect_duplicate_articles(title1, url1, title2, url2) is expected


@pytest.mark.unit
def test_validate_article_with_raw_item() -> None:
    item = RawItem(
        url="https://example.com/1",
        title="서울 아파트",
        summary="요약",
        source_id="rss",
        published_at=datetime.now(),
        region=None,
        property_type="아파트",
        price=None,
        area=None,
    )
    is_valid, errors = validate_article(item)
    assert is_valid is True
    assert errors == []


@pytest.mark.unit
def test_validate_article_with_dict_alias_fields() -> None:
    article = {
        "title": "제목",
        "url": "https://example.com/1",
        "content": "본문",
        "source_name": "rss",
        "content_type": "news",
    }
    is_valid, errors = validate_article(article)
    assert is_valid is True
    assert errors == []


@pytest.mark.unit
@pytest.mark.parametrize(
    "article",
    [
        {"title": "", "url": "https://example.com", "summary": "x", "source": "s", "category": "c"},
        {"title": "t", "url": "bad", "summary": "x", "source": "s", "category": "c"},
        {"title": "t", "url": "https://example.com", "summary": "", "source": "s", "category": "c"},
        {"title": "t", "url": "https://example.com", "summary": "x", "source": "", "category": "c"},
        {"title": "t", "url": "https://example.com", "summary": "x", "source": "s", "category": ""},
    ],
)
def test_validate_article_invalid_cases(article: dict[str, str]) -> None:
    is_valid, errors = validate_article(article)
    assert is_valid is False
    assert errors


@pytest.mark.unit
@pytest.mark.parametrize(
    ("price", "expected"),
    [
        (None, True),
        (1_000_000, True),
        (100_000_000_000, True),
        (999_999, False),
        (100_000_000_001, False),
    ],
)
def test_validate_price(price: int | None, expected: bool) -> None:
    assert validate_price(price) is expected


@pytest.mark.unit
@pytest.mark.parametrize(
    ("area", "expected"),
    [
        (None, True),
        (1.0, True),
        (10_000.0, True),
        (0.9, False),
        (10_000.1, False),
    ],
)
def test_validate_area(area: float | None, expected: bool) -> None:
    assert validate_area(area) is expected


@pytest.mark.unit
@pytest.mark.parametrize(
    ("location", "expected"),
    [
        ("서울", True),
        ("강남구", True),
        ("A", False),
        ("", False),
    ],
)
def test_validate_location(location: str, expected: bool) -> None:
    assert validate_location(location) is expected
