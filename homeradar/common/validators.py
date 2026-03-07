from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Any, Optional
from urllib.parse import urlparse


def normalize_title(title: str) -> str:
    if not title:
        return ""

    normalized = title.lower()
    normalized = re.sub(r"\s+", " ", normalized).strip()
    normalized = re.sub(r"[^\w\s\-]", "", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def validate_url_format(url: str) -> bool:
    if not url or not isinstance(url, str):
        return False

    try:
        parsed = urlparse(url)
        return bool(parsed.scheme and parsed.netloc)
    except Exception:
        return False


def is_similar_url(url1: str, url2: str, threshold: float = 0.8) -> bool:
    try:
        parsed1 = urlparse(url1)
        parsed2 = urlparse(url2)

        if parsed1.netloc != parsed2.netloc:
            return False

        path1 = parsed1.path
        path2 = parsed2.path

        if path1 == path2:
            return True

        ratio = SequenceMatcher(None, path1, path2).ratio()
        return ratio >= threshold
    except Exception:
        return False


def detect_duplicate_articles(
    title1: str,
    url1: str,
    title2: str,
    url2: str,
    title_threshold: float = 0.85,
    url_threshold: float = 0.8,
) -> bool:
    norm_title1 = normalize_title(title1)
    norm_title2 = normalize_title(title2)

    title_ratio = SequenceMatcher(None, norm_title1, norm_title2).ratio()
    if title_ratio < title_threshold:
        return False

    return is_similar_url(url1, url2, url_threshold)


def _get_value(article: Any, *keys: str) -> Any:
    if isinstance(article, dict):
        for key in keys:
            if key in article:
                return article[key]
        return None

    for key in keys:
        if hasattr(article, key):
            return getattr(article, key)
    return None


def validate_article(article: Any) -> tuple[bool, list[str]]:
    errors: list[str] = []

    title = _get_value(article, "title")
    link = _get_value(article, "link", "url")
    summary = _get_value(article, "summary", "content")
    source = _get_value(article, "source", "source_id", "source_name")
    category = _get_value(article, "category", "content_type", "property_type")

    if not title or not isinstance(title, str):
        errors.append("title is missing or not a string")
    elif len(title.strip()) == 0:
        errors.append("title is empty")

    if not link or not isinstance(link, str):
        errors.append("link is missing or not a string")
    elif not validate_url_format(link):
        errors.append(f"link has invalid URL format: {link}")

    if not summary or not isinstance(summary, str):
        errors.append("summary is missing or not a string")
    elif len(summary.strip()) == 0:
        errors.append("summary is empty")

    if not source or not isinstance(source, str):
        errors.append("source is missing or not a string")

    if not category or not isinstance(category, str):
        errors.append("category is missing or not a string")

    return len(errors) == 0, errors


def validate_price(price: Optional[float]) -> bool:
    if price is None:
        return True
    return 1_000_000 <= price <= 100_000_000_000


def validate_area(area: Optional[float]) -> bool:
    if area is None:
        return True
    return 1.0 <= area <= 10000.0


def validate_location(location: str) -> bool:
    return bool(location and len(location) >= 2)
