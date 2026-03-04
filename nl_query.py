from __future__ import annotations

import re
from dataclasses import dataclass


DEFAULT_LIMIT = 20


@dataclass(frozen=True)
class ParsedQuery:
    original_query: str
    search_text: str
    days: int | None
    limit: int


def parse_query(query: str) -> ParsedQuery:
    normalized = " ".join(query.strip().split())
    working = normalized

    days, working = _extract_days(working)
    limit, working = _extract_limit(working)

    return ParsedQuery(
        original_query=normalized,
        search_text=" ".join(working.split()),
        days=days,
        limit=limit,
    )


def _extract_days(text: str) -> tuple[int | None, str]:
    korean_match = re.search(r"(?:최근|지난)\s*(\d+)\s*(일|주|개월)", text)
    if korean_match:
        amount = int(korean_match.group(1))
        unit = korean_match.group(2)
        multiplier = 1 if unit == "일" else 7 if unit == "주" else 30
        return amount * multiplier, _remove_match(text, korean_match)

    english_match = re.search(
        r"\blast\s*(\d+)\s*(day|days|week|weeks|month|months)\b",
        text,
        re.IGNORECASE,
    )
    if english_match:
        amount = int(english_match.group(1))
        unit = english_match.group(2).lower()
        multiplier = 1 if unit in {"day", "days"} else 7 if unit in {"week", "weeks"} else 30
        return amount * multiplier, _remove_match(text, english_match)

    return None, text


def _extract_limit(text: str) -> tuple[int, str]:
    match = re.search(r"(?:\b(?:top|limit)\s*(\d+)\b|(\d+)\s*개)", text, re.IGNORECASE)
    if not match:
        return DEFAULT_LIMIT, text

    value = int(match.group(1) or match.group(2))
    return value, _remove_match(text, match)


def _remove_match(text: str, match: re.Match[str]) -> str:
    return f"{text[:match.start()]} {text[match.end():]}"
