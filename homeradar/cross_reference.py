"""Cross-reference MOLIT / REB / ONBID records into a single property key.

HomeRadar pulls aparment data from three official sources whose schemas
share city + dong (행정동) + complex name but differ in everything else.
This module gives every record a canonical ``property_key`` so a single
aparment complex can be joined across data sources.

The key is intentionally string-based (rather than numeric) so it can be
re-derived from any record without an external mapping table:

    property_key = f"{si_do}|{si_gun_gu}|{dong}|{normalized_complex_name}"

The normalization strips suffixes like "아파트", "단지", "1차", "Ⅰ" and
collapses whitespace. It does NOT promise uniqueness across the entire
country (two unrelated complexes with the same name in the same dong
remain a hand-resolved tie), but does dedupe the noisy 99% case.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Optional

# Suffixes / variants that don't help disambiguate a complex.
_SUFFIX_TOKENS = (
    "아파트",
    "단지",
    "타운",
    "오피스텔",
    "주상복합",
    "the",
)

# Phase / wing markers like 1차 / 2차 / 3단지 / Ⅰ etc.
_PHASE_RE = re.compile(
    r"\s*("
    r"\d+차|\d+단지|\d+\s*동|\d+|\([\w가-힣\s]+\)|[IVXⅠⅡⅢⅣⅤⅥⅦⅧ]+차?"
    r")\s*$"
)
_PUNCT_RE = re.compile(r"[^\w가-힣\s]", re.UNICODE)
_MULTISPACE_RE = re.compile(r"\s+")


def normalize_complex_name(name: str | None) -> str:
    """Return a canonical lower-case representation of a complex name."""
    if not name:
        return ""
    text = unicodedata.normalize("NFKC", name).lower()
    text = _PUNCT_RE.sub(" ", text)
    text = _MULTISPACE_RE.sub(" ", text).strip()
    # Alternate stripping suffix tokens and phase markers until stable, since
    # patterns like "래미안 1차 아파트" need suffix removed first, then phase.
    for _ in range(6):
        changed = False
        for suffix in _SUFFIX_TOKENS:
            if text.endswith(suffix):
                text = text[: -len(suffix)].strip()
                changed = True
        new = _PHASE_RE.sub("", text).strip()
        if new != text:
            text = new
            changed = True
        if not changed:
            break
    text = _MULTISPACE_RE.sub(" ", text).strip()
    return text


def normalize_region(region: str | None) -> str:
    """Map short 시도 forms to their canonical full names."""
    if not region:
        return ""
    aliases = {
        "서울": "서울특별시",
        "서울시": "서울특별시",
        "부산": "부산광역시",
        "대구": "대구광역시",
        "인천": "인천광역시",
        "광주": "광주광역시",
        "대전": "대전광역시",
        "울산": "울산광역시",
        "세종": "세종특별자치시",
        "경기": "경기도",
        "강원": "강원특별자치도",
        "충북": "충청북도",
        "충남": "충청남도",
        "전북": "전북특별자치도",
        "전남": "전라남도",
        "경북": "경상북도",
        "경남": "경상남도",
        "제주": "제주특별자치도",
    }
    return aliases.get(region.strip(), region.strip())


@dataclass(frozen=True)
class PropertyRecord:
    si_do: str
    si_gun_gu: str
    dong: str
    complex_name: str
    source: str  # e.g. "molit_apt_trade" / "reb_subscription" / "onbid_auction"
    raw: dict[str, object] | None = None


def property_key(record: PropertyRecord) -> str:
    return "|".join(
        [
            normalize_region(record.si_do),
            (record.si_gun_gu or "").strip(),
            (record.dong or "").strip(),
            normalize_complex_name(record.complex_name),
        ]
    )


def group_by_property(records: Iterable[PropertyRecord]) -> dict[str, list[PropertyRecord]]:
    """Bucket records by property_key. An empty / unresolved key is dropped."""
    out: dict[str, list[PropertyRecord]] = {}
    for record in records:
        key = property_key(record)
        parts = key.split("|") if key else []
        # Require all four key components to be populated.
        if len(parts) != 4 or not all(parts):
            continue
        out.setdefault(key, []).append(record)
    return out


def cross_reference_sources(
    records: Iterable[PropertyRecord],
) -> dict[str, dict[str, list[PropertyRecord]]]:
    """For each property_key, return ``{source_name: [records, ...]}``.

    Useful for the dashboard: "this complex has MOLIT trade rows + REB
    subscription rows but no ONBID auctions" etc.
    """
    out: dict[str, dict[str, list[PropertyRecord]]] = {}
    for key, rows in group_by_property(records).items():
        per_source: dict[str, list[PropertyRecord]] = {}
        for row in rows:
            per_source.setdefault(row.source, []).append(row)
        out[key] = per_source
    return out


__all__ = [
    "PropertyRecord",
    "normalize_complex_name",
    "normalize_region",
    "property_key",
    "group_by_property",
    "cross_reference_sources",
]
