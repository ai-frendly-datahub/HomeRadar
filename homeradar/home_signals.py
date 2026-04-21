from __future__ import annotations

from typing import Any, Iterable


_EVENT_MARKERS: dict[str, tuple[str, ...]] = {
    "transaction_price": (
        "transaction",
        "trade price",
        "sale price",
        "actual transaction",
        "실거래",
        "매매",
        "거래가",
        "신고가",
    ),
    "rent_transaction": (
        "rent",
        "lease",
        "jeonse",
        "monthly rent",
        "전세",
        "월세",
        "전월세",
    ),
    "subscription_notice": (
        "subscription",
        "pre-sale",
        "presale",
        "competition rate",
        "청약",
        "분양",
        "경쟁률",
    ),
    "auction_signal": (
        "auction",
        "bid",
        "public auction",
        "온비드",
        "공매",
        "입찰",
    ),
    "listing_inventory": (
        "listing",
        "inventory",
        "asking price",
        "매물",
        "호가",
        "재고",
    ),
    "policy_context": (
        "policy",
        "regulation",
        "supply plan",
        "government announced",
        "정책",
        "규제",
        "공급대책",
        "국토교통부",
    ),
    "market_context": (
        "market",
        "trend",
        "forecast",
        "시장",
        "전망",
        "동향",
    ),
}


def _contains_any(text_lower: str, markers: Iterable[str]) -> bool:
    return any(marker.lower() in text_lower for marker in markers)


def _string_value(source: dict[str, Any] | None, key: str) -> str:
    if not source:
        return ""
    value = source.get(key)
    if isinstance(value, str):
        return value.strip()
    return ""


def _append_unique(values: list[str], candidate: str) -> None:
    if candidate and candidate not in values:
        values.append(candidate)


def classify_home_events(text: str) -> list[str]:
    text_lower = text.lower()
    events: list[str] = []
    for event_model, markers in _EVENT_MARKERS.items():
        if _contains_any(text_lower, markers):
            events.append(event_model)
    return events


def infer_home_verification_state(source: dict[str, Any] | None, events: Iterable[str]) -> str:
    event_list = list(events)
    if not event_list:
        return "no_operational_event"

    role = _string_value(source, "verification_role")
    trust_tier = _string_value(source, "trust_tier")

    if role.startswith("official_primary"):
        return "official_primary"
    if role == "official_policy_corroboration":
        return "official_policy_corroboration"
    if role == "market_corroboration":
        return "market_corroboration_requires_official_source"
    if role.startswith("backlog_"):
        return "backlog_candidate"
    if trust_tier.startswith("T1"):
        return "official_confirmed"
    if trust_tier.startswith(("T2", "T3")):
        return "corroboration_requires_official_source"
    return "verification_required"


def build_source_lookup(sources: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for source in sources:
        source_id = _string_value(source, "id")
        source_name = _string_value(source, "name")
        if source_id:
            lookup[source_id] = source
        if source_name:
            lookup[source_name] = source
    return lookup


def enrich_home_verification_fields(
    items: Iterable[Any], source_lookup: dict[str, dict[str, Any]]
) -> list[Any]:
    enriched: list[Any] = []
    for item in items:
        source = source_lookup.get(getattr(item, "source_id", ""), {})
        text = f"{getattr(item, 'title', '')} {getattr(item, 'summary', '')}"

        events = classify_home_events(text)
        source_event = _string_value(source, "event_model")
        _append_unique(events, source_event)

        verification_state = infer_home_verification_state(source, events)
        home_quality = {
            "operational_events": events,
            "verification_state": verification_state,
            "verification_role": _string_value(source, "verification_role"),
            "merge_policy": _string_value(source, "merge_policy"),
            "source_trust_tier": _string_value(source, "trust_tier"),
            "event_model": source_event,
            "event_date_field": _string_value(source, "event_date_field"),
            "observed_date_field": _string_value(source, "observed_date_field"),
            "canonical_key_fields": (
                list(source.get("canonical_key_fields") or []) if source else []
            ),
        }

        raw_data = dict(getattr(item, "raw_data", {}) or {})
        raw_data["home_quality"] = home_quality
        raw_data["HomeVerificationState"] = verification_state
        item.raw_data = raw_data
        enriched.append(item)
    return enriched
