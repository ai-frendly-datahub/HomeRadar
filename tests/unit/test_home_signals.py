from __future__ import annotations

from datetime import UTC, datetime

from collectors.base import RawItem
from homeradar.home_signals import (
    build_source_lookup,
    classify_home_events,
    enrich_home_verification_fields,
    infer_home_verification_state,
)


def test_classify_home_events_detects_transaction_subscription_and_listing_terms() -> None:
    events = classify_home_events("강남 실거래 신고가와 청약 경쟁률, 매물 재고가 동시에 증가")

    assert events == ["transaction_price", "subscription_notice", "listing_inventory"]


def test_infer_home_verification_state_distinguishes_source_roles() -> None:
    assert (
        infer_home_verification_state(
            {"verification_role": "official_primary_transaction", "trust_tier": "T1_official"},
            ["transaction_price"],
        )
        == "official_primary"
    )
    assert (
        infer_home_verification_state(
            {"verification_role": "market_corroboration", "trust_tier": "T2_professional"},
            ["market_context"],
        )
        == "market_corroboration_requires_official_source"
    )


def test_enrich_home_verification_fields_adds_raw_data_quality_state() -> None:
    item = RawItem(
        url="https://example.com/home",
        title="강남 아파트 실거래 신고가",
        summary="시장 전망 기사",
        source_id="hankyung_realestate",
        published_at=datetime(2026, 4, 12, tzinfo=UTC),
    )
    lookup = build_source_lookup(
        [
            {
                "id": "hankyung_realestate",
                "name": "Korea Economic Daily - Real Estate",
                "trust_tier": "T2_professional",
                "event_model": "market_context",
                "verification_role": "market_corroboration",
                "merge_policy": "cannot_override_official_transaction",
                "observed_date_field": "collected_at",
            }
        ]
    )

    enriched = enrich_home_verification_fields([item], lookup)[0]
    home_quality = enriched.raw_data["home_quality"]

    assert home_quality["operational_events"] == ["transaction_price", "market_context"]
    assert home_quality["verification_state"] == "market_corroboration_requires_official_source"
    assert home_quality["merge_policy"] == "cannot_override_official_transaction"
    assert enriched.raw_data["HomeVerificationState"] == "market_corroboration_requires_official_source"


def test_enrich_home_verification_fields_marks_official_api_as_primary() -> None:
    item = RawItem(
        url="molit://11680/202604/apt/1",
        title="MOLIT transaction",
        summary="",
        source_id="molit_apt_transaction",
        published_at=datetime(2026, 4, 12, tzinfo=UTC),
    )
    lookup = build_source_lookup(
        [
            {
                "id": "molit_apt_transaction",
                "trust_tier": "T1_official",
                "event_model": "transaction_price",
                "verification_role": "official_primary_transaction",
                "merge_policy": "authoritative_source",
                "event_date_field": "deal_date",
                "observed_date_field": "collected_at",
                "canonical_key_fields": ["lawd_cd", "deal_ymd", "aptNm"],
            }
        ]
    )

    enriched = enrich_home_verification_fields([item], lookup)[0]
    home_quality = enriched.raw_data["home_quality"]

    assert home_quality["operational_events"] == ["transaction_price"]
    assert home_quality["verification_state"] == "official_primary"
    assert home_quality["canonical_key_fields"] == ["lawd_cd", "deal_ymd", "aptNm"]
