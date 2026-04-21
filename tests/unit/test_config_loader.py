from __future__ import annotations

from homeradar.config_loader import load_source_quality_config


def test_real_sources_expose_verification_matrix() -> None:
    metadata = load_source_quality_config()
    data_quality = metadata["data_quality"]

    assert data_quality["priority"] == "P0"
    assert data_quality["weakest_dimension"] == "verification"
    assert (
        data_quality["quality_outputs"]["freshness_report"]
        == "reports/home_quality.json"
    )
    assert set(data_quality["quality_outputs"]["tracked_source_ids"]) == {
        "molit_apt_transaction",
        "molit_apt_rent",
        "reb_subscription",
        "onbid_auction",
    }
    assert "transaction_price" in data_quality["event_models"]
    assert "verification_claim" in data_quality["canonical_keys"]

    matrix = data_quality["verification_matrix"]
    assert matrix["transaction_price"]["primary_sources"] == ["molit_apt_transaction"]
    assert "hankyung_realestate" in matrix["transaction_price"]["corroborating_sources"]
    assert matrix["listing_inventory"]["merge_policy"] == "backlog_only_until_tos_review"


def test_real_sources_preserve_verification_roles() -> None:
    metadata = load_source_quality_config()
    sources = {source["id"]: source for source in metadata["sources"]}

    assert sources["molit_apt_transaction"]["event_model"] == "transaction_price"
    assert sources["molit_apt_transaction"]["verification_role"] == "official_primary_transaction"
    assert sources["reb_subscription"]["verification_role"] == "official_primary_subscription"
    assert sources["onbid_auction"]["verification_role"] == "official_primary_auction"
    assert sources["hankyung_realestate"]["merge_policy"] == "cannot_override_official_transaction"
    assert sources["korea_policy_news"]["verification_role"] == "official_policy_corroboration"
    assert sources["naver_land_main"]["verification_role"] == "backlog_listing_candidate"
