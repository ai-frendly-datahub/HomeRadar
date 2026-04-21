from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from collectors.base import RawItem
from graph.graph_store import GraphStore
from homeradar.quality_report import build_quality_report, write_quality_report


def test_build_quality_report_tracks_fresh_stale_skip_and_verification_counts(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("MOLIT_SERVICE_KEY", "test-key")
    monkeypatch.delenv("SUBSCRIPTION_API_KEY", raising=False)
    now = datetime(2026, 4, 12, tzinfo=UTC)
    store = GraphStore(tmp_path / "homeradar.duckdb")
    store.add_items(
        [
            RawItem(
                url="https://example.com/molit/1",
                title="Fresh official transaction",
                summary="MOLIT transaction",
                source_id="molit_apt_transaction",
                published_at=now - timedelta(hours=3),
                region="서울",
                raw_data={
                    "home_quality": {
                        "verification_state": "official_primary",
                        "verification_role": "official_primary_transaction",
                        "merge_policy": "authoritative_source",
                        "event_model": "transaction_price",
                    }
                },
            ),
            RawItem(
                url="https://example.com/market/1",
                title="Old market metric",
                summary="market context",
                source_id="market_metric",
                published_at=now - timedelta(days=5),
                region="서울",
                raw_data={
                    "home_quality": {
                        "verification_state": "market_corroboration_requires_official_source",
                        "verification_role": "market_corroboration",
                        "merge_policy": "cannot_override_official_transaction",
                        "event_model": "market_context",
                    }
                },
            ),
        ]
    )
    sources = [
        {
            "id": "molit_apt_transaction",
            "name": "MOLIT Transaction",
            "type": "api",
            "enabled": True,
            "freshness_sla_days": 2,
            "event_model": "transaction_price",
            "verification_role": "official_primary_transaction",
        },
        {
            "id": "reb_subscription",
            "name": "REB Subscription",
            "type": "subscription",
            "enabled": True,
            "freshness_sla_days": 1,
            "event_model": "subscription_notice",
            "verification_role": "official_primary_subscription",
        },
        {
            "id": "market_metric",
            "name": "Market Metric",
            "type": "rss",
            "enabled": True,
            "freshness_sla_days": 1,
            "event_model": "market_context",
            "verification_role": "market_corroboration",
        },
        {
            "id": "missing_metric",
            "name": "Missing Metric",
            "type": "rss",
            "enabled": True,
            "freshness_sla_days": 1,
        },
        {
            "id": "disabled_listing",
            "name": "Disabled Listing",
            "type": "rss",
            "enabled": False,
            "freshness_sla_days": 1,
        },
        {
            "id": "general_news",
            "name": "General News",
            "type": "rss",
            "enabled": True,
        },
    ]

    report = build_quality_report(
        sources=sources,
        store=store,
        generated_at=now,
    )

    assert report["summary"]["fresh_sources"] == 1
    assert report["summary"]["stale_sources"] == 1
    assert report["summary"]["stale_source_ids"] == ["market_metric"]
    assert report["summary"]["missing_sources"] == 1
    assert report["summary"]["missing_source_ids"] == ["missing_metric"]
    assert report["summary"]["skipped_missing_env_sources"] == 1
    assert report["summary"]["skipped_missing_env_source_ids"] == ["reb_subscription"]
    assert report["summary"]["skipped_disabled_sources"] == 1
    assert report["summary"]["skipped_disabled_source_ids"] == ["disabled_listing"]
    assert report["summary"]["not_tracked_sources"] == 1
    assert report["summary"]["official_primary_status"] == "partial_fresh"
    assert report["summary"]["official_primary_sources"] == 2
    assert report["summary"]["official_primary_fresh_sources"] == 1
    assert report["summary"]["official_primary_blocked_sources"] == 1
    assert report["summary"]["official_primary_blocked_source_ids"] == ["reb_subscription"]
    assert report["summary"]["official_primary_required_env"] == ["SUBSCRIPTION_API_KEY"]
    assert report["summary"]["official_primary_items"] == 1
    assert report["summary"]["corroboration_only_items"] == 1
    assert report["verification_states"] == {
        "market_corroboration_requires_official_source": 1,
        "official_primary": 1,
    }

    statuses = {row["source_id"]: row["status"] for row in report["sources"]}
    assert statuses["molit_apt_transaction"] == "fresh"
    assert statuses["reb_subscription"] == "skipped_missing_env"
    assert statuses["market_metric"] == "stale"
    assert statuses["missing_metric"] == "missing"
    assert statuses["disabled_listing"] == "skipped_disabled"
    assert statuses["general_news"] == "not_tracked"
    assert {row["source_id"]: row["official_primary"] for row in report["sources"]}[
        "market_metric"
    ] is False


def test_build_quality_report_marks_all_official_sources_blocked_by_env(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.delenv("MOLIT_SERVICE_KEY", raising=False)
    monkeypatch.delenv("SUBSCRIPTION_API_KEY", raising=False)
    monkeypatch.delenv("ONBID_API_KEY", raising=False)
    store = GraphStore(tmp_path / "homeradar.duckdb")
    sources = [
        {
            "id": "molit_apt_transaction",
            "name": "MOLIT Transaction",
            "type": "api",
            "enabled": True,
            "freshness_sla_days": 2,
            "event_model": "transaction_price",
            "verification_role": "official_primary_transaction",
        },
        {
            "id": "reb_subscription",
            "name": "REB Subscription",
            "type": "subscription",
            "enabled": True,
            "freshness_sla_days": 1,
            "event_model": "subscription_notice",
            "verification_role": "official_primary_subscription",
        },
        {
            "id": "onbid_auction",
            "name": "Onbid Auction",
            "type": "onbid",
            "enabled": True,
            "freshness_sla_days": 1,
            "event_model": "auction_signal",
            "verification_role": "official_primary_auction",
        },
    ]

    report = build_quality_report(
        sources=sources,
        store=store,
        quality_config={
            "data_quality": {
                "quality_outputs": {
                    "tracked_source_ids": [
                        "molit_apt_transaction",
                        "reb_subscription",
                        "onbid_auction",
                    ]
                }
            }
        },
        generated_at=datetime(2026, 4, 12, tzinfo=UTC),
    )

    assert report["summary"]["official_primary_status"] == "blocked_missing_env"
    assert report["summary"]["official_primary_sources"] == 3
    assert report["summary"]["official_primary_credentialed_sources"] == 0
    assert report["summary"]["official_primary_blocked_source_ids"] == [
        "molit_apt_transaction",
        "reb_subscription",
        "onbid_auction",
    ]
    assert report["summary"]["skipped_missing_env_source_ids"] == [
        "molit_apt_transaction",
        "reb_subscription",
        "onbid_auction",
    ]
    assert report["summary"]["official_primary_required_env"] == [
        "MOLIT_SERVICE_KEY",
        "ONBID_API_KEY",
        "SUBSCRIPTION_API_KEY",
    ]


def test_write_quality_report_writes_latest_and_dated_files(tmp_path) -> None:
    report = {
        "category": "home",
        "generated_at": "2026-04-12T03:04:05+00:00",
        "summary": {},
        "verification_states": {},
        "sources": [],
    }

    paths = write_quality_report(report, output_dir=tmp_path, category_name="home")

    assert paths["latest"] == tmp_path / "home_quality.json"
    assert paths["dated"] == tmp_path / "home_20260412_quality.json"
    assert json.loads(paths["latest"].read_text(encoding="utf-8")) == report
    assert json.loads(paths["dated"].read_text(encoding="utf-8")) == report
