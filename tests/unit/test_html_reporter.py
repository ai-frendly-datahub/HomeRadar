from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from collectors.base import RawItem
from graph.graph_store import GraphStore
from reporters.html_reporter import HtmlReporter, generate_index_html


@pytest.fixture
def tmp_store(tmp_path: Path) -> GraphStore:
    return GraphStore(db_path=tmp_path / "reporter_test.duckdb")


@pytest.fixture
def sample_region_items() -> list[RawItem]:
    published_at = datetime(2026, 3, 1, tzinfo=UTC)
    return [
        RawItem(
            url="https://example.com/region/seoul-1",
            title="Seoul market update",
            summary="Gangnam district apartment demand increases.",
            source_id="rss_news",
            published_at=published_at,
            region="서울",
            property_type=None,
            price=None,
            area=None,
            raw_data={"district": "강남구"},
        ),
        RawItem(
            url="https://example.com/region/gyeonggi-1",
            title="Gyeonggi market update",
            summary="Bundang district transaction volume rises.",
            source_id="rss_news",
            published_at=published_at,
            region="경기",
            property_type=None,
            price=None,
            area=None,
            raw_data={"district": "분당구"},
        ),
        RawItem(
            url="https://example.com/region/busan-1",
            title="Busan market update",
            summary="Busan apartment supply outlook.",
            source_id="rss_news",
            published_at=published_at,
            region="부산",
            property_type=None,
            price=None,
            area=None,
            raw_data={"district": "해운대구"},
        ),
    ]


def test_get_sido_distribution_from_region_nodes(
    tmp_store: GraphStore,
    sample_region_items: list[RawItem],
) -> None:
    tmp_store.add_items(sample_region_items)
    tmp_store.add_entities(sample_region_items[0].url, {"district": ["강남구"]})
    tmp_store.add_entities(sample_region_items[1].url, {"district": ["분당구"]})

    reporter = HtmlReporter()
    distribution = reporter._get_sido_distribution(tmp_store)

    counts = {item["sido"]: item["count"] for item in distribution}
    assert counts["서울"] == 1
    assert counts["경기"] == 1


def test_get_sido_distribution_falls_back_to_urls(
    tmp_store: GraphStore,
    sample_region_items: list[RawItem],
) -> None:
    tmp_store.add_items(sample_region_items)

    reporter = HtmlReporter()
    distribution = reporter._get_sido_distribution(tmp_store)

    counts = {item["sido"]: item["count"] for item in distribution}
    assert counts["서울"] == 1
    assert counts["경기"] == 1
    assert counts["부산"] == 1


def test_render_region_distribution_returns_table_when_geojson_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reporter = HtmlReporter()

    def _raise_geojson_error() -> dict[str, object]:
        raise RuntimeError("geojson unavailable")

    monkeypatch.setattr(reporter, "_load_korea_geojson", _raise_geojson_error)

    html, used_fallback = reporter._render_region_distribution(
        [
            {
                "sido": "서울",
                "sido_full": "서울특별시",
                "count": 5,
            }
        ]
    )

    assert used_fallback is True
    assert "<table" in html
    assert "서울" in html


def test_generate_report_renders_home_verification_fields(
    tmp_path: Path,
    tmp_store: GraphStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    item = RawItem(
        url="https://example.com/molit/transaction/1",
        title="Official transaction update",
        summary="MOLIT transaction record for apartment market monitoring.",
        source_id="molit_apt_transaction",
        published_at=datetime(2026, 3, 1, tzinfo=UTC),
        region="서울",
        raw_data={
            "district": "강남구",
            "home_quality": {
                "verification_state": "official_primary",
                "verification_role": "official_primary_transaction",
                "merge_policy": "authoritative_source",
                "event_model": "transaction_price",
            },
        },
    )
    tmp_store.add_items([item])

    template_dir = Path(__file__).resolve().parents[2] / "reporters" / "templates"
    reporter = HtmlReporter(template_dir=template_dir)
    monkeypatch.setattr(reporter, "_render_region_distribution", lambda _distribution: ("", True))

    output_path = tmp_path / "reports" / "daily_report.html"
    reporter.generate_report(tmp_store, output_path)
    rendered = output_path.read_text(encoding="utf-8")

    assert 'data-visual-system="radar-unified-v2"' in rendered
    assert 'data-visual-surface="report"' in rendered
    assert "official_primary" in rendered
    assert "transaction_price" in rendered
    assert "authoritative_source" in rendered


def test_generate_report_renders_home_quality_summary(
    tmp_path: Path,
    tmp_store: GraphStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    item = RawItem(
        url="https://example.com/molit/transaction/2",
        title="Official transaction update",
        summary="MOLIT transaction record for apartment market monitoring.",
        source_id="molit_apt_transaction",
        published_at=datetime(2026, 3, 1, tzinfo=UTC),
        region="서울",
        raw_data={
            "home_quality": {
                "verification_state": "official_primary",
                "verification_role": "official_primary_transaction",
                "merge_policy": "authoritative_source",
                "event_model": "transaction_price",
            },
        },
    )
    tmp_store.add_items([item])
    quality_report = {
        "summary": {
            "fresh_sources": 1,
            "stale_sources": 1,
            "missing_sources": 0,
            "skipped_sources": 1,
            "verification_state_count": 1,
            "official_primary_status": "blocked_missing_env",
            "official_primary_fresh_sources": 0,
            "official_primary_blocked_sources": 1,
            "official_primary_blocked_source_ids": ["reb_subscription"],
            "official_primary_required_env": ["SUBSCRIPTION_API_KEY"],
            "corroboration_only_items": 1,
            "daily_review_item_count": 2,
        },
        "verification_states": {"official_primary": 1},
        "daily_review_items": [
            {
                "reason": "official_primary_missing_env",
                "source_id": "reb_subscription",
                "event_model": "subscription_notice",
                "required_env": "SUBSCRIPTION_API_KEY",
            },
            {
                "reason": "home_verification_requires_official_primary",
                "source_id": "hankyung_realestate",
                "event_model": "market_context",
                "verification_state": "market_corroboration_requires_official_source",
                "title": "Market article needs official transaction confirmation",
            },
        ],
        "sources": [
            {
                "source_id": "molit_apt_transaction",
                "status": "fresh",
                "freshness_sla_days": 2,
                "age_days": 0.1,
                "skip_reason": "",
            },
            {
                "source_id": "reb_subscription",
                "status": "skipped_missing_env",
                "freshness_sla_days": 1,
                "age_days": None,
                "skip_reason": "SUBSCRIPTION_API_KEY not set",
            },
            {
                "source_id": "disabled_listing",
                "status": "skipped_disabled",
                "freshness_sla_days": None,
                "age_days": None,
                "skip_reason": "source disabled",
            },
        ],
    }

    template_dir = Path(__file__).resolve().parents[2] / "reporters" / "templates"
    reporter = HtmlReporter(template_dir=template_dir)
    monkeypatch.setattr(reporter, "_render_region_distribution", lambda _distribution: ("", True))

    output_path = tmp_path / "reports" / "daily_report.html"
    reporter.generate_report(tmp_store, output_path, quality_report=quality_report)
    rendered = output_path.read_text(encoding="utf-8")

    assert "Home Quality" in rendered
    assert "home_quality.json" in rendered
    assert "official_primary" in rendered
    assert "blocked_missing_env" in rendered
    assert "Daily Review Items" in rendered
    assert "official_primary_missing_env" in rendered
    assert "home_verification_requires_official_primary" in rendered
    assert "reb_subscription" in rendered
    assert "SUBSCRIPTION_API_KEY not set" in rendered
    assert "disabled_listing" in rendered
    assert "source disabled" in rendered


def test_generate_index_html_uses_unified_surface_markers(tmp_path: Path) -> None:
    report_dir = tmp_path / "reports"
    report_dir.mkdir(parents=True)
    (report_dir / "daily_report.html").write_text("sample", encoding="utf-8")

    index_path = generate_index_html(report_dir)
    rendered = index_path.read_text(encoding="utf-8")

    assert 'data-visual-system="radar-unified-v2"' in rendered
    assert 'data-visual-surface="report"' in rendered
    assert 'data-visual-page="index"' in rendered
