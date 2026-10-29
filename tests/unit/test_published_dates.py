"""Published-date parsing must stay on UTC when the process zone is KST."""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime, timedelta

from collectors.base import RawItem
from collectors.rss_collector import RSSCollector
from graph.graph_store import GraphStore
from homeradar.collector import _extract_datetime
from homeradar.models import Source


_STRUCT_UTC = time.struct_time((2026, 10, 28, 15, 0, 0, 2, 301, 0))


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def test_feedparser_struct_stores_1500_utc_under_asia_seoul(tmp_path) -> None:
    """2026-10-28 15:00 UTC struct_time must be stored as 15:00 UTC in KST."""
    previous = os.environ.get("TZ")
    os.environ["TZ"] = "Asia/Seoul"
    time.tzset()
    try:
        entry = {"published_parsed": _STRUCT_UTC}
        source = Source(name="Example", type="rss", url="https://example.com/feed.xml")
        published = _extract_datetime(entry, source)
        assert published == datetime(2026, 10, 28, 15, 0, tzinfo=UTC)

        collector = RSSCollector(
            "example_rss",
            {
                "url": "https://example.com/feed.xml",
                "type": "rss",
                "health_db_path": str(tmp_path / "health.duckdb"),
            },
        )
        rss_published = collector._parse_published_date(entry)
        assert rss_published == datetime(2026, 10, 28, 15, 0, tzinfo=UTC)

        store = GraphStore(tmp_path / "published.duckdb")
        store.add_items(
            [
                RawItem(
                    url="https://example.com/struct",
                    title="Struct time",
                    source_id="example_rss",
                    published_at=published,
                ),
                RawItem(
                    url="https://example.com/rss-struct",
                    title="RSS struct time",
                    source_id="example_rss",
                    published_at=rss_published,
                ),
            ]
        )
        with store._connection() as conn:
            rows = conn.execute(
                """
                SELECT urls.url, urls.published_at, articles.published_at, articles.collected_at
                FROM urls
                JOIN articles ON articles.url = urls.url
                ORDER BY urls.url
                """
            ).fetchall()
    finally:
        if previous is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = previous
        time.tzset()

    assert len(rows) == 2
    for _url, urls_published, articles_published, collected_at in rows:
        assert _as_utc(urls_published) == datetime(2026, 10, 28, 15, 0, tzinfo=UTC)
        assert _as_utc(articles_published) == datetime(2026, 10, 28, 15, 0, tzinfo=UTC)
        assert collected_at is not None


def test_missing_publish_date_is_stored_as_null(tmp_path) -> None:
    store = GraphStore(tmp_path / "missing.duckdb")
    store.add_items(
        [
            RawItem(
                url="https://example.com/no-date",
                title="No publish date",
                source_id="example_rss",
                published_at=None,
            )
        ]
    )
    with store._connection() as conn:
        row = conn.execute(
            """
            SELECT urls.published_at, articles.published_at, articles.collected_at
            FROM urls
            JOIN articles ON articles.url = urls.url
            """
        ).fetchone()
    assert row is not None
    assert row[0] is None
    assert row[1] is None
    assert row[2] is not None


def test_offset_string_keeps_offset_and_zoneless_kr_uses_seoul() -> None:
    offset_source = Source(name="Example", type="rss", url="https://example.com/feed.xml")
    offset = _extract_datetime(
        {"published": "Wed, 28 Oct 2026 15:00:00 +0900"},
        offset_source,
    )
    assert offset is not None
    assert offset.utcoffset() == timedelta(hours=9)
    assert offset.hour == 15
    assert _as_utc(offset) == datetime(2026, 10, 28, 6, 0, tzinfo=UTC)

    forced = Source(
        name="Example",
        type="rss",
        url="https://example.com/feed.xml",
        default_timezone="Asia/Seoul",
    )
    forced_value = _extract_datetime({"published": "2026-10-28 15:00:00"}, forced)
    assert forced_value is not None
    assert forced_value.utcoffset() == timedelta(hours=9)
    assert _as_utc(forced_value) == datetime(2026, 10, 28, 6, 0, tzinfo=UTC)

    korean = Source(name="MOLIT", type="rss", url="https://www.molit.go.kr/rss")
    zone_less = _extract_datetime(
        {
            "published": "2026-10-28 15:00:00",
            "published_parsed": _STRUCT_UTC,
        },
        korean,
    )
    assert zone_less is not None
    assert zone_less.utcoffset() == timedelta(hours=9)
    assert zone_less.hour == 15
    assert _as_utc(zone_less) == datetime(2026, 10, 28, 6, 0, tzinfo=UTC)
