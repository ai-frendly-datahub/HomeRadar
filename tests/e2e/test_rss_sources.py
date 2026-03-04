"""E2E smoke test for RSS sources that do not require authentication."""

from pathlib import Path

import feedparser
import pytest
import requests
import yaml


ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT / "config" / "sources.yaml"
HEADERS = {
    "User-Agent": "HomeRadar/0.1 (tests; +https://github.com/<username>/HomeRadar)"
}


def _load_rss_sources() -> list[dict]:
    data = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    sources = data.get("sources", [])
    return [src for src in sources if src.get("enabled") and src.get("type") == "rss"]


RSS_SOURCES = _load_rss_sources()

if not RSS_SOURCES:
    pytestmark = pytest.mark.skip(reason="No enabled RSS sources to test.")

KNOWN_EMPTY_FEEDS: dict[str, str] = {
    "mk_realestate": "Feed responds with 200 but empty body from test network",
}


@pytest.mark.e2e
@pytest.mark.parametrize("source", RSS_SOURCES, ids=lambda s: s["id"])
def test_rss_source_is_reachable(source: dict) -> None:
    url = source["url"]
    response = requests.get(url, headers=HEADERS, timeout=10)
    assert response.status_code == 200, f"{source['id']} returned {response.status_code}"

    if response.content:
        feed = feedparser.parse(response.content)
    else:
        feed = feedparser.parse(url, request_headers=HEADERS)

    status = feed.get("status") or response.status_code
    if not feed.entries:
        if source["id"] in KNOWN_EMPTY_FEEDS:
            pytest.xfail(f"{KNOWN_EMPTY_FEEDS[source['id']]} (status {status})")
        pytest.fail(f"{source['id']} feed has no entries (status {status}, bozo={feed.bozo})")
