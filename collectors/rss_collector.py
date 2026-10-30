"""
RSS-based collector for HomeRadar news sources.

Collects real estate news from RSS feeds (newspapers, government feeds, etc.)
and converts them to RawItem format.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import feedparser
import requests
from radar_core.dates import parse_feed_entry, resolve_default_timezone

from collectors.base import BaseCollector, RawItem


FeedFetcher = Callable[[str], bytes]


class RSSCollector(BaseCollector):
    """
    RSS feed collector for news sources.

    Supports any RSS/Atom feed and converts entries to RawItem format.
    """

    def __init__(
        self,
        source_id: str,
        source_config: dict[str, Any],
        fetcher: FeedFetcher | None = None,
    ):
        """
        Initialize RSS collector.

        Args:
            source_id: Unique identifier for this source
            source_config: Configuration dict from sources.yaml
            fetcher: Optional custom feed fetcher (for testing)
        """
        super().__init__(source_id, source_config)
        self.url = source_config["url"]
        self.fetcher = fetcher or self._default_fetcher

    def _default_fetcher(self, url: str) -> bytes:
        """Fetch RSS feed content."""
        response = self._request("GET", url, timeout=15)
        return response.content

    def collect(self) -> list[RawItem]:
        """
        Collect items from RSS feed.

        Returns:
            List of RawItem objects from the feed

        Raises:
            CollectorError: If feed fetch or parsing fails
        """
        try:
            raw_feed = self.fetcher(self.url)
            feed = feedparser.parse(raw_feed)

            items = []
            now = datetime.now(UTC)

            for entry in feed.entries:
                # A missing feed date stays NULL. collected_at holds the run time.
                published_at, published_raw = self._parse_published(entry)
                category = self._item_category()
                raw_data = {
                    "entry_id": entry.get("id", ""),
                    "author": entry.get("author", ""),
                    "category": category,
                    "tags": [tag.get("term", "") for tag in entry.get("tags", [])],
                }
                if published_raw:
                    raw_data["published_raw"] = published_raw

                # Build RawItem
                raw_item = RawItem(
                    url=entry.get("link", ""),
                    title=entry.get("title", "").strip(),
                    summary=self._extract_summary(entry),
                    source_id=self.source_id,
                    published_at=published_at,
                    collected_at=now,
                    property_type=category,
                    raw_data=raw_data,
                )

                # Skip entries without valid URL
                if not raw_item.url:
                    continue

                items.append(raw_item)

            return items

        except requests.RequestException as e:
            from collectors.base import CollectorError

            raise CollectorError(f"Failed to fetch RSS feed from {self.url}: {e}") from e
        except Exception as e:
            from collectors.base import CollectorError

            raise CollectorError(f"Failed to parse RSS feed from {self.url}: {e}") from e

    def _parse_published_date(self, entry: Any) -> datetime | None:
        """
        Parse publication date from RSS entry.

        Args:
            entry: feedparser entry object

        Returns:
            datetime object or None if not found
        """
        published, _published_raw = self._parse_published(entry)
        return published

    def _parse_published(self, entry: Any) -> tuple[datetime | None, str | None]:
        """Return the UTC instant and the original date string from one entry."""
        configured = self.source_config.get("default_timezone")
        explicit = configured if isinstance(configured, str) else ""
        default_timezone = resolve_default_timezone(
            source_url=str(self.url or ""),
            default_timezone=explicit,
            config=self.source_config,
        )
        return parse_feed_entry(entry, default_timezone=default_timezone)

    def _item_category(self) -> str:
        for key in ("event_model", "info_purpose", "type"):
            value = self.source_config.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return "news"

    def _extract_summary(self, entry: Any) -> str:
        """
        Extract and generate summary from RSS entry.

        Tries in order:
        1. entry.summary (if present and non-empty)
        2. entry.content (first 280 chars)
        3. entry.title

        Args:
            entry: feedparser entry object

        Returns:
            Summary text
        """
        # Try summary field first
        summary = entry.get("summary", "").strip()
        if summary:
            return summary

        # Try content field
        if "content" in entry and entry.content:
            content = entry.content[0].get("value", "")
            # Strip HTML and normalize whitespace
            normalized = " ".join(content.split())
            if len(normalized) > 280:
                # Truncate at word boundary
                normalized = normalized[:280].rsplit(" ", 1)[0].rstrip() + "…"
            if normalized:
                return normalized

        # Fallback to title
        return entry.get("title", "").strip()
