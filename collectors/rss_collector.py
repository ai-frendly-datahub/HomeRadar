"""
RSS-based collector for HomeRadar news sources.

Collects real estate news from RSS feeds (newspapers, government feeds, etc.)
and converts them to RawItem format.
"""

from __future__ import annotations

import calendar
from datetime import datetime, timezone
from typing import Optional, Any, Callable

import feedparser
import requests

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
        fetcher: Optional[FeedFetcher] = None,
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
            now = datetime.now(timezone.utc)

            for entry in feed.entries:
                # Parse publication date
                published_at = self._parse_published_date(entry) or now

                # Build RawItem
                raw_item = RawItem(
                    url=entry.get("link", ""),
                    title=entry.get("title", "").strip(),
                    summary=self._extract_summary(entry),
                    source_id=self.source_id,
                    published_at=published_at,
                    raw_data={
                        "entry_id": entry.get("id", ""),
                        "author": entry.get("author", ""),
                        "tags": [tag.get("term", "") for tag in entry.get("tags", [])],
                    },
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

    def _parse_published_date(self, entry: Any) -> Optional[datetime]:
        """
        Parse publication date from RSS entry.

        Args:
            entry: feedparser entry object

        Returns:
            datetime object or None if not found
        """
        # Try multiple date fields
        published = entry.get("published_parsed") or entry.get("updated_parsed")

        if published:
            # Convert time.struct_time to datetime
            return datetime.fromtimestamp(calendar.timegm(published), tz=timezone.utc)

        return None

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
