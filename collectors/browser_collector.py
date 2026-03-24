from __future__ import annotations

import importlib
import logging
from datetime import UTC, datetime
from typing import Any

from collectors.base import BaseCollector, CollectorError, RawItem


logger = logging.getLogger(__name__)


class BrowserCollector(BaseCollector):
    def collect(self) -> list[RawItem]:
        if not self.source_config.get("enabled", False):
            return []

        url = str(
            self.source_config.get("url")
            or self.source_config.get("search_url")
            or self.source_config.get("base_url")
            or ""
        ).strip()
        if not url:
            raise CollectorError(f"{self.source_id}: Missing browser source URL")

        source_name = str(self.source_config.get("name", self.source_id))
        browser_config: dict[str, Any] = {
            "timeout": int(self.source_config.get("timeout", 15_000)),
            "wait_for": self.source_config.get("wait_for"),
            "content_selector": self.source_config.get("content_selector"),
            "title_selector": self.source_config.get("title_selector"),
            "link_selector": self.source_config.get("link_selector"),
        }

        try:
            browser_module = importlib.import_module("radar_core.browser_collector")
            collect_browser_sources = browser_module.collect_browser_sources
        except ImportError:
            logger.warning(
                "%s: browser collection unavailable (radar-core[browser] missing)", source_name
            )
            return []

        source_dict = {
            "name": source_name,
            "type": "browser",
            "url": url,
            "config": {k: v for k, v in browser_config.items() if v not in (None, "")},
        }

        try:
            articles, errors = collect_browser_sources(
                [source_dict],
                category=str(self.source_config.get("info_purpose", "listing")),
                timeout=int(browser_config["timeout"]),
                health_db_path=str(
                    self.source_config.get("health_db_path") or "data/radar_data.duckdb"
                ),
            )
        except Exception as exc:
            raise CollectorError(f"{source_name}: Browser collection failed: {exc}") from exc

        for error in errors:
            logger.warning("%s", error)

        now = datetime.now(tz=UTC)
        return [
            RawItem(
                url=article.link,
                title=article.title,
                summary=article.summary,
                source_id=self.source_id,
                published_at=article.published or now,
                collected_at=now,
                region=None,
                property_type=None,
                price=None,
                area=None,
                raw_data={
                    "source": source_name,
                    "collection_method": "playwright",
                },
            )
            for article in articles
            if article.link
        ]
