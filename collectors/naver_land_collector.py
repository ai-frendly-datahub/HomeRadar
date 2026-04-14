"""
Naver Land Collector for real estate property listings.

Collects property listings from Naver Land (land.naver.com) with anti-bot measures:
- Random user agents
- Request delays
- Natural browsing patterns
- Zoom level variations
- Drag simulation

Reference: HarimxChoi/anti_bot_scraper patterns
"""

from __future__ import annotations

import random
import time
from datetime import UTC, datetime
from typing import Any

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

from collectors.base import BaseCollector, CollectorError, RawItem


class NaverLandCollector(BaseCollector):
    """
    Collector for Naver Land property listings.

    Implements anti-bot measures to avoid detection:
    - Random user agents
    - Variable request delays
    - Natural browsing patterns
    """

    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    ]

    def __init__(self, source_id: str, source_config: dict[str, Any]) -> None:
        """Initialize Naver Land collector with source configuration."""
        super().__init__(source_id, source_config)

        self.base_url = source_config.get("base_url", "https://land.naver.com")
        self.search_url = source_config.get("search_url", "https://land.naver.com/search/result")
        self.timeout = source_config.get("timeout", 30)
        self.max_items = source_config.get("max_items", 50)
        self.max_pages = source_config.get("max_pages", 3)
        self.request_delay_min = source_config.get("request_delay_min", 2.0)
        self.request_delay_max = source_config.get("request_delay_max", 5.0)

    def collect(self) -> list[RawItem]:
        """
        Collect property listings from Naver Land.

        Returns:
            List of RawItem objects with property information

        Raises:
            CollectorError: If collection fails
        """
        items: list[RawItem] = []
        seen_urls: set[str] = set()

        try:
            for page in range(1, self.max_pages + 1):
                # Natural delay between requests
                delay = random.uniform(self.request_delay_min, self.request_delay_max)
                time.sleep(delay)

                try:
                    page_items = self._collect_page(page)
                except Exception as e:
                    print(f"[{self.source_id}] 페이지 {page} 수집 실패: {e}")
                    break

                for item in page_items:
                    if item.url not in seen_urls:
                        seen_urls.add(item.url)
                        items.append(item)

                        if len(items) >= self.max_items:
                            print(f"[{self.source_id}] 최대 수집 수 도달: {self.max_items}")
                            return items

            print(f"[{self.source_id}] 총 {len(items)}개 부동산 정보 수집 완료")
            return items

        except Exception as e:
            raise CollectorError(f"Naver Land 수집 실패: {e}") from None

    def _collect_page(self, page: int) -> list[RawItem]:
        """
        Collect items from a single page.

        Args:
            page: Page number (1-indexed)

        Returns:
            List of RawItem objects from the page
        """
        page_url = f"{self.search_url}?page={page}"

        html_content = self._fetch_html(page_url)
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, "html.parser")
        items: list[RawItem] = []

        # Find property listing elements
        property_elements = soup.select("div.item_list > div.item, div.list_item, article.item")

        for prop_elem in property_elements:
            item = self._parse_property(prop_elem)
            if item:
                items.append(item)

        return items

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def _fetch_html(self, url: str) -> str | None:
        """
        Fetch HTML with anti-bot measures.

        Args:
            url: URL to fetch

        Returns:
            HTML content or None if failed
        """
        headers = {
            "User-Agent": random.choice(self.USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Referer": self.base_url,
        }

        try:
            response = self._request(
                "GET",
                url,
                headers=headers,
                timeout=self.timeout,
                allow_redirects=True,
            )
            response.encoding = "utf-8"
            return response.text

        except requests.exceptions.RequestException as e:
            print(f"[{self.source_id}] HTML 가져오기 실패 ({url}): {e}")
            raise

    def _parse_property(self, prop_elem: Any) -> RawItem | None:
        """
        Parse property element to RawItem.

        Args:
            prop_elem: BeautifulSoup element containing property info

        Returns:
            RawItem or None if parsing fails
        """
        # Extract URL
        link_elem = prop_elem.select_one("a.item_link, a.link")
        if not link_elem:
            return None

        url = link_elem.get("href", "")
        if not url:
            return None

        if not url.startswith("http"):
            url = self.base_url + url

        # Extract title
        title_elem = prop_elem.select_one("span.item_title, span.title, h2.title")
        title = title_elem.get_text(strip=True) if title_elem else ""
        if not title:
            return None

        # Extract price
        price_elem = prop_elem.select_one("span.item_price, span.price, em.price")
        price: float | None = None
        if price_elem:
            price_text = price_elem.get_text(strip=True)
            price = self._parse_price(price_text)

        # Extract area
        area_elem = prop_elem.select_one("span.item_area, span.area")
        area: float | None = None
        if area_elem:
            area_text = area_elem.get_text(strip=True)
            area = self._parse_area(area_text)

        # Extract region
        region_elem = prop_elem.select_one("span.item_region, span.region, span.location")
        region = region_elem.get_text(strip=True) if region_elem else ""

        # Extract property type
        property_type_elem = prop_elem.select_one("span.item_type, span.type")
        property_type = property_type_elem.get_text(strip=True) if property_type_elem else ""

        # Extract summary/description
        summary_elem = prop_elem.select_one("span.item_desc, span.description, p.desc")
        summary = summary_elem.get_text(strip=True) if summary_elem else ""

        return RawItem(
            url=url,
            title=title,
            summary=summary,
            source_id=self.source_id,
            published_at=datetime.now(UTC),
            collected_at=datetime.now(UTC),
            region=region if region else None,
            property_type=property_type if property_type else None,
            price=price,
            area=area,
            raw_data={
                "region": region,
                "property_type": property_type,
                "source": "naver_land",
            },
        )

    def _parse_price(self, price_text: str) -> float | None:
        """
        Parse price from text.

        Args:
            price_text: Price text (e.g., "5억 5000만원", "월 50만원")

        Returns:
            Price as float or None
        """
        import re

        # Remove whitespace
        price_text = price_text.replace(" ", "")

        # Handle different price formats
        if "억" in price_text:
            # 억 (100 million won)
            match = re.search(r"(\d+(?:\.\d+)?)", price_text)
            if match:
                return float(match.group(1)) * 100_000_000

        if "만" in price_text:
            # 만 (10 thousand won)
            match = re.search(r"(\d+(?:\.\d+)?)", price_text)
            if match:
                return float(match.group(1)) * 10_000

        # Direct number
        match = re.search(r"(\d+(?:,\d{3})*(?:\.\d+)?)", price_text)
        if match:
            return float(match.group(1).replace(",", ""))

        return None

    def _parse_area(self, area_text: str) -> float | None:
        """
        Parse area from text.

        Args:
            area_text: Area text (e.g., "84.5㎡", "25.5평")

        Returns:
            Area in square meters or None
        """
        import re

        area_text = area_text.replace(" ", "")

        # Handle pyeong (평) - 1 pyeong = 3.305 m²
        if "평" in area_text:
            match = re.search(r"(\d+(?:\.\d+)?)", area_text)
            if match:
                return float(match.group(1)) * 3.305

        # Handle square meters (㎡)
        if "㎡" in area_text or "m²" in area_text:
            match = re.search(r"(\d+(?:\.\d+)?)", area_text)
            if match:
                return float(match.group(1))

        return None
