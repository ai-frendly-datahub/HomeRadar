"""
Subscription (청약) API Collector for apartment pre-sale information.

Collects apartment subscription and pre-sale data from Korea Real Estate Board:
- Apartment subscription notices (청약 공고)
- Pre-sale schedules and information
- Competition rates and application details

API Documentation: https://www.data.go.kr/data/15101046/openapi.do
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from collectors.base import BaseCollector, CollectorError, RawItem


class SubscriptionCollector(BaseCollector):
    """
    Collector for apartment subscription and pre-sale information.

    Requires:
        - SUBSCRIPTION_API_KEY environment variable or source['api_key']

    Optional:
        - source['num_of_rows']: Number of rows per request (default: 100)
        - source['page_no']: Starting page number (default: 1)
    """

    api_key: str
    base_url: str
    endpoint: str
    num_of_rows: int
    page_no: int

    def __init__(self, source_id: str, source: dict[str, Any]) -> None:
        """Initialize subscription collector with source configuration."""
        super().__init__(source_id, source)

        # Get API key from env or config
        self.api_key = source.get("api_key") or os.getenv("SUBSCRIPTION_API_KEY") or ""
        if not self.api_key:
            raise ValueError(
                "Subscription collector requires 'api_key' in source config or SUBSCRIPTION_API_KEY env var"
            )

        # API endpoint
        self.base_url = source.get("base_url", "https://api.odcloud.kr/api/15101046/v1")
        self.endpoint = "getAPTLttotPblancDetail"

        # Default parameters
        self.num_of_rows = source.get("num_of_rows", 100)
        self.page_no = source.get("page_no", 1)

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True
    )
    def _make_request(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Make API request with retry logic.

        Args:
            params: Query parameters

        Returns:
            Parsed JSON response

        Raises:
            CollectorError: If request fails after retries
        """
        url = f"{self.base_url}/{self.endpoint}"
        params["serviceKey"] = self.api_key

        try:
            response = self._request("GET", url, params=params, timeout=30)
            return response.json()
        except requests.exceptions.RequestException as e:
            raise CollectorError(f"Subscription API request failed: {e}") from None
        except ValueError as e:
            raise CollectorError(f"Failed to parse Subscription API response: {e}") from None

    def collect(self) -> list[RawItem]:
        """
        Collect apartment subscription data from API.

        Returns:
            List of RawItem objects containing subscription data

        Raises:
            CollectorError: If collection fails
        """
        items: list[RawItem] = []

        try:
            # Fetch subscription data
            params: dict[str, Any] = {
                "numOfRows": self.num_of_rows,
                "pageNo": self.page_no,
            }

            response = self._make_request(params)

            # Parse response
            if not response or "response" not in response:
                return items

            body = response.get("response", {}).get("body", {})
            items_data = body.get("items", [])

            if not isinstance(items_data, list):
                items_data = [items_data] if items_data else []

            for item_data in items_data:
                try:
                    raw_item = self._parse_item(item_data)
                    if raw_item:
                        items.append(raw_item)
                except Exception as e:
                    # Log parse error but continue with other items
                    print(f"Warning: Failed to parse subscription item: {e}")
                    continue

        except CollectorError as e:
            raise e
        except Exception as e:
            raise CollectorError(f"Subscription collection failed: {e}") from None

        return items

    def _parse_item(self, item_data: dict[str, Any]) -> RawItem | None:
        """
        Parse a single subscription item from API response.

        Args:
            item_data: Dictionary containing item data

        Returns:
            RawItem object or None if parsing fails
        """
        # Extract essential fields
        prj_no = item_data.get("prjNo", "")
        if isinstance(prj_no, str):
            prj_no = prj_no.strip()
        prj_nm = item_data.get("prjNm", "")
        if isinstance(prj_nm, str):
            prj_nm = prj_nm.strip()
        notice_date = item_data.get("noticeDate", "")

        # Skip if essential fields are missing
        if not prj_no or not prj_nm:
            return None

        # Extract subscription info
        subscription_start = item_data.get("subscriptionStartDate", "")
        subscription_end = item_data.get("subscriptionEndDate", "")
        competition_rate = item_data.get("competitionRate", "")  # 경쟁률

        # Extract location and property info
        location = item_data.get("location", "")
        if isinstance(location, str):
            location = location.strip()
        region = item_data.get("region", "")
        if isinstance(region, str):
            region = region.strip()
        property_type = item_data.get("propertyType", "")
        if isinstance(property_type, str):
            property_type = property_type.strip()

        # Extract price info
        supply_price = item_data.get("supplyPrice", "")  # 공급가격
        area = self._parse_area(item_data.get("area", ""))

        # Parse date
        published_at = self._parse_date(notice_date)

        # Build title
        title = f"{prj_nm} - {location}".strip()

        # Build summary
        summary_parts: list[str] = []
        if subscription_start and subscription_end:
            summary_parts.append(f"청약: {subscription_start} ~ {subscription_end}")
        if competition_rate:
            summary_parts.append(f"경쟁률: {competition_rate}")
        if supply_price:
            summary_parts.append(f"공급가격: {supply_price}원")
        if property_type:
            summary_parts.append(f"유형: {property_type}")
        if area:
            summary_parts.append(f"면적: {area}㎡")

        summary = " | ".join(summary_parts)

        # Create unique URL
        url = f"subscription://project/{prj_no}"

        # Parse price
        try:
            price = float(supply_price) if supply_price else None
        except (ValueError, TypeError):
            price = None

        # Build raw data
        raw_data: dict[str, Any] = {
            "prjNo": prj_no,
            "prjNm": prj_nm,
            "location": location,
            "region": region,
            "propertyType": property_type,
            "noticeDate": notice_date,
            "subscriptionStartDate": subscription_start,
            "subscriptionEndDate": subscription_end,
            "competitionRate": competition_rate,
            "supplyPrice": supply_price,
            "area": item_data.get("area", ""),
        }

        # Create RawItem
        raw_item = RawItem(
            url=url,
            title=title,
            summary=summary,
            source_id=self.source_id,
            published_at=published_at,
            region=region or location,
            property_type=property_type or "아파트",
            price=price,
            area=area,
            raw_data=raw_data,
        )

        return raw_item

    def _parse_area(self, area_str: Any) -> float | None:
        """
        Parse area value from string.

        Args:
            area_str: Area string (may contain units)

        Returns:
            Area as float or None
        """
        if not area_str:
            return None

        try:
            # Remove common units
            cleaned = str(area_str).replace("㎡", "").replace("m²", "").strip()
            return float(cleaned) if cleaned else None
        except (ValueError, TypeError):
            return None

    def _parse_date(self, date_str: Any) -> datetime:
        """
        Parse date string from API response.

        Args:
            date_str: Date string (various formats possible)

        Returns:
            datetime object in UTC
        """
        if not date_str:
            return datetime.now(UTC)

        date_str_str = str(date_str).strip()

        # Try common date formats
        formats = [
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%Y%m%d",
            "%d-%m-%Y",
            "%d/%m/%Y",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str_str, fmt).replace(tzinfo=UTC)
                return dt.replace(tzinfo=UTC)
            except ValueError:
                continue

        # If no format matches, return current time
        return datetime.now(UTC)

    def collect_by_region(self, region: str) -> list[RawItem]:
        """
        Collect subscription data for a specific region.

        Args:
            region: Region name (서울, 경기 등)

        Returns:
            List of RawItem objects
        """
        params: dict[str, Any] = {
            "numOfRows": self.num_of_rows,
            "pageNo": self.page_no,
            "region": region,
        }

        items: list[RawItem] = []
        try:
            response = self._make_request(params)

            if not response or "response" not in response:
                return items

            body = response.get("response", {}).get("body", {})
            items_data = body.get("items", [])

            if not isinstance(items_data, list):
                items_data = [items_data] if items_data else []

            for item_data in items_data:
                try:
                    raw_item = self._parse_item(item_data)
                    if raw_item:
                        items.append(raw_item)
                except Exception as e:
                    print(f"Warning: Failed to parse subscription item: {e}")
                    continue

        except CollectorError as e:
            raise e

        return items
