"""
Onbid (온비드) API Collector for public auction properties.

Collects real estate auction data from Onbid OpenAPI:
- Public auction property listings
- Auction results and bidding information
- Property appraisal prices and winning bids

API Documentation: https://www.data.go.kr/data/15101046/openapi.do
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from collectors.base import BaseCollector, CollectorError, RawItem


class OnbidCollector(BaseCollector):
    """
    Collector for Onbid public auction property data.

    Requires:
        - ONBID_API_KEY environment variable or source['api_key']

    Optional:
        - source['num_of_rows']: Number of rows per request (default: 100)
        - source['page_no']: Starting page number (default: 1)
    """

    api_key: str
    base_url: str
    bid_result_endpoint: str
    property_detail_endpoint: str
    num_of_rows: int
    page_no: int

    def __init__(self, source_id: str, source: dict[str, Any]) -> None:
        """Initialize Onbid collector with source configuration."""
        super().__init__(source_id, source)

        # Get API key from env or config
        self.api_key = source.get("api_key") or os.getenv("ONBID_API_KEY") or ""
        if not self.api_key:
            raise ValueError(
                "Onbid collector requires 'api_key' in source config or ONBID_API_KEY env var"
            )

        # API endpoints
        self.base_url = source.get("base_url", "https://api.go.kr/B010003")
        self.bid_result_endpoint = "OnbidCltrBidRsltListSrvc"
        self.property_detail_endpoint = "UnifyUsageCltr"

        # Default parameters
        self.num_of_rows = source.get("num_of_rows", 100)
        self.page_no = source.get("page_no", 1)

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True
    )
    def _make_request(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        """
        Make API request with retry logic.

        Args:
            endpoint: API endpoint name
            params: Query parameters

        Returns:
            Parsed JSON response

        Raises:
            CollectorError: If request fails after retries
        """
        url = f"{self.base_url}/{endpoint}"
        params["serviceKey"] = self.api_key
        params["type"] = "json"

        try:
            response = self._request("GET", url, params=params, timeout=30)
            return response.json()
        except requests.exceptions.RequestException as e:
            raise CollectorError(f"Onbid API request failed: {e}")
        except ValueError as e:
            raise CollectorError(f"Failed to parse Onbid API response: {e}")

    def collect(self) -> list[RawItem]:
        """
        Collect auction property data from Onbid API.

        Returns:
            List of RawItem objects containing auction data

        Raises:
            CollectorError: If collection fails
        """
        items: list[RawItem] = []

        try:
            # Fetch auction results
            params: dict[str, Any] = {
                "numOfRows": self.num_of_rows,
                "pageNo": self.page_no,
            }

            response = self._make_request(self.bid_result_endpoint, params)

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
                    print(f"Warning: Failed to parse Onbid item: {e}")
                    continue

        except CollectorError as e:
            raise e
        except Exception as e:
            raise CollectorError(f"Onbid collection failed: {e}")

        return items

    def _parse_item(self, item_data: dict[str, Any]) -> RawItem | None:
        """
        Parse a single auction item from API response.

        Args:
            item_data: Dictionary containing item data

        Returns:
            RawItem object or None if parsing fails
        """
        # Extract essential fields
        cltr_no = item_data.get("cltrNo", "")
        if isinstance(cltr_no, str):
            cltr_no = cltr_no.strip()
        cltr_nm = item_data.get("cltrNm", "")
        if isinstance(cltr_nm, str):
            cltr_nm = cltr_nm.strip()
        appraisal_price = item_data.get("appraisalPrice", "")  # 감정가
        min_bid_price = item_data.get("minBidPrice", "")  # 최저입찰가
        winning_bid_price = item_data.get("winningBidPrice", "")  # 낙찰가

        # Skip if essential fields are missing
        if not cltr_no or not cltr_nm:
            return None

        # Parse prices
        try:
            appraisal = float(appraisal_price) if appraisal_price else None
            min_bid = float(min_bid_price) if min_bid_price else None
            winning_bid = float(winning_bid_price) if winning_bid_price else None
        except (ValueError, TypeError):
            appraisal = None
            min_bid = None
            winning_bid = None

        # Use winning bid if available, otherwise appraisal price
        price = winning_bid or appraisal or min_bid

        # Extract location and property info
        location = item_data.get("location", "")
        if isinstance(location, str):
            location = location.strip()
        property_type = item_data.get("propertyType", "")
        if isinstance(property_type, str):
            property_type = property_type.strip()
        area = self._parse_area(item_data.get("area", ""))

        # Parse date
        bid_date_str = item_data.get("bidDate", "")
        published_at = self._parse_date(bid_date_str)

        # Build title
        title = f"{cltr_nm} - {location}".strip()

        # Build summary
        summary_parts: list[str] = []
        if appraisal:
            summary_parts.append(f"감정가: {appraisal:,.0f}원")
        if min_bid:
            summary_parts.append(f"최저입찰가: {min_bid:,.0f}원")
        if winning_bid:
            summary_parts.append(f"낙찰가: {winning_bid:,.0f}원")
        if property_type:
            summary_parts.append(f"유형: {property_type}")
        if area:
            summary_parts.append(f"면적: {area}㎡")

        summary = " | ".join(summary_parts)

        # Create unique URL
        url = f"onbid://auction/{cltr_no}"

        # Build raw data
        raw_data: dict[str, Any] = {
            "cltrNo": cltr_no,
            "cltrNm": cltr_nm,
            "location": location,
            "propertyType": property_type,
            "appraisalPrice": appraisal_price,
            "minBidPrice": min_bid_price,
            "winningBidPrice": winning_bid_price,
            "area": item_data.get("area", ""),
            "bidDate": bid_date_str,
        }

        # Create RawItem
        raw_item = RawItem(
            url=url,
            title=title,
            summary=summary,
            source_id=self.source_id,
            published_at=published_at,
            region=location,
            property_type=property_type or "부동산",
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
                dt = datetime.strptime(date_str_str, fmt)
                return dt.replace(tzinfo=UTC)
            except ValueError:
                continue

        # If no format matches, return current time
        return datetime.now(UTC)

    def collect_by_region(self, region_code: str) -> list[RawItem]:
        """
        Collect auction data for a specific region.

        Args:
            region_code: Region code (시도코드)

        Returns:
            List of RawItem objects
        """
        params: dict[str, Any] = {
            "numOfRows": self.num_of_rows,
            "pageNo": self.page_no,
            "regionCode": region_code,
        }

        items: list[RawItem] = []
        try:
            response = self._make_request(self.bid_result_endpoint, params)

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
                    print(f"Warning: Failed to parse Onbid item: {e}")
                    continue

        except CollectorError as e:
            raise e

        return items
