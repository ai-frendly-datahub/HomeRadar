"""
MOLIT (Ministry of Land, Infrastructure and Transport) API Collector.

Collects apartment transaction data from MOLIT OpenAPI:
- Apartment sales transactions (매매)
- Apartment rent transactions (전월세)

API Documentation: https://www.data.go.kr/data/15126469/openapi.do
"""

import time
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from typing import Any

import requests

from collectors.base import BaseCollector, RawItem


class MOLITCollector(BaseCollector):
    """
    Collector for MOLIT apartment transaction data.

    Requires:
        - source['service_key']: API service key from data.go.kr
        - source['lawd_cd']: 5-digit regional code (법정동코드)
        - source['deal_ymd']: 6-digit year-month (YYYYMM)

    Optional:
        - source['num_of_rows']: Number of rows per request (default: 1000)
    """

    def __init__(self, source_id: str, source: dict[str, Any]):
        """Initialize MOLIT collector with source configuration."""
        super().__init__(source_id, source)

        # Validate required fields
        if "service_key" not in source:
            raise ValueError("MOLIT collector requires 'service_key' in source config")

        self.service_key = source["service_key"]
        self.api_url = source.get("url", "")

        # Default parameters
        self.num_of_rows = source.get("num_of_rows", 1000)

    def collect(self, lawd_cd: str, deal_ymd: str, page_no: int = 1) -> list[RawItem]:
        """
        Collect apartment transaction data for a specific region and month.

        Args:
            lawd_cd: 5-digit regional code (e.g., "11110" for Seoul Jongno-gu)
            deal_ymd: 6-digit year-month (e.g., "202401" for January 2024)
            page_no: Page number for pagination (default: 1)

        Returns:
            List of RawItem objects containing transaction data
        """
        # Build request parameters
        params = {
            "serviceKey": self.service_key,
            "LAWD_CD": lawd_cd,
            "DEAL_YMD": deal_ymd,
            "numOfRows": self.num_of_rows,
            "pageNo": page_no,
        }

        # Make API request
        try:
            response = self._request("GET", self.api_url, params=params, timeout=30)
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"MOLIT API request failed: {e}") from None

        # Parse XML response
        try:
            root = ET.fromstring(response.content)
        except ET.ParseError as e:
            raise RuntimeError(f"Failed to parse MOLIT API response: {e}") from None

        # Check response header
        header = root.find(".//header")
        if header is not None:
            result_code = header.findtext("resultCode", "")
            result_msg = header.findtext("resultMsg", "")

            if result_code != "00":
                raise RuntimeError(f"MOLIT API error: {result_code} - {result_msg}")

        # Parse items
        items = []
        body = root.find(".//body")

        if body is None:
            return items

        item_nodes = body.findall(".//item")

        for item_node in item_nodes:
            try:
                raw_item = self._parse_item(item_node, lawd_cd, deal_ymd)
                if raw_item:
                    items.append(raw_item)
            except Exception as e:
                # Log parse error but continue with other items
                print(f"Warning: Failed to parse item: {e}")
                continue

        return items

    def _parse_item(self, item_node: ET.Element, lawd_cd: str, deal_ymd: str) -> RawItem | None:
        """
        Parse a single transaction item from XML.

        Args:
            item_node: XML Element containing item data
            lawd_cd: Regional code for context
            deal_ymd: Year-month for context

        Returns:
            RawItem object or None if parsing fails
        """
        # Extract fields
        apt_name = self._get_text(item_node, "aptNm", "").strip()
        deal_amount = self._get_text(item_node, "dealAmount", "").strip()

        # Skip if essential fields are missing
        if not apt_name or not deal_amount:
            return None

        # Parse date
        deal_year = self._get_text(item_node, "dealYear", "")
        deal_month = self._get_text(item_node, "dealMonth", "").zfill(2)
        deal_day = self._get_text(item_node, "dealDay", "").zfill(2)

        # Build transaction date
        try:
            deal_date = datetime(int(deal_year), int(deal_month), int(deal_day), tzinfo=UTC)
        except (ValueError, TypeError):
            # Use deal_ymd as fallback
            deal_date = datetime(int(deal_ymd[:4]), int(deal_ymd[4:6]), 1, tzinfo=UTC)

        # Parse amount (remove commas and convert to integer)
        try:
            amount = int(deal_amount.replace(",", "").strip())
        except ValueError:
            amount = None

        # Parse area
        exclu_use_ar = self._get_text(item_node, "excluUseAr", "").strip()
        try:
            area = float(exclu_use_ar) if exclu_use_ar else None
        except ValueError:
            area = None

        # Build location string
        umd_nm = self._get_text(item_node, "umdNm", "").strip()
        jibun = self._get_text(item_node, "jibun", "").strip()
        location = f"{umd_nm} {jibun}".strip()

        # Build title
        floor = self._get_text(item_node, "floor", "").strip()
        floor_text = f"{floor}층" if floor else ""
        area_text = f"{exclu_use_ar}㎡" if exclu_use_ar else ""

        title = f"{apt_name} {area_text} {floor_text}".strip()

        # Build summary
        summary = (
            f"거래금액: {deal_amount}만원 | "
            f"위치: {location} | "
            f"건축년도: {self._get_text(item_node, 'buildYear', 'N/A')}"
        )

        # Create unique URL (MOLIT doesn't provide URLs)
        # Use combination of fields to create pseudo-URL
        url = (
            f"molit://transaction/{lawd_cd}/{deal_ymd}/"
            f"{apt_name}/{deal_year}{deal_month}{deal_day}/{exclu_use_ar}/{floor}"
        )

        # Build raw data with all fields
        raw_data = {
            "sggCd": self._get_text(item_node, "sggCd", ""),
            "umdNm": umd_nm,
            "jibun": jibun,
            "aptNm": apt_name,
            "aptDong": self._get_text(item_node, "aptDong", ""),
            "excluUseAr": exclu_use_ar,
            "dealYear": deal_year,
            "dealMonth": deal_month,
            "dealDay": deal_day,
            "dealAmount": deal_amount,
            "floor": floor,
            "buildYear": self._get_text(item_node, "buildYear", ""),
            "lawd_cd": lawd_cd,
            "deal_ymd": deal_ymd,
        }

        # Create RawItem
        raw_item = RawItem(
            url=url,
            title=title,
            summary=summary,
            source_id=self.source_id,
            published_at=deal_date,
            region=umd_nm,
            property_type="아파트",
            price=amount,
            area=area,
            raw_data=raw_data,
        )

        return raw_item

    def _get_text(self, node: ET.Element, tag: str, default: str = "") -> str:
        """
        Safely extract text from XML element.

        Args:
            node: XML Element
            tag: Tag name to find
            default: Default value if tag not found

        Returns:
            Text content or default value
        """
        element = node.find(tag)
        if element is not None and element.text:
            return element.text.strip()
        return default

    def collect_multiple_months(
        self, lawd_cd: str, start_ym: str, end_ym: str, delay: float = 0.5
    ) -> list[RawItem]:
        """
        Collect data for multiple consecutive months.

        Args:
            lawd_cd: 5-digit regional code
            start_ym: Start year-month (YYYYMM)
            end_ym: End year-month (YYYYMM)
            delay: Delay between requests in seconds (default: 0.5)

        Returns:
            Combined list of RawItem objects
        """
        all_items = []

        # Parse start and end dates
        start_year = int(start_ym[:4])
        start_month = int(start_ym[4:6])
        end_year = int(end_ym[:4])
        end_month = int(end_ym[4:6])

        # Iterate through months
        current_year = start_year
        current_month = start_month

        while current_year < end_year or (current_year == end_year and current_month <= end_month):
            deal_ymd = f"{current_year}{current_month:02d}"

            try:
                items = self.collect(lawd_cd, deal_ymd)
                all_items.extend(items)
                print(f"  Collected {len(items)} items for {deal_ymd}")
            except Exception as e:
                print(f"  Error collecting {deal_ymd}: {e}")

            # Add delay to avoid rate limiting
            time.sleep(delay)

            # Increment month
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1

        return all_items
