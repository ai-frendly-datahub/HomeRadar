"""
Base collector interface for HomeRadar.

Defines the common data model (RawItem) and collector interface
for various real estate data sources.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Optional, Union

import requests
from pydantic import BaseModel, Field

from resilience import SourceCircuitBreakerManager


class RawItem(BaseModel):
    """
    Raw collected item from any real estate data source.

    This is the common format used across all collectors before
    analysis and entity extraction.
    """

    url: str = Field(..., description="Unique URL or ID of the item")
    title: str = Field(..., description="Title or heading")
    summary: str = Field(default="", description="Summary or description")
    source_id: str = Field(..., description="Source identifier from config")
    published_at: datetime = Field(..., description="Publication/transaction date")
    collected_at: datetime = Field(default_factory=datetime.now, description="Collection timestamp")
    raw_data: dict[str, Any] = Field(
        default_factory=dict, description="Original raw data from source"
    )

    region: Optional[str] = Field(None, description="Region/district (서울, 경기 등)")
    property_type: Optional[str] = Field(
        None, description="Property type (아파트, 빌라, 오피스텔 등)"
    )
    price: Optional[float] = Field(None, description="Transaction or listing price")
    area: Optional[float] = Field(None, description="Area in square meters")


class BaseCollector(ABC):
    """
    Abstract base class for all HomeRadar collectors.

    Collectors fetch data from various sources (APIs, RSS, HTML)
    and convert them to RawItem format.
    """

    def __init__(self, source_id: str, source_config: dict[str, Any]):
        """
        Initialize collector with source configuration.

        Args:
            source_id: Unique identifier for this source
            source_config: Configuration dict from sources.yaml
        """
        self.source_id = source_id
        self.source_config = source_config
        self.breaker_manager = SourceCircuitBreakerManager()

    def _resolve_source_name(self) -> str:
        source_name = self.source_config.get("name")
        if isinstance(source_name, str) and source_name:
            return source_name
        return self.source_id

    def _fetch(self, url: str) -> requests.Response:
        source_name = self._resolve_source_name()
        breaker = self.breaker_manager.get_breaker(source_name)

        def _fetch_impl() -> requests.Response:
            timeout = self.source_config.get("timeout", 30)
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            return response

        return breaker.call(
            lambda source=source_name: _fetch_impl(),
            source=source_name,
        )

    def _fetch_html(self, url: str) -> Optional[str]:
        source_name = self._resolve_source_name()
        breaker = self.breaker_manager.get_breaker(source_name)

        def _fetch_html_impl() -> Optional[str]:
            timeout = self.source_config.get("timeout", 30)
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            response.encoding = response.apparent_encoding or "utf-8"
            return response.text

        return breaker.call(
            lambda source=source_name: _fetch_html_impl(),
            source=source_name,
        )

    def _fetch_json(self, url: str) -> Union[dict[str, Any], list[Any]]:
        source_name = self._resolve_source_name()
        breaker = self.breaker_manager.get_breaker(source_name)

        def _fetch_json_impl() -> Union[dict[str, Any], list[Any]]:
            timeout = self.source_config.get("timeout", 30)
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            return response.json()

        return breaker.call(
            lambda source=source_name: _fetch_json_impl(),
            source=source_name,
        )

    @abstractmethod
    def collect(self) -> list[RawItem]:
        """
        Collect data from the source and return RawItems.

        Returns:
            List of RawItem objects collected from the source

        Raises:
            CollectorError: If collection fails
        """
        pass

    @property
    def trust_tier(self) -> str:
        """Get trust tier from source config."""
        return self.source_config.get("trust_tier", "T3_aggregator")

    @property
    def info_purpose(self) -> str:
        """Get info purpose from source config."""
        return self.source_config.get("info_purpose", "listing")


class CollectorError(Exception):
    """Exception raised when collection fails."""

    pass
