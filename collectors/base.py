"""
Base collector interface for HomeRadar.

Defines the common data model (RawItem) and collector interface
for various real estate data sources.
"""

import os
import threading
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

import requests
from pydantic import BaseModel, Field
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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

    region: str | None = Field(None, description="Region/district (서울, 경기 등)")
    property_type: str | None = Field(None, description="Property type (아파트, 빌라, 오피스텔 등)")
    price: float | None = Field(None, description="Transaction or listing price")
    area: float | None = Field(None, description="Area in square meters")


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
        self._session = _create_session()
        self._rate_limiters: dict[str, RateLimiter] = {}

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        source_name = self._resolve_source_name()
        breaker = self.breaker_manager.get_breaker(source_name)
        timeout = kwargs.pop("timeout", self.source_config.get("timeout", 30))

        min_interval = self.source_config.get("request_interval", 0.5)
        if not isinstance(min_interval, (int, float)):
            min_interval = 0.5

        host = urlparse(url).netloc.lower() or source_name
        limiter = self._rate_limiters.setdefault(host, RateLimiter(float(min_interval)))

        def _request_impl() -> requests.Response:
            limiter.acquire()
            if method.upper() == "POST":
                response = requests.post(url, timeout=timeout, **kwargs)
            else:
                response = requests.get(url, timeout=timeout, **kwargs)
            response.raise_for_status()
            return response

        return breaker.call(
            lambda source=source_name: _request_impl(),
            source=source_name,
        )

    def _resolve_source_name(self) -> str:
        source_name = self.source_config.get("name")
        if isinstance(source_name, str) and source_name:
            return source_name
        return self.source_id

    def _fetch(self, url: str) -> requests.Response:
        return self._request("GET", url)

    def _fetch_html(self, url: str) -> str | None:
        response = self._request("GET", url)
        response.encoding = response.apparent_encoding or "utf-8"
        return response.text

    def _fetch_json(self, url: str) -> dict[str, Any] | list[Any]:
        response = self._request("GET", url)
        return response.json()

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


class RateLimiter:
    def __init__(self, min_interval: float = 0.5):
        self._min_interval = min_interval
        self._last_request = 0.0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last_request = time.monotonic()


def resolve_max_workers(max_workers: int | None = None) -> int:
    if max_workers is None:
        raw_value = os.environ.get("RADAR_MAX_WORKERS", "5")
        try:
            parsed = int(raw_value)
        except ValueError:
            parsed = 5
    else:
        parsed = max_workers

    return max(1, min(parsed, 10))


def _create_session() -> requests.Session:
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
