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


_DEFAULT_HEALTH_DB_PATH = "data/radar_data.duckdb"


def _load_adaptive_controls() -> tuple[type[Any], type[Any]]:
    module = __import__("radar_core", fromlist=["AdaptiveThrottler", "CrawlHealthStore"])
    return module.AdaptiveThrottler, module.CrawlHealthStore


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
        min_delay = source_config.get("request_interval", 0.5)
        if not isinstance(min_delay, (int, float)):
            min_delay = 0.5
        throttler_cls, health_store_cls = _load_adaptive_controls()
        self._throttler = throttler_cls(min_delay=max(0.001, float(min_delay)))
        self._health_store = health_store_cls(
            source_config.get("health_db_path")
            or os.environ.get("RADAR_CRAWL_HEALTH_DB_PATH", _DEFAULT_HEALTH_DB_PATH)
        )

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        source_name = self._resolve_source_name()
        breaker = self.breaker_manager.get_breaker(source_name)
        timeout = kwargs.pop("timeout", self.source_config.get("timeout", 30))

        min_interval = self.source_config.get("request_interval", 0.5)
        if not isinstance(min_interval, (int, float)):
            min_interval = 0.5

        host = urlparse(url).netloc.lower() or source_name
        limiter = self._rate_limiters.setdefault(host, RateLimiter(float(min_interval)))
        max_attempts_raw = self.source_config.get("max_retry_attempts", 3)
        max_attempts = max_attempts_raw if isinstance(max_attempts_raw, int) else 3
        max_attempts = max(1, max_attempts)
        retryable_errors = (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError,
        )

        def _request_impl() -> requests.Response:
            for attempt in range(max_attempts):
                limiter.acquire()
                self._throttler.acquire(source_name)

                try:
                    if method.upper() == "POST":
                        response = self._session.post(url, timeout=timeout, **kwargs)
                    else:
                        response = self._session.get(url, timeout=timeout, **kwargs)
                    response.raise_for_status()

                    self._throttler.record_success(source_name)
                    delay = self._throttler.get_current_delay(source_name)
                    self._health_store.record_success(source_name, delay)
                    return response
                except retryable_errors as exc:
                    retry_after: int | str | None = None
                    if isinstance(exc, requests.exceptions.HTTPError):
                        response = exc.response
                        if response is not None and response.status_code == 429:
                            retry_after = _parse_retry_after(response.headers.get("Retry-After"))

                    self._throttler.record_failure(source_name, retry_after=retry_after)
                    delay = self._throttler.get_current_delay(source_name)
                    self._health_store.record_failure(source_name, str(exc), delay)

                    if attempt == max_attempts - 1:
                        raise

            raise RuntimeError("Retry loop exited unexpectedly")

        return breaker.call(
            lambda source=source_name: _request_impl(),
            source=source_name,
        )

    def _resolve_source_name(self) -> str:
        source_name = self.source_config.get("name")
        if isinstance(source_name, str) and source_name:
            return source_name
        return self.source_id

    def __del__(self) -> None:
        self._session.close()
        self._health_store.close()

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
        status_forcelist=[408, 429, 500, 502, 503, 504, 522, 524],
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _parse_retry_after(value: str | None) -> int | str | None:
    if value is None:
        return None

    stripped = value.strip()
    if not stripped:
        return None

    if stripped.isdigit():
        return int(stripped)

    return stripped
