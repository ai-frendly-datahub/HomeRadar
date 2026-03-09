# Canonical Notifier implementation for AI-Friendly DataHub
# Synced from: Radar-Template/radar/notifier.py
# DO NOT MODIFY core classes (Notifier, NotificationPayload, EmailNotifier, WebhookNotifier, CompositeNotifier)
# Domain-specific detection functions (detect_home_notifications) preserved below

from __future__ import annotations

import smtplib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from email.mime.text import MIMEText
from typing import Optional, Any, Protocol

import requests
import structlog

from collectors.base import RawItem

logger = structlog.get_logger(__name__)


@dataclass
class NotificationPayload:
    """Payload for notification delivery."""

    category_name: str
    sources_count: int
    collected_count: int
    matched_count: int
    errors_count: int
    timestamp: datetime
    report_url: Optional[str] = None

    def to_dict(self) -> dict[str, object]:
        """Convert payload to dictionary for JSON serialization."""
        return {
            "category_name": self.category_name,
            "sources_count": self.sources_count,
            "collected_count": self.collected_count,
            "matched_count": self.matched_count,
            "errors_count": self.errors_count,
            "timestamp": self.timestamp.isoformat(),
            "report_url": self.report_url,
        }


class Notifier(Protocol):
    """Protocol for notification delivery."""

    def send(self, payload: NotificationPayload) -> bool:
        """Send notification. Return True if successful, False otherwise."""
        ...


class EmailNotifier:
    """Send notifications via email using SMTP."""

    def __init__(
        self,
        smtp_host: str,
        smtp_port: int,
        smtp_user: str,
        smtp_password: str,
        from_addr: str,
        to_addrs: list[str],
    ) -> None:
        """Initialize email notifier.

        Args:
            smtp_host: SMTP server hostname
            smtp_port: SMTP server port
            smtp_user: SMTP username
            smtp_password: SMTP password
            from_addr: Sender email address
            to_addrs: List of recipient email addresses
        """
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password
        self.from_addr = from_addr
        self.to_addrs = to_addrs

    def send(self, payload: NotificationPayload) -> bool:
        """Send email notification.

        Args:
            payload: Notification payload

        Returns:
            True if successful, False otherwise
        """
        try:
            subject = f"Radar Pipeline Complete: {payload.category_name}"
            body = self._build_email_body(payload)

            msg = MIMEText(body, "plain")
            msg["Subject"] = subject
            msg["From"] = self.from_addr
            msg["To"] = ", ".join(self.to_addrs)

            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)

            logger.info("email_notification_sent", category=payload.category_name)
            return True
        except Exception as e:
            logger.error(
                "email_notification_failed",
                category=payload.category_name,
                error=str(e),
            )
            return False

    def _build_email_body(self, payload: NotificationPayload) -> str:
        """Build email body from payload."""
        lines = [
            f"Radar Pipeline Completion Report",
            f"================================",
            f"",
            f"Category: {payload.category_name}",
            f"Timestamp: {payload.timestamp.isoformat()}",
            f"",
            f"Statistics:",
            f"  Sources: {payload.sources_count}",
            f"  Collected: {payload.collected_count}",
            f"  Matched: {payload.matched_count}",
            f"  Errors: {payload.errors_count}",
        ]
        if payload.report_url:
            lines.append(f"")
            lines.append(f"Report: {payload.report_url}")
        return "\n".join(lines)


class WebhookNotifier:
    """Send notifications via HTTP webhook."""

    def __init__(
        self,
        url: str,
        method: str = "POST",
        headers: dict[str, str] | None = None,
    ) -> None:
        """Initialize webhook notifier.

        Args:
            url: Webhook URL
            method: HTTP method (POST or GET)
            headers: Optional HTTP headers
        """
        self.url = url
        self.method = method.upper()
        self.headers = headers or {}

    def send(self, payload: NotificationPayload) -> bool:
        """Send webhook notification.

        Args:
            payload: Notification payload

        Returns:
            True if successful, False otherwise
        """
        try:
            if self.method == "POST":
                response = requests.post(
                    self.url,
                    json=payload.to_dict(),
                    headers=self.headers,
                    timeout=10,
                )
            elif self.method == "GET":
                response = requests.get(
                    self.url,
                    headers=self.headers,
                    timeout=10,
                )
            else:
                logger.error(
                    "webhook_invalid_method",
                    method=self.method,
                    url=self.url,
                )
                return False

            if response.status_code >= 400:
                logger.error(
                    "webhook_notification_failed",
                    url=self.url,
                    status_code=response.status_code,
                )
                return False

            logger.info("webhook_notification_sent", url=self.url)
            return True
        except Exception as e:
            logger.error(
                "webhook_notification_failed",
                url=self.url,
                error=str(e),
            )
            return False


class CompositeNotifier:
    """Send notifications to multiple notifiers."""

    def __init__(self, notifiers: list[object]) -> None:
        """Initialize composite notifier.

        Args:
            notifiers: List of notifiers to send to
        """
        self.notifiers = notifiers

    def send(self, payload: NotificationPayload) -> bool:
        """Send notification to all notifiers.

        Args:
            payload: Notification payload

        Returns:
            True if all notifiers succeeded, False if any failed
        """
        if not self.notifiers:
            return True

        results = []
        for notifier in self.notifiers:
            try:
                result = getattr(notifier, "send")(payload)
                results.append(result)
            except Exception:
                results.append(False)
        return all(results) if results else True


# Domain-specific configuration and event classes (preserved from original)
@dataclass
class NotificationConfig:
    enabled: bool
    channels: list[str]
    email_settings: dict[str, Any] = field(default_factory=dict)
    webhook_url: str = ""
    telegram_config: dict[str, str] = field(default_factory=dict)
    rules: dict[str, Any] = field(default_factory=dict)


@dataclass
class NotificationEvent:
    title: str
    message: str
    priority: str
    event_type: str
    metadata: dict[str, Any] = field(default_factory=dict)


def detect_home_notifications(
    items: list[RawItem],
    *,
    previous_region_prices: dict[str, float],
    known_urls: set[str],
    rules: dict[str, Any],
) -> list[NotificationEvent]:
    """Detect home-specific notification events.

    Args:
        items: List of collected raw items
        previous_region_prices: Map of region to previous average price
        known_urls: Set of previously known URLs
        rules: Notification rules from config/notifications.yaml

    Returns:
        List of notification events to send
    """
    events: list[NotificationEvent] = []

    volatility_threshold = float(rules.get("price_volatility_percent", 5.0))
    watched_regions = {
        str(region).strip()
        for region in rules.get("new_listing_regions", [])
        if str(region).strip()
    }
    price_ranges = rules.get("new_listing_price_ranges", [])
    policy_keywords = [
        str(keyword).strip().lower()
        for keyword in rules.get("policy_keywords", ["정책", "규제", "대출", "세금", "청약"])
        if str(keyword).strip()
    ]

    for item in items:
        region = (item.region or "").strip()
        title = item.title.strip()

        if region and item.price is not None and region in previous_region_prices:
            base_price = previous_region_prices[region]
            if base_price > 0:
                change_pct = ((item.price - base_price) / base_price) * 100.0
                if abs(change_pct) >= volatility_threshold:
                    direction = "급등" if change_pct > 0 else "급락"
                    events.append(
                        NotificationEvent(
                            title=f"[HomeRadar] {region} 가격 {direction} 감지",
                            message=(
                                f"{title}\n"
                                f"현재 가격: {item.price:.0f}, 기준가: {base_price:.0f}, "
                                f"변동률: {change_pct:+.1f}%"
                            ),
                            priority="high"
                            if abs(change_pct) >= volatility_threshold * 2
                            else "normal",
                            event_type="price_volatility",
                            metadata={"region": region, "change_pct": change_pct, "url": item.url},
                        )
                    )

        if item.url not in known_urls:
            region_ok = not watched_regions or region in watched_regions
            price_ok = _price_in_ranges(item.price, price_ranges)
            if region_ok and price_ok:
                events.append(
                    NotificationEvent(
                        title=f"[HomeRadar] 신규 매물 감지: {title}",
                        message=(
                            f"지역: {region or '미상'}\n"
                            f"가격: {item.price if item.price is not None else '미상'}\n"
                            f"URL: {item.url}"
                        ),
                        priority="normal",
                        event_type="new_listing",
                        metadata={"region": region, "price": item.price, "url": item.url},
                    )
                )

        haystack = f"{item.title}\n{item.summary}".lower()
        if any(keyword in haystack for keyword in policy_keywords):
            events.append(
                NotificationEvent(
                    title=f"[HomeRadar] 정책 변경 신호 감지: {title}",
                    message=f"정책/규제 관련 키워드가 감지되었습니다.\nURL: {item.url}",
                    priority="high",
                    event_type="policy_change",
                    metadata={"url": item.url},
                )
            )

    return events


def _price_in_ranges(price: Optional[float], ranges: Any) -> bool:
    if price is None:
        return True
    if not isinstance(ranges, list) or not ranges:
        return True

    for price_range in ranges:
        if not isinstance(price_range, dict):
            continue
        min_price = float(price_range.get("min", float("-inf")))
        max_price = float(price_range.get("max", float("inf")))
        if min_price <= price <= max_price:
            return True
    return False
