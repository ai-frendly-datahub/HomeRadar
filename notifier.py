from __future__ import annotations

import smtplib
from dataclasses import dataclass, field
from email.mime.text import MIMEText
from typing import Any

import requests

from collectors.base import RawItem


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


class Notifier:
    def __init__(self, config: NotificationConfig):
        self.config = config

    def send(
        self,
        title: str,
        message: str,
        priority: str = "normal",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        if not self.config.enabled:
            return

        payload = {
            "title": title,
            "message": message,
            "priority": priority,
            "metadata": metadata or {},
        }

        channels = {channel.strip().lower() for channel in self.config.channels}
        if "email" in channels:
            self._send_email(payload)
        if "webhook" in channels:
            self._send_webhook(payload)
        if "telegram" in channels:
            self._send_telegram(payload)

    def _send_email(self, payload: dict[str, Any]) -> None:
        settings = self.config.email_settings
        smtp_host = str(settings.get("smtp_host", "")).strip()
        smtp_port = int(settings.get("smtp_port", 587) or 587)
        from_address = str(settings.get("from_address", "")).strip()
        to_addresses = settings.get("to_addresses", [])
        username = str(settings.get("username", "")).strip()
        password = str(settings.get("password", "")).strip()

        if (
            not smtp_host
            or not from_address
            or not isinstance(to_addresses, list)
            or not to_addresses
        ):
            return

        msg = MIMEText(str(payload["message"]), "plain", "utf-8")
        msg["Subject"] = str(payload["title"])
        msg["From"] = from_address
        msg["To"] = ", ".join(str(addr) for addr in to_addresses)

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            if username and password:
                server.login(username, password)
            server.send_message(msg)

    def _send_webhook(self, payload: dict[str, Any]) -> None:
        if not self.config.webhook_url:
            return
        requests.post(self.config.webhook_url, json=payload, timeout=10)

    def _send_telegram(self, payload: dict[str, Any]) -> None:
        token = self.config.telegram_config.get("bot_token", "")
        chat_id = self.config.telegram_config.get("chat_id", "")
        if not token or not chat_id:
            return

        text = f"[{payload['priority'].upper()}] {payload['title']}\n{payload['message']}"
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=10,
        )


def detect_home_notifications(
    items: list[RawItem],
    *,
    previous_region_prices: dict[str, float],
    known_urls: set[str],
    rules: dict[str, Any],
) -> list[NotificationEvent]:
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


def _price_in_ranges(price: float | None, ranges: Any) -> bool:
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
