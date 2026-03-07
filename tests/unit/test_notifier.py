from __future__ import annotations

from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from collectors.base import RawItem
from config_loader import load_notification_config
from notifier import NotificationConfig, Notifier, detect_home_notifications


@pytest.mark.unit
def test_notifier_sends_webhook_channel() -> None:
    notifier = Notifier(
        NotificationConfig(
            enabled=True,
            channels=["webhook"],
            webhook_url="https://example.com/webhook",
        )
    )

    with patch("notifier.requests.post") as mock_post:
        notifier.send("title", "message", priority="high")

    mock_post.assert_called_once()


@pytest.mark.unit
def test_notifier_sends_email_channel() -> None:
    notifier = Notifier(
        NotificationConfig(
            enabled=True,
            channels=["email"],
            email_settings={
                "smtp_host": "smtp.example.com",
                "smtp_port": 587,
                "from_address": "from@example.com",
                "to_addresses": ["to@example.com"],
            },
        )
    )

    with patch("notifier.smtplib.SMTP") as mock_smtp:
        notifier.send("title", "message", priority="normal")

    mock_smtp.assert_called_once()


@pytest.mark.unit
def test_notifier_sends_telegram_channel() -> None:
    notifier = Notifier(
        NotificationConfig(
            enabled=True,
            channels=["telegram"],
            telegram_config={"bot_token": "token", "chat_id": "chat"},
        )
    )

    with patch("notifier.requests.post") as mock_post:
        notifier.send("title", "message", priority="high")

    mock_post.assert_called_once()


@pytest.mark.unit
def test_load_notification_config_resolves_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("WEBHOOK_URL", "https://hooks.example")
    config_file = tmp_path / "notifications.yaml"
    config_file.write_text(
        """
notifications:
  enabled: true
  channels: [webhook]
  webhook_url: "${WEBHOOK_URL}"
  rules:
    price_volatility_percent: 5
""".strip(),
        encoding="utf-8",
    )

    config = load_notification_config(config_file)
    assert config.enabled is True
    assert config.webhook_url == "https://hooks.example"
    assert config.rules["price_volatility_percent"] == 5


@pytest.mark.unit
def test_detect_home_notifications_classifies_priority() -> None:
    item = RawItem(
        url="https://example.com/item/1",
        title="서울 강남 신규 매물",
        summary="주택 정책 변경 포함",
        source_id="test",
        published_at=datetime.now(),
        region="서울",
        property_type="아파트",
        price=120000.0,
        area=84.0,
    )

    events = detect_home_notifications(
        [item],
        previous_region_prices={"서울": 100000.0},
        known_urls=set(),
        rules={
            "price_volatility_percent": 5,
            "new_listing_regions": ["서울"],
            "new_listing_price_ranges": [{"min": 100000, "max": 130000}],
            "policy_keywords": ["정책"],
        },
    )

    event_types = {event.event_type for event in events}
    assert "price_volatility" in event_types
    assert "new_listing" in event_types
    assert "policy_change" in event_types
    assert any(event.priority == "high" for event in events)
