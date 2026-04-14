from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, cast

import yaml

from notifier import NotificationConfig


_ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")


def load_notification_config(config_path: Path) -> NotificationConfig:
    if not config_path.exists():
        return NotificationConfig(enabled=False, channels=[])

    loaded = cast(object, yaml.safe_load(config_path.read_text(encoding="utf-8")))
    root = cast(dict[str, Any], loaded if isinstance(loaded, dict) else {})
    notifications = cast(dict[str, Any], root.get("notifications", {}))

    channels = notifications.get("channels", [])
    if not isinstance(channels, list):
        channels = []

    return NotificationConfig(
        enabled=bool(notifications.get("enabled", False)),
        channels=[str(channel) for channel in channels],
        email_settings=_resolve_env_refs(cast(dict[str, Any], notifications.get("email", {}))),
        webhook_url=str(_resolve_env_refs(notifications.get("webhook_url", ""))),
        telegram_config=_resolve_env_refs(cast(dict[str, Any], notifications.get("telegram", {}))),
        rules=_resolve_env_refs(cast(dict[str, Any], notifications.get("rules", {}))),
    )


def _resolve_env_refs(value: Any) -> Any:
    if isinstance(value, str):
        return _ENV_PATTERN.sub(lambda match: os.environ.get(match.group(1), ""), value)
    if isinstance(value, list):
        return [_resolve_env_refs(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _resolve_env_refs(item) for key, item in value.items()}
    return value
