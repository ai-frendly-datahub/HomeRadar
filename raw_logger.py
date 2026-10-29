from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _payload_url(payload: dict[str, Any]) -> str:
    """Return the URL used to dedupe a raw record."""
    for key in ("url", "link"):
        value = payload.get(key)
        if isinstance(value, str):
            text = value.strip()
            if text:
                return text
    return ""


class RawLogger:
    def __init__(self, raw_dir: Path):
        self.raw_dir = raw_dir

    def log(
        self,
        items: Iterable[Any],
        *,
        source_name: str,
        run_id: str | None = None,
    ) -> Path:
        """Log RawItem-like dicts to JSONL with date-partitioned path.

        The same URL is written at most once per file, including a later run
        on the same day and repeats inside a single batch. The last payload
        for a URL in the batch is the one that is eligible to be appended.
        """
        now = datetime.now(UTC)
        date_dir = self.raw_dir / now.strftime("%Y-%m-%d")
        date_dir.mkdir(parents=True, exist_ok=True)
        safe_source_name = source_name.replace("/", "_").replace("\\", "_")
        output_path = (
            date_dir / f"{safe_source_name}_{run_id}.jsonl"
            if run_id is not None
            else date_dir / f"{safe_source_name}.jsonl"
        )

        existing_links = self._load_existing_urls(output_path)
        normalized = [self._normalize_item(item) for item in items]
        winners: dict[str, dict[str, Any]] = {}
        for payload in normalized:
            url = _payload_url(payload)
            if url:
                winners[url] = payload

        with output_path.open("a", encoding="utf-8") as file_obj:
            for payload in normalized:
                url = _payload_url(payload)
                if url:
                    if winners[url] is not payload:
                        continue
                    if url in existing_links:
                        continue
                    existing_links.add(url)

                file_obj.write(json.dumps(payload, ensure_ascii=False, default=str))
                file_obj.write("\n")

        return output_path

    def _load_existing_urls(self, output_path: Path) -> set[str]:
        existing_links: set[str] = set()
        if not output_path.exists():
            return existing_links
        try:
            with output_path.open("r", encoding="utf-8") as file_obj:
                for line in file_obj:
                    if not line.strip():
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(record, dict):
                        url = _payload_url(record)
                        if url:
                            existing_links.add(url)
        except OSError:
            return set()
        return existing_links

    def log_skip(
        self,
        *,
        source_name: str,
        required_env: str,
    ) -> Path:
        """Record a keyless skip. The file stores the env var name only."""
        env_name = str(required_env).strip()
        if not env_name or any(char.isspace() for char in env_name):
            raise ValueError("required_env must be an environment variable name")

        now = datetime.now(UTC)
        date_dir = self.raw_dir / now.strftime("%Y-%m-%d")
        date_dir.mkdir(parents=True, exist_ok=True)
        safe_source_name = source_name.replace("/", "_").replace("\\", "_")
        output_path = date_dir / f"{safe_source_name}.skip.json"
        payload = {
            "record_type": "skip",
            "source_id": source_name,
            "status": "skipped_missing_env",
            "required_env": env_name,
            "skip_reason": f"{env_name} not set",
        }
        output_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        return output_path

    def _normalize_item(self, item: Any) -> dict[str, Any]:
        model_dump = getattr(item, "model_dump", None)
        if callable(model_dump):
            model_payload = model_dump(mode="json")
            if isinstance(model_payload, dict):
                return model_payload
            raise TypeError("model_dump(mode='json') must return dict")

        if isinstance(item, dict):
            return item

        raise TypeError("RawLogger expects dict items or objects with model_dump(mode='json')")
