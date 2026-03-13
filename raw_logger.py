from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


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
        """Log RawItem-like dicts to JSONL with date-partitioned path."""
        now = datetime.now(UTC)
        date_dir = self.raw_dir / now.strftime("%Y-%m-%d")
        date_dir.mkdir(parents=True, exist_ok=True)
        safe_source_name = source_name.replace("/", "_").replace("\\", "_")
        output_path = (
            date_dir / f"{safe_source_name}_{run_id}.jsonl"
            if run_id is not None
            else date_dir / f"{safe_source_name}.jsonl"
        )

        existing_links: set[str] = set()
        if run_id is not None and output_path.exists():
            try:
                with output_path.open("r", encoding="utf-8") as file_obj:
                    for line in file_obj:
                        if not line.strip():
                            continue
                        record = json.loads(line)
                        link = record.get("link") or record.get("url")
                        if isinstance(link, str) and link:
                            existing_links.add(link)
            except (json.JSONDecodeError, OSError):
                pass

        with output_path.open("a", encoding="utf-8") as file_obj:
            for item in items:
                payload = self._normalize_item(item)
                link: str | None = None
                if run_id is not None:
                    candidate = payload.get("link") or payload.get("url")
                    if isinstance(candidate, str):
                        link = candidate
                    if isinstance(link, str) and link in existing_links:
                        continue

                file_obj.write(json.dumps(payload, ensure_ascii=False, default=str))
                file_obj.write("\n")

                if run_id is not None and link:
                    existing_links.add(link)

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
