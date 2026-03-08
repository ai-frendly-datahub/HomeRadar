from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


class RawLogger:
    def __init__(self, raw_dir: Path):
        self.raw_dir = raw_dir

    def log(self, items: Iterable[Any], *, source_name: str) -> Path:
        """Log RawItem-like dicts to JSONL with date-partitioned path."""
        date_dir = self.raw_dir / datetime.now().strftime("%Y-%m-%d")
        date_dir.mkdir(parents=True, exist_ok=True)
        output_path = date_dir / f"{source_name}.jsonl"

        with output_path.open("a", encoding="utf-8") as file:
            for item in items:
                payload = self._normalize_item(item)
                file.write(json.dumps(payload, ensure_ascii=False, default=str))
                file.write("\n")

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
