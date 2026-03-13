from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from collectors.base import RawItem
from raw_logger import RawLogger


@pytest.mark.unit
def test_log_writes_pydantic_items_as_jsonl(tmp_path: Path) -> None:
    logger = RawLogger(tmp_path / "raw")
    items = [
        RawItem(
            url="https://example.com/a",
            title="강남 거래",
            summary="요약",
            source_id="molit_apt_transaction",
            published_at=datetime(2026, 3, 4, 10, 0, 0, tzinfo=UTC),
            region="강남구",
            property_type="아파트",
            price=125000.0,
            area=84.9,
        )
    ]

    output_path = logger.log(items, source_name="molit")

    assert output_path.parent.name.count("-") == 2
    assert output_path.name == "molit.jsonl"

    lines = output_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1

    payload = json.loads(lines[0])
    assert payload["url"] == "https://example.com/a"
    assert payload["title"] == "강남 거래"
    assert payload["published_at"].startswith("2026-03-04")


@pytest.mark.unit
def test_log_returns_path_for_empty_input(tmp_path: Path) -> None:
    logger = RawLogger(tmp_path / "raw")

    output_path = logger.log([], source_name="rss")

    assert output_path.exists()
    assert output_path.read_text(encoding="utf-8") == ""
