"""URL dedupe for the collection pipeline's raw JSONL log."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from raw_logger import RawLogger


def _urls(path: Path) -> list[str]:
    urls: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        urls.append(str(record.get("url") or record.get("link")))
    return urls


@pytest.mark.unit
def test_log_dedupes_same_url_within_batch_and_same_day_rerun(tmp_path: Path) -> None:
    logger = RawLogger(tmp_path)
    first = {"url": "https://example.com/a", "title": "first"}
    revised = {"url": "https://example.com/a", "title": "revised"}
    other = {"link": "https://example.com/b", "title": "other"}

    output_path = logger.log([first, revised, other, revised], source_name="src")

    rows = [
        json.loads(line)
        for line in output_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert [(row.get("url") or row.get("link"), row["title"]) for row in rows] == [
        ("https://example.com/a", "revised"),
        ("https://example.com/b", "other"),
    ]

    logger.log(
        [revised, other, {"url": "https://example.com/c", "title": "new"}],
        source_name="src",
    )

    assert _urls(output_path) == [
        "https://example.com/a",
        "https://example.com/b",
        "https://example.com/c",
    ]


@pytest.mark.unit
def test_log_collapses_whitespace_variants_of_the_same_url(
    tmp_path: Path,
) -> None:
    logger = RawLogger(tmp_path)
    output_path = logger.log(
        [
            {"url": " https://example.com/a ", "title": "spaced"},
            {"url": "https://example.com/a", "title": "trimmed duplicate"},
            {"title": "no-url"},
        ],
        source_name="news/src",
    )

    assert output_path.name == "news_src.jsonl"
    rows = [
        json.loads(line)
        for line in output_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(rows) == 2
    assert rows[0]["title"] == "trimmed duplicate"
    assert rows[1]["title"] == "no-url"
