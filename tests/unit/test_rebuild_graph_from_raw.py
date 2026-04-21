from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import yaml

from scripts.rebuild_graph_from_raw import rebuild_graph_from_raw


def _write_jsonl(path: Path, records: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(record, ensure_ascii=False, default=str) + "\n" for record in records),
        encoding="utf-8",
    )


def _write_sources(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(
            {
                "sources": [
                    {
                        "id": "hankyung_realestate",
                        "name": "Korea Economic Daily - Real Estate",
                        "type": "rss",
                        "trust_tier": "T2_professional",
                        "event_model": "market_context",
                        "verification_role": "market_corroboration",
                        "merge_policy": "cannot_override_official_transaction",
                    }
                ]
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )


def test_rebuild_graph_from_raw_skips_test_sources_and_upserts(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    sources_path = tmp_path / "sources.yaml"
    db_path = tmp_path / "homeradar.duckdb"
    now = datetime(2026, 4, 12, 10, 0, tzinfo=UTC)

    _write_sources(sources_path)
    _write_jsonl(
        raw_dir / "2026-04-12" / "hankyung_realestate.jsonl",
        [
            {
                "url": "https://example.com/market",
                "title": "서울 부동산 시장 동향",
                "summary": "강남구 아파트 시장 전망",
                "source_id": "hankyung_realestate",
                "published_at": now.isoformat(),
                "collected_at": now.isoformat(),
                "raw_data": {},
                "region": "서울",
                "property_type": "아파트",
                "price": None,
                "area": None,
            }
        ],
    )
    _write_jsonl(
        raw_dir / "2026-04-12" / "test_rss.jsonl",
        [
            {
                "url": "https://example.com/test",
                "title": "test",
                "summary": "test",
                "source_id": "test_rss",
                "published_at": now.isoformat(),
                "collected_at": now.isoformat(),
                "raw_data": {},
            }
        ],
    )

    stats = rebuild_graph_from_raw(
        raw_dir=raw_dir,
        db_path=db_path,
        sources_path=sources_path,
    )

    assert stats.scanned == 2
    assert stats.loaded == 1
    assert stats.skipped_test == 1
    assert stats.invalid == 0
    assert stats.inserted == 1
    assert stats.updated == 0

    with duckdb.connect(str(db_path), read_only=True) as conn:
        row = conn.execute(
            """
            SELECT source_id, verification_state, verification_role, event_model
            FROM urls
            """
        ).fetchone()

    assert row == (
        "hankyung_realestate",
        "market_corroboration_requires_official_source",
        "market_corroboration",
        "market_context",
    )


def test_rebuild_graph_from_raw_dry_run_does_not_create_db(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    sources_path = tmp_path / "sources.yaml"
    db_path = tmp_path / "homeradar.duckdb"
    now = datetime(2026, 4, 12, 10, 0, tzinfo=UTC)

    _write_sources(sources_path)
    _write_jsonl(
        raw_dir / "2026-04-12" / "hankyung_realestate.jsonl",
        [
            {
                "url": "https://example.com/market",
                "title": "서울 부동산 시장 동향",
                "summary": "강남구 아파트 시장 전망",
                "source_id": "hankyung_realestate",
                "published_at": now.isoformat(),
                "collected_at": now.isoformat(),
                "raw_data": {},
            }
        ],
    )

    stats = rebuild_graph_from_raw(
        raw_dir=raw_dir,
        db_path=db_path,
        sources_path=sources_path,
        dry_run=True,
    )

    assert stats.loaded == 1
    assert stats.inserted == 0
    assert stats.updated == 0
    assert not db_path.exists()
