#!/usr/bin/env python3
"""Rebuild HomeRadar graph rows from recorded raw JSONL logs."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable

import yaml


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
RADAR_CORE_ROOT = PROJECT_ROOT.parent / "radar-core"
if RADAR_CORE_ROOT.exists():
    sys.path.insert(0, str(RADAR_CORE_ROOT))

from analyzers import EntityExtractor  # noqa: E402
from collectors.base import RawItem  # noqa: E402
from graph import GraphStore  # noqa: E402
from homeradar.config_loader import load_settings  # noqa: E402
from homeradar.home_signals import build_source_lookup, enrich_home_verification_fields  # noqa: E402


@dataclass
class RebuildStats:
    scanned: int = 0
    loaded: int = 0
    skipped_test: int = 0
    invalid: int = 0
    inserted: int = 0
    updated: int = 0
    entities: int = 0


def _read_sources(sources_path: Path) -> list[dict[str, Any]]:
    loaded = yaml.safe_load(sources_path.read_text(encoding="utf-8"))
    root = loaded if isinstance(loaded, dict) else {}
    sources = root.get("sources", [])
    return [source for source in sources if isinstance(source, dict)]


def _is_test_payload(path: Path, payload: dict[str, Any]) -> bool:
    source_id = str(payload.get("source_id") or "").strip()
    return path.name.startswith("test_") or source_id.startswith("test_")


def _iter_raw_items(
    raw_dir: Path,
    *,
    include_test_sources: bool,
    stats: RebuildStats,
) -> Iterable[RawItem]:
    for path in sorted(raw_dir.glob("**/*.jsonl")):
        with path.open(encoding="utf-8") as file_obj:
            for line in file_obj:
                if not line.strip():
                    continue
                stats.scanned += 1
                try:
                    payload = json.loads(line)
                    if not isinstance(payload, dict):
                        raise ValueError("JSONL record is not an object")
                    if not include_test_sources and _is_test_payload(path, payload):
                        stats.skipped_test += 1
                        continue
                    yield RawItem(**payload)
                    stats.loaded += 1
                except Exception:
                    stats.invalid += 1


def rebuild_graph_from_raw(
    *,
    raw_dir: Path,
    db_path: Path,
    sources_path: Path,
    include_test_sources: bool = False,
    dry_run: bool = False,
) -> RebuildStats:
    stats = RebuildStats()
    sources = _read_sources(sources_path)
    items = list(
        _iter_raw_items(
            raw_dir,
            include_test_sources=include_test_sources,
            stats=stats,
        )
    )

    if dry_run or not items:
        return stats

    enriched_items = enrich_home_verification_fields(items, build_source_lookup(sources))
    store = GraphStore(db_path=db_path)
    upsert_result = store.add_items(enriched_items)
    stats.inserted = int(upsert_result["inserted"])
    stats.updated = int(upsert_result["updated"])

    extractor = EntityExtractor()
    for item in enriched_items:
        entities = extractor.extract_from_item({"title": item.title, "summary": item.summary})
        if entities:
            stats.entities += store.add_entities(item.url, entities)

    return stats


def main() -> int:
    settings = load_settings()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, default=settings.raw_data_dir)
    parser.add_argument("--db-path", type=Path, default=settings.database_path)
    parser.add_argument("--sources-path", type=Path, default=PROJECT_ROOT / "config" / "sources.yaml")
    parser.add_argument("--include-test-sources", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    stats = rebuild_graph_from_raw(
        raw_dir=args.raw_dir,
        db_path=args.db_path,
        sources_path=args.sources_path,
        include_test_sources=args.include_test_sources,
        dry_run=args.dry_run,
    )
    print(json.dumps(asdict(stats), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
