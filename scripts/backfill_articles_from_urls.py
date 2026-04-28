#!/usr/bin/env python3
"""Backfill HomeRadar articles rows from the existing urls table."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
RADAR_CORE_ROOT = PROJECT_ROOT.parent / "radar-core"
if RADAR_CORE_ROOT.exists():
    sys.path.insert(0, str(RADAR_CORE_ROOT))

from graph import GraphStore  # noqa: E402
from homeradar.config_loader import load_settings  # noqa: E402


def main() -> int:
    settings = load_settings()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=settings.database_path)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--overwrite-existing",
        action="store_true",
        help="refresh articles rows that already have a matching urls row",
    )
    args = parser.parse_args()

    stats = GraphStore(args.db_path).backfill_articles_from_urls(
        only_missing=not args.overwrite_existing,
        limit=args.limit,
    )
    print(json.dumps(stats, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
