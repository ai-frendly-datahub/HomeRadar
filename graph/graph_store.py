"""
DuckDB-based graph storage for HomeRadar.

Stores real estate news, transactions, and their relationships
in a graph structure optimized for queries.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Any

import duckdb

from collectors.base import RawItem
from exceptions import StorageError


logger = logging.getLogger(__name__)


DB_ENV_VAR = "HOMERADAR_DB_PATH"
DEFAULT_DB_PATH = Path("data") / "homeradar.duckdb"

URLS_QUALITY_COLUMNS = {
    "verification_state": "TEXT",
    "verification_role": "TEXT",
    "merge_policy": "TEXT",
    "event_model": "TEXT",
}

# Cycle 14 (Option D phase 1+2): dual-write articles mart with ontology_json so that
# `articles_ontology_capable` (radar-analysis builder.py:868) flips to True without
# touching the urls read path.  HomeRadar domain columns are flattened into the
# ontology_json payload below; event_model_id mirrors the existing `event_model`
# label reused by cycle 11's `_resolve_event_model_key` helper.
HOMERADAR_ONTOLOGY_DOMAIN_KEYS = (
    "region",
    "district",
    "property_type",
    "price",
    "area",
    "trust_tier",
    "info_purpose",
    "verification_state",
    "verification_role",
)


@dataclass
class DatabasePaths:
    """Database file paths."""

    path: Path


def _resolve_db_path(db_path: Optional[Path | str] = None) -> Path:
    """
    Resolve database path from arguments or environment.

    Priority:
    1. db_path argument
    2. HOMERADAR_DB_PATH environment variable
    3. Default: data/homeradar.duckdb

    Args:
        db_path: Optional explicit path

    Returns:
        Resolved Path object
    """
    if db_path is not None:
        path = Path(db_path)
    elif db_env := os.environ.get(DB_ENV_VAR):
        path = Path(db_env)
    else:
        path = _settings_db_path()

    # Ensure parent directory exists
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _settings_db_path() -> Path:
    """Resolve the configured graph DB path, falling back to the historical default."""
    try:
        from homeradar.config_loader import load_settings

        return load_settings().database_path
    except Exception:
        return DEFAULT_DB_PATH


def _ensure_urls_quality_columns(conn: duckdb.DuckDBPyConnection) -> None:
    """Add HomeRadar quality columns to existing `urls` tables."""
    existing_columns = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info('urls')").fetchall()
    }
    for column_name, column_type in URLS_QUALITY_COLUMNS.items():
        if column_name not in existing_columns:
            conn.execute(f"ALTER TABLE urls ADD COLUMN {column_name} {column_type}")


def _text_or_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _home_quality_value(item: RawItem, key: str) -> str | None:
    raw_data = item.raw_data if isinstance(item.raw_data, dict) else {}
    home_quality = raw_data.get("home_quality")
    quality_payload = home_quality if isinstance(home_quality, dict) else {}

    if key == "verification_state":
        return _text_or_none(
            quality_payload.get(key) or raw_data.get("HomeVerificationState")
        )
    return _text_or_none(quality_payload.get(key))


def _coerce_number(value: object) -> float | None:
    """Best-effort numeric coercion for ontology_json payload values."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _build_homeradar_ontology_json(item: RawItem) -> str:
    """Serialize HomeRadar domain + quality columns into a JSON ontology payload.

    The payload mirrors the keys consumed by ``radar-analysis`` (specifically the
    ``event_model_id`` and ``source_role_id`` lookups in builder.py).  Domain
    columns that are empty or ``None`` are dropped so the resulting JSON stays
    compact and the count signals (ontology_row_count, event_model_counts) only
    fire on rows with meaningful data.
    """
    payload: dict[str, Any] = {}

    raw_data = item.raw_data if isinstance(item.raw_data, dict) else {}

    # Domain columns sourced from RawItem fields when populated; otherwise fall
    # back to raw_data (mirrors GraphStore.add_items column choices).
    candidates: dict[str, Any] = {
        "region": item.region if item.region is not None else raw_data.get("region"),
        "district": raw_data.get("district"),
        "property_type": (
            item.property_type
            if item.property_type is not None
            else raw_data.get("property_type")
        ),
        "price": item.price if item.price is not None else raw_data.get("price"),
        "area": item.area if item.area is not None else raw_data.get("area"),
        "trust_tier": raw_data.get("trust_tier"),
        "info_purpose": raw_data.get("info_purpose"),
        "verification_state": _home_quality_value(item, "verification_state"),
        "verification_role": _home_quality_value(item, "verification_role"),
    }

    for key in HOMERADAR_ONTOLOGY_DOMAIN_KEYS:
        raw_value = candidates.get(key)
        if raw_value is None:
            continue
        if key in {"price", "area"}:
            num = _coerce_number(raw_value)
            if num is None:
                continue
            payload[key] = num
        else:
            text = _text_or_none(raw_value)
            if text is None:
                continue
            payload[key] = text

    event_model_id = _home_quality_value(item, "event_model")
    if event_model_id is None:
        # Fall back to row-level raw_data label if home_quality is absent.
        event_model_id = _text_or_none(raw_data.get("event_model"))
    if event_model_id is not None:
        payload["event_model_id"] = event_model_id

    merge_policy = _home_quality_value(item, "merge_policy")
    if merge_policy is not None:
        payload["merge_policy"] = merge_policy

    source_role = _home_quality_value(item, "verification_role")
    if source_role is not None:
        # Surface the verification role as a source_role_id alias too so the
        # builder's source_role_counts can register HomeRadar without further
        # downstream wiring changes.
        payload.setdefault("source_role_id", source_role)

    return json.dumps(payload, ensure_ascii=False)


def init_database(db_path: Optional[Path | str] = None) -> DatabasePaths:
    """
    Initialize DuckDB database with required tables.

    Creates:
    - urls: Main content table
    - url_entities: Entity relationships

    Args:
        db_path: Optional database path

    Returns:
        DatabasePaths with resolved path
    """
    path = _resolve_db_path(db_path)

    with duckdb.connect(str(path)) as conn:
        # Main URLs/content table
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS urls (
                url TEXT PRIMARY KEY,
                title TEXT,
                summary TEXT,
                source_id TEXT,
                published_at TIMESTAMP,

                -- Geographic metadata
                region TEXT,              -- 지역 (서울, 경기, 부산 등)
                district TEXT,            -- 구/군 (강남구, 분당구 등)

                -- Property metadata
                property_type TEXT,       -- 아파트, 빌라, 오피스텔 등
                price DOUBLE,             -- 거래가/시세
                area DOUBLE,              -- 면적 (m²)

                -- Source metadata
                trust_tier TEXT,          -- T1_official, T2_professional, T3_aggregator
                info_purpose TEXT,        -- transaction, news, listing, subscription

                -- HomeRadar quality overlay
                verification_state TEXT,  -- HomeVerificationState for output triage
                verification_role TEXT,   -- official_primary, market_corroboration, etc.
                merge_policy TEXT,        -- authoritative_source, cannot_override_*, etc.
                event_model TEXT,         -- transaction_price, policy_context, etc.

                -- Scoring and timestamps
                score DOUBLE DEFAULT 0.0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        _ensure_urls_quality_columns(conn)

        # Entity relationships table
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS url_entities (
                url TEXT,
                entity_type TEXT,         -- complex, district, project, keyword
                entity_value TEXT,        -- 래미안, 강남구, GTX, 급등
                weight DOUBLE DEFAULT 1.0,
                first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (url, entity_type, entity_value),
                FOREIGN KEY (url) REFERENCES urls(url)
            )
            """
        )

        # Cycle 14 (Option D phase 1): articles dual-write mart.  Mirrors the
        # radar-core RadarStorage articles table layout (link/source/published
        # naming aligned with the spec) so downstream `articles_ontology_capable`
        # detection in radar-analysis fires when ontology_json rows exist.  The
        # urls table remains the source of truth for HomeRadar's read paths.
        conn.execute(
            "CREATE SEQUENCE IF NOT EXISTS articles_id_seq START 1"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS articles (
                id BIGINT PRIMARY KEY DEFAULT nextval('articles_id_seq'),
                url TEXT UNIQUE NOT NULL,
                title TEXT,
                summary TEXT,
                source_id TEXT,
                published_at TIMESTAMP,
                collected_at TIMESTAMP,
                event_model_id TEXT,
                ontology_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # Create indexes for common queries
        conn.execute("CREATE INDEX IF NOT EXISTS idx_urls_published ON urls(published_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_urls_source ON urls(source_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_urls_region ON urls(region)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_entities_type ON url_entities(entity_type)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_entities_value ON url_entities(entity_value)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_articles_category_time ON articles(source_id, published_at)"
        )

    return DatabasePaths(path=path)


class GraphStore:
    """
    Graph-based storage for HomeRadar data.

    Uses DuckDB for fast analytical queries on real estate news
    and transaction data.
    """

    def __init__(self, db_path: Optional[Path | str] = None):
        """
        Initialize GraphStore.

        Args:
            db_path: Optional database path (defaults to data/homeradar.duckdb)
        """
        self.db_paths = init_database(db_path)
        self.db_path = self.db_paths.path

    def _connection(self) -> duckdb.DuckDBPyConnection:
        """Get database connection."""
        return duckdb.connect(str(self.db_path))

    def add_items(self, items: list[RawItem]) -> dict[str, int]:
        """
        Add or update items in the database.

        Args:
            items: List of RawItem objects to store

        Returns:
            Dictionary with counts: {"inserted": N, "updated": M}
        """
        if not items:
            return {"inserted": 0, "updated": 0}

        inserted = 0
        updated = 0

        try:
            with self._connection() as conn:
                for item in items:
                    existing = conn.execute(
                        "SELECT url FROM urls WHERE url = ?", [item.url]
                    ).fetchone()
                    now = datetime.now()

                    if existing:
                        conn.execute("DELETE FROM url_entities WHERE url = ?", [item.url])
                        conn.execute(
                            """
                            UPDATE urls SET
                                title = ?,
                                summary = ?,
                                source_id = ?,
                                published_at = ?,
                                region = ?,
                                district = ?,
                                property_type = ?,
                                price = ?,
                                area = ?,
                                verification_state = ?,
                                verification_role = ?,
                                merge_policy = ?,
                                event_model = ?,
                                updated_at = ?,
                                last_seen_at = ?
                            WHERE url = ?
                            """,
                            [
                                item.title,
                                item.summary,
                                item.source_id,
                                item.published_at,
                                item.region,
                                item.raw_data.get("district"),
                                item.property_type,
                                item.price,
                                item.area,
                                _home_quality_value(item, "verification_state"),
                                _home_quality_value(item, "verification_role"),
                                _home_quality_value(item, "merge_policy"),
                                _home_quality_value(item, "event_model"),
                                now,
                                now,
                                item.url,
                            ],
                        )
                        updated += 1
                    else:
                        conn.execute(
                            """
                            INSERT INTO urls (
                                url, title, summary, source_id, published_at,
                                region, district, property_type, price, area,
                                verification_state, verification_role, merge_policy, event_model,
                                created_at, updated_at, last_seen_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            [
                                item.url,
                                item.title,
                                item.summary,
                                item.source_id,
                                item.published_at,
                                item.region,
                                item.raw_data.get("district"),
                                item.property_type,
                                item.price,
                                item.area,
                                _home_quality_value(item, "verification_state"),
                                _home_quality_value(item, "verification_role"),
                                _home_quality_value(item, "merge_policy"),
                                _home_quality_value(item, "event_model"),
                                now,
                                now,
                                now,
                            ],
                        )
                        inserted += 1

                    # Cycle 14 (Option D phase 2): best-effort dual-write into the
                    # articles ontology mart.  Failures here MUST NOT impact the
                    # urls upsert above; we log a warning and move on so the urls
                    # path stays the source of truth for HomeRadar reads.
                    try:
                        ontology_json = _build_homeradar_ontology_json(item)
                        event_model_id = _home_quality_value(item, "event_model")
                        conn.execute(
                            """
                            INSERT INTO articles (
                                url, title, summary, source_id, published_at,
                                collected_at, event_model_id, ontology_json
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT (url) DO UPDATE SET
                                title = EXCLUDED.title,
                                summary = EXCLUDED.summary,
                                source_id = EXCLUDED.source_id,
                                published_at = EXCLUDED.published_at,
                                collected_at = EXCLUDED.collected_at,
                                event_model_id = EXCLUDED.event_model_id,
                                ontology_json = EXCLUDED.ontology_json
                            """,
                            [
                                item.url,
                                item.title,
                                item.summary,
                                item.source_id,
                                item.published_at,
                                now,
                                event_model_id,
                                ontology_json,
                            ],
                        )
                    except Exception as exc:  # noqa: BLE001 - best-effort dual-write
                        logger.warning(
                            "HomeRadar articles dual-write failed for source=%s url=%s: %s",
                            item.source_id,
                            item.url,
                            exc,
                        )

        except duckdb.Error as exc:
            raise StorageError(f"Failed to upsert HomeRadar items: {exc}") from exc

        return {"inserted": inserted, "updated": updated}

    def add_entities(self, url: str, entities: dict[str, list[str]], weight: float = 1.0) -> int:
        """
        Add entity relationships for a URL.

        Args:
            url: URL to associate entities with
            entities: Dictionary mapping entity types to lists of values
                     e.g., {"complex": ["래미안", "힐스테이트"],
                            "district": ["강남구"]}
            weight: Importance weight for these entities

        Returns:
            Number of entity relationships added
        """
        count = 0
        now = datetime.now()

        try:
            with self._connection() as conn:
                conn.execute("BEGIN TRANSACTION")
                for entity_type, values in entities.items():
                    for value in values:
                        conn.execute(
                            """
                            INSERT INTO url_entities (url, entity_type, entity_value, weight, first_seen_at, last_seen_at)
                            VALUES (?, ?, ?, ?, ?, ?)
                            ON CONFLICT (url, entity_type, entity_value)
                            DO UPDATE SET
                                weight = EXCLUDED.weight,
                                last_seen_at = EXCLUDED.last_seen_at
                            """,
                            [url, entity_type, value, weight, now, now],
                        )
                        count += 1
                conn.execute("COMMIT")
        except duckdb.Error as exc:
            raise StorageError(f"Failed to upsert HomeRadar entities for '{url}': {exc}") from exc

        return count

    def get_recent_items(
        self, limit: int = 50, source_id: Optional[str] = None
    ) -> list[dict[str, Any]]:
        """
        Get recent items, optionally filtered by source.

        Args:
            limit: Maximum number of items to return
            source_id: Optional source ID filter

        Returns:
            List of item dictionaries
        """
        with self._connection() as conn:
            if source_id:
                query = """
                    SELECT * FROM urls
                    WHERE source_id = ?
                    ORDER BY COALESCE(published_at, last_seen_at, updated_at, created_at) DESC, url DESC
                    LIMIT ?
                """
                result = conn.execute(query, [source_id, limit])
            else:
                query = """
                    SELECT * FROM urls
                    ORDER BY COALESCE(published_at, last_seen_at, updated_at, created_at) DESC, url DESC
                    LIMIT ?
                """
                result = conn.execute(query, [limit])

            columns = [desc[0] for desc in result.description]
            items = [dict(zip(columns, row)) for row in result.fetchall()]
            self._attach_entities(conn, items)
            return items

    def _attach_entities(
        self,
        conn: duckdb.DuckDBPyConnection,
        items: list[dict[str, Any]],
    ) -> None:
        urls = [str(item.get("url", "")) for item in items if item.get("url")]
        for item in items:
            item["entities"] = {}
        if not urls:
            return

        placeholders = ", ".join("?" for _ in urls)
        rows = conn.execute(
            f"""
            SELECT url, entity_type, entity_value
            FROM url_entities
            WHERE url IN ({placeholders})
            ORDER BY url, entity_type, entity_value
            """,
            urls,
        ).fetchall()

        entity_map: dict[str, dict[str, list[str]]] = {}
        for url, entity_type, entity_value in rows:
            entity_map.setdefault(str(url), {}).setdefault(str(entity_type), []).append(
                str(entity_value)
            )

        for item in items:
            item["entities"] = entity_map.get(str(item.get("url", "")), {})

    def get_by_region(self, region: str, limit: int = 50) -> list[dict[str, Any]]:
        """
        Get items by region.

        Args:
            region: Region name (e.g., "서울", "경기")
            limit: Maximum number of items

        Returns:
            List of item dictionaries
        """
        with self._connection() as conn:
            query = """
                SELECT * FROM urls
                WHERE region = ?
                ORDER BY published_at DESC
                LIMIT ?
            """
            result = conn.execute(query, [region, limit])
            columns = [desc[0] for desc in result.description]
            return [dict(zip(columns, row)) for row in result.fetchall()]

    def search_entities(
        self, entity_type: str, entity_value: str, limit: int = 50
    ) -> list[dict[str, Any]]:
        """
        Search for items by entity.

        Args:
            entity_type: Entity type (complex, district, project, etc.)
            entity_value: Entity value to search for
            limit: Maximum number of items

        Returns:
            List of item dictionaries with entity information
        """
        with self._connection() as conn:
            query = """
                SELECT u.*, e.entity_type, e.entity_value, e.weight
                FROM urls u
                JOIN url_entities e ON u.url = e.url
                WHERE e.entity_type = ? AND e.entity_value = ?
                ORDER BY u.published_at DESC
                LIMIT ?
            """
            result = conn.execute(query, [entity_type, entity_value, limit])
            columns = [desc[0] for desc in result.description]
            return [dict(zip(columns, row)) for row in result.fetchall()]

    def get_stats(self) -> dict[str, Any]:
        """
        Get database statistics.

        Returns:
            Dictionary with various statistics
        """
        with self._connection() as conn:
            # Total counts
            url_count = conn.execute("SELECT COUNT(*) FROM urls").fetchone()[0]
            entity_count = conn.execute("SELECT COUNT(*) FROM url_entities").fetchone()[0]

            # Source distribution
            source_dist = conn.execute(
                """
                SELECT source_id, COUNT(*) as count
                FROM urls
                GROUP BY source_id
                ORDER BY count DESC
                """
            ).fetchall()

            # Region distribution
            region_dist = conn.execute(
                """
                SELECT region, COUNT(*) as count
                FROM urls
                WHERE region IS NOT NULL
                GROUP BY region
                ORDER BY count DESC
                """
            ).fetchall()

            # Entity type distribution
            entity_types = conn.execute(
                """
                SELECT entity_type, COUNT(DISTINCT entity_value) as unique_values
                FROM url_entities
                GROUP BY entity_type
                ORDER BY unique_values DESC
                """
            ).fetchall()

            return {
                "total_urls": url_count,
                "total_entities": entity_count,
                "sources": dict(source_dist),
                "regions": dict(region_dist),
                "entity_types": dict(entity_types),
            }

    def delete_older_than(self, days: int) -> int:
        cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=days)
        try:
            with self._connection() as conn:
                count_row = conn.execute(
                    "SELECT COUNT(*) FROM urls WHERE COALESCE(published_at, last_seen_at) < ?",
                    [cutoff],
                ).fetchone()
                to_delete = int(count_row[0]) if count_row else 0
                if to_delete == 0:
                    return 0

                # DuckDB may still enforce the URL FK against deleted child rows
                # inside the same explicit transaction. Let each delete commit
                # before moving to the parent table.
                conn.execute(
                    """
                    DELETE FROM url_entities
                    WHERE url IN (
                        SELECT url FROM urls WHERE COALESCE(published_at, last_seen_at) < ?
                    )
                    """,
                    [cutoff],
                )
                conn.execute(
                    "DELETE FROM urls WHERE COALESCE(published_at, last_seen_at) < ?",
                    [cutoff],
                )
                return to_delete
        except duckdb.Error as exc:
            raise StorageError(f"Failed to delete expired HomeRadar records: {exc}") from exc

    def get_sources_stats(self) -> list[dict[str, Any]]:
        """
        Get statistics for each source.

        Returns:
            List of dictionaries with source_id and count
        """
        with self._connection() as conn:
            result = conn.execute(
                """
                SELECT source_id, COUNT(*) as count
                FROM urls
                GROUP BY source_id
                ORDER BY count DESC
                """
            ).fetchall()
            return [{"source_id": row[0], "count": row[1]} for row in result]
