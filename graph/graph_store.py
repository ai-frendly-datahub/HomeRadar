"""
DuckDB-based graph storage for HomeRadar.

Stores real estate news, transactions, and their relationships
in a graph structure optimized for queries.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Any

import duckdb

from collectors.base import RawItem
from exceptions import StorageError


DB_ENV_VAR = "HOMERADAR_DB_PATH"
DEFAULT_DB_PATH = Path("data") / "homeradar.duckdb"


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
        path = DEFAULT_DB_PATH

    # Ensure parent directory exists
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


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

                -- Scoring and timestamps
                score DOUBLE DEFAULT 0.0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

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

        # Create indexes for common queries
        conn.execute("CREATE INDEX IF NOT EXISTS idx_urls_published ON urls(published_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_urls_source ON urls(source_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_urls_region ON urls(region)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_entities_type ON url_entities(entity_type)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_entities_value ON url_entities(entity_value)")

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
                conn.execute("BEGIN TRANSACTION")
                for item in items:
                    existing = conn.execute(
                        "SELECT url FROM urls WHERE url = ?", [item.url]
                    ).fetchone()
                    now = datetime.now()

                    conn.execute(
                        """
                        INSERT INTO urls (
                            url, title, summary, source_id, published_at,
                            region, district, property_type, price, area,
                            created_at, updated_at, last_seen_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (url) DO UPDATE SET
                            title = EXCLUDED.title,
                            summary = EXCLUDED.summary,
                            source_id = EXCLUDED.source_id,
                            published_at = EXCLUDED.published_at,
                            region = EXCLUDED.region,
                            district = EXCLUDED.district,
                            property_type = EXCLUDED.property_type,
                            price = EXCLUDED.price,
                            area = EXCLUDED.area,
                            updated_at = EXCLUDED.updated_at,
                            last_seen_at = EXCLUDED.last_seen_at
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
                            now,
                            now,
                            now,
                        ],
                    )

                    if existing:
                        updated += 1
                    else:
                        inserted += 1

                conn.execute("COMMIT")
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
                    ORDER BY published_at DESC
                    LIMIT ?
                """
                result = conn.execute(query, [source_id, limit])
            else:
                query = """
                    SELECT * FROM urls
                    ORDER BY published_at DESC
                    LIMIT ?
                """
                result = conn.execute(query, [limit])

            columns = [desc[0] for desc in result.description]
            return [dict(zip(columns, row)) for row in result.fetchall()]

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
                conn.execute("BEGIN TRANSACTION")
                count_row = conn.execute(
                    "SELECT COUNT(*) FROM urls WHERE COALESCE(published_at, last_seen_at) < ?",
                    [cutoff],
                ).fetchone()
                to_delete = int(count_row[0]) if count_row else 0
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
                conn.execute("COMMIT")
                return to_delete
        except duckdb.Error as exc:
            raise StorageError(f"Failed to delete expired HomeRadar records: {exc}") from exc

    def create_daily_snapshot(self, snapshot_dir: Optional[str] = None) -> Optional[Path]:
        """Create a daily snapshot of the database.

        Args:
            snapshot_dir: Optional directory for snapshots.
                         Defaults to ``<db_parent>/daily``.

        Returns:
            Path to the created snapshot file, or ``None`` if the
            source database does not exist.
        """
        from .date_storage import snapshot_database

        snapshot_root = Path(snapshot_dir) if snapshot_dir else self.db_path.parent / "daily"
        return snapshot_database(self.db_path, snapshot_root=snapshot_root)

    def cleanup_old_snapshots(self, snapshot_dir: Optional[str] = None, keep_days: int = 90) -> int:
        """Remove snapshot date-directories older than *keep_days*.

        Args:
            snapshot_dir: Optional directory containing snapshots.
                         Defaults to ``<db_parent>/daily``.
            keep_days: Number of days to retain.

        Returns:
            Number of directories removed.
        """
        from .date_storage import cleanup_date_directories

        snapshot_root = Path(snapshot_dir) if snapshot_dir else self.db_path.parent / "daily"
        return cleanup_date_directories(snapshot_root, keep_days=keep_days)

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
