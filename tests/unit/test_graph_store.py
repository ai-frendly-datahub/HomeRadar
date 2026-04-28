"""
Unit tests for GraphStore.
"""

import json
import logging
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

from collectors.base import RawItem
from graph.graph_store import GraphStore, _build_homeradar_ontology_json, init_database


@pytest.fixture
def temp_db():
    """Create temporary database for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        yield db_path


@pytest.fixture
def store(temp_db):
    """Create GraphStore instance with temp database."""
    return GraphStore(temp_db)


@pytest.fixture
def sample_items():
    """Sample RawItem objects for testing."""
    now = datetime.now(UTC)
    return [
        RawItem(
            url="https://example.com/news/gangnam-surge",
            title="Gangnam Apartment Prices Surge 10%",
            summary="Apartment prices in Gangnam district rose by 10% this month.",
            source_id="test_news",
            published_at=now,
            region="서울",
            raw_data={"district": "강남구"},
        ),
        RawItem(
            url="https://example.com/news/gtx-boost",
            title="GTX Line Opens, Property Values Rise",
            summary="The opening of GTX line increased property values in the area.",
            source_id="test_news",
            published_at=now,
            region="경기",
            raw_data={"district": "분당구"},
        ),
    ]


class TestGraphStoreInit:
    """Tests for database initialization."""

    def test_init_creates_database(self, temp_db):
        """Test that initialization creates database file."""
        assert not temp_db.exists()

        store = GraphStore(temp_db)

        assert temp_db.exists()
        assert store.db_path == temp_db

    def test_init_creates_tables(self, temp_db):
        """Test that initialization creates required tables."""
        store = GraphStore(temp_db)

        with store._connection() as conn:
            # Check urls table
            result = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='urls'"
            )
            assert result.fetchone() is not None

            # Check url_entities table
            result = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='url_entities'"
            )
            assert result.fetchone() is not None

    def test_init_database_function(self, temp_db):
        """Test init_database standalone function."""
        paths = init_database(temp_db)

        assert paths.path == temp_db
        assert temp_db.exists()


class TestGraphStoreAddItems:
    """Tests for adding items."""

    def test_add_single_item(self, store, sample_items):
        """Test adding a single item."""
        result = store.add_items([sample_items[0]])

        assert result["inserted"] == 1
        assert result["updated"] == 0

        # Verify item was added
        items = store.get_recent_items(limit=10)
        assert len(items) == 1
        assert items[0]["url"] == sample_items[0].url
        assert items[0]["title"] == sample_items[0].title

    def test_add_multiple_items(self, store, sample_items):
        """Test adding multiple items."""
        result = store.add_items(sample_items)

        assert result["inserted"] == 2
        assert result["updated"] == 0

        items = store.get_recent_items(limit=10)
        assert len(items) == 2

    def test_add_duplicate_updates(self, store, sample_items):
        """Test that adding duplicate URL updates existing item."""
        # Add first time
        result1 = store.add_items([sample_items[0]])
        assert result1["inserted"] == 1

        # Add again (should update)
        result2 = store.add_items([sample_items[0]])
        assert result2["inserted"] == 0
        assert result2["updated"] == 1

        # Should still have only one item
        items = store.get_recent_items(limit=10)
        assert len(items) == 1

    def test_add_duplicate_updates_when_entities_reference_url(self, store, sample_items):
        """Update existing URL rows without violating url_entities FK references."""
        item = sample_items[0]
        store.add_items([item])
        store.add_entities(item.url, {"district": ["강남구"], "keyword": ["청약"]})

        updated_item = RawItem(
            url=item.url,
            title="Updated Gangnam Apartment Prices",
            summary="Updated article summary.",
            source_id=item.source_id,
            published_at=item.published_at,
            region=item.region,
            raw_data={"district": "강남구"},
        )
        result = store.add_items([updated_item])

        assert result == {"inserted": 0, "updated": 1}
        with store._connection() as conn:
            row = conn.execute(
                "SELECT title FROM urls WHERE url = ?", [item.url]
            ).fetchone()
            entity_count = conn.execute(
                "SELECT COUNT(*) FROM url_entities WHERE url = ?", [item.url]
            ).fetchone()[0]

        assert row[0] == "Updated Gangnam Apartment Prices"
        assert entity_count == 0

        reinserted_entities = store.add_entities(item.url, {"district": ["강남구"], "keyword": ["청약"]})
        assert reinserted_entities == 2

    def test_add_empty_list(self, store):
        """Test adding empty list."""
        result = store.add_items([])

        assert result["inserted"] == 0
        assert result["updated"] == 0

    def test_add_item_persists_home_verification_fields(self, store):
        """Test that HomeRadar quality overlay fields are stored on urls."""
        item = RawItem(
            url="https://example.com/molit/transaction/1",
            title="MOLIT transaction",
            summary="Official transaction record.",
            source_id="molit_apt_transaction",
            published_at=datetime.now(UTC),
            region="서울",
            raw_data={
                "district": "강남구",
                "home_quality": {
                    "verification_state": "official_primary",
                    "verification_role": "official_primary_transaction",
                    "merge_policy": "authoritative_source",
                    "event_model": "transaction_price",
                },
            },
        )

        result = store.add_items([item])
        assert result["inserted"] == 1

        items = store.get_recent_items(limit=10)
        assert items[0]["verification_state"] == "official_primary"
        assert items[0]["verification_role"] == "official_primary_transaction"
        assert items[0]["merge_policy"] == "authoritative_source"
        assert items[0]["event_model"] == "transaction_price"


class TestGraphStoreEntities:
    """Tests for entity management."""

    def test_add_entities(self, store, sample_items):
        """Test adding entities to a URL."""
        # Add item first
        store.add_items([sample_items[0]])

        # Add entities
        entities = {
            "complex": ["래미안", "힐스테이트"],
            "district": ["강남구"],
            "keyword": ["급등", "가격"],
        }

        count = store.add_entities(sample_items[0].url, entities)

        assert count == 5  # 2 + 1 + 2

    def test_add_entities_with_weight(self, store, sample_items):
        """Test adding entities with custom weight."""
        store.add_items([sample_items[0]])

        entities = {"complex": ["래미안"]}
        count = store.add_entities(sample_items[0].url, entities, weight=2.0)

        assert count == 1

    def test_search_entities(self, store, sample_items):
        """Test searching by entity."""
        store.add_items(sample_items)
        store.add_entities(sample_items[0].url, {"complex": ["래미안"], "district": ["강남구"]})

        # Search for complex
        results = store.search_entities("complex", "래미안")
        assert len(results) == 1
        assert results[0]["url"] == sample_items[0].url

        # Search for district
        results = store.search_entities("district", "강남구")
        assert len(results) == 1

    def test_delete_older_than_removes_child_entities_before_urls(self, store):
        """Delete expired URL rows after their entity FK rows are removed."""
        old_item = RawItem(
            url="https://example.com/news/old",
            title="Old policy article",
            summary="Expired article with entities.",
            source_id="test_news",
            published_at=datetime.now(UTC) - timedelta(days=91),
            region="서울",
            raw_data={"district": "강남구"},
        )
        recent_item = RawItem(
            url="https://example.com/news/recent",
            title="Recent policy article",
            summary="Recent article with entities.",
            source_id="test_news",
            published_at=datetime.now(UTC),
            region="서울",
            raw_data={"district": "강남구"},
        )
        store.add_items([old_item, recent_item])
        store.add_entities(old_item.url, {"district": ["강남구"], "keyword": ["청약"]})
        store.add_entities(recent_item.url, {"district": ["강남구"]})

        deleted = store.delete_older_than(90)

        assert deleted == 1
        with store._connection() as conn:
            old_url = conn.execute(
                "SELECT COUNT(*) FROM urls WHERE url = ?", [old_item.url]
            ).fetchone()[0]
            old_entities = conn.execute(
                "SELECT COUNT(*) FROM url_entities WHERE url = ?", [old_item.url]
            ).fetchone()[0]
            recent_url = conn.execute(
                "SELECT COUNT(*) FROM urls WHERE url = ?", [recent_item.url]
            ).fetchone()[0]
            recent_entities = conn.execute(
                "SELECT COUNT(*) FROM url_entities WHERE url = ?", [recent_item.url]
            ).fetchone()[0]

        assert old_url == 0
        assert old_entities == 0
        assert recent_url == 1
        assert recent_entities == 1


class TestGraphStoreQueries:
    """Tests for query methods."""

    def test_get_recent_items(self, store, sample_items):
        """Test getting recent items."""
        store.add_items(sample_items)

        items = store.get_recent_items(limit=10)

        assert len(items) == 2
        # Should be sorted by published_at DESC
        assert items[0]["url"] in [item.url for item in sample_items]
        assert items[0]["entities"] == {}

    def test_get_recent_items_includes_entities(self, store, sample_items):
        """Recent report rows include entity maps for HTML rendering."""
        store.add_items(sample_items)
        store.add_entities(sample_items[0].url, {"district": ["강남구"], "keyword": ["청약"]})

        items = store.get_recent_items(limit=10)
        item_by_url = {item["url"]: item for item in items}

        assert item_by_url[sample_items[0].url]["entities"] == {
            "district": ["강남구"],
            "keyword": ["청약"],
        }
        assert item_by_url[sample_items[1].url]["entities"] == {}

    def test_get_recent_items_uses_observed_timestamp_fallback(self, store):
        """Recent report windows should not collapse when published ordering is sparse."""
        older_items = [
            RawItem(
                url=f"https://example.com/policy/{index}",
                title=f"Policy context {index}",
                summary="Official policy context for housing market monitoring.",
                source_id="korea_policy_news",
                published_at=datetime(2026, 3, 5, 12, index % 60, tzinfo=UTC),
                region="서울",
                raw_data={
                    "home_quality": {
                        "verification_state": "official_policy_corroboration",
                        "verification_role": "official_policy_corroboration",
                        "merge_policy": "official_context_only",
                        "event_model": "policy_context",
                    }
                },
            )
            for index in range(40)
        ]
        market_items = [
            RawItem(
                url=f"https://example.com/market/{index}",
                title=f"Market context {index}",
                summary="Market context for apartment monitoring.",
                source_id="hankyung_realestate",
                published_at=datetime(2026, 4, 10, 12, index % 60, tzinfo=UTC),
                region="서울",
                raw_data={
                    "home_quality": {
                        "verification_state": "market_corroboration_requires_official_source",
                        "verification_role": "market_corroboration",
                        "merge_policy": "cannot_override_official_transaction",
                        "event_model": "market_context",
                    }
                },
            )
            for index in range(70)
        ]
        store.add_items([*older_items, *market_items])

        items = store.get_recent_items(limit=100)

        assert len(items) == 100
        assert {item["source_id"] for item in items} == {
            "hankyung_realestate",
            "korea_policy_news",
        }
        assert items[0]["source_id"] == "hankyung_realestate"

    def test_get_recent_items_with_source_filter(self, store, sample_items):
        """Test getting recent items filtered by source."""
        store.add_items(sample_items)

        items = store.get_recent_items(limit=10, source_id="test_news")

        assert len(items) == 2

        # Test with non-existent source
        items = store.get_recent_items(limit=10, source_id="nonexistent")
        assert len(items) == 0

    def test_get_by_region(self, store, sample_items):
        """Test getting items by region."""
        store.add_items(sample_items)

        # Get Seoul items
        items = store.get_by_region("서울")
        assert len(items) == 1
        assert items[0]["region"] == "서울"

        # Get Gyeonggi items
        items = store.get_by_region("경기")
        assert len(items) == 1
        assert items[0]["region"] == "경기"

    def test_get_stats(self, store, sample_items):
        """Test getting database statistics."""
        store.add_items(sample_items)
        store.add_entities(sample_items[0].url, {"complex": ["래미안"]})

        stats = store.get_stats()

        assert stats["total_urls"] == 2
        assert stats["total_entities"] == 1
        assert "test_news" in stats["sources"]
        assert stats["sources"]["test_news"] == 2


class TestGraphStoreEdgeCases:
    """Tests for edge cases and error handling."""

    def test_query_empty_database(self, store):
        """Test queries on empty database."""
        items = store.get_recent_items()
        assert items == []

        stats = store.get_stats()
        assert stats["total_urls"] == 0
        assert stats["total_entities"] == 0

    def test_add_item_with_none_fields(self, store):
        """Test adding item with None optional fields."""
        item = RawItem(
            url="https://example.com/test",
            title="Test",
            summary="Summary",
            source_id="test",
            published_at=datetime.now(UTC),
            region=None,  # None value
            property_type=None,
        )

        result = store.add_items([item])
        assert result["inserted"] == 1

        items = store.get_recent_items()
        assert len(items) == 1
        assert items[0]["region"] is None


class TestArticlesDualWrite:
    """Cycle 14 (Option D phase 1+2): articles ontology mart dual-write."""

    @staticmethod
    def _full_item(url: str = "https://example.com/articles/molit/1") -> RawItem:
        return RawItem(
            url=url,
            title="MOLIT transaction",
            summary="Official transaction record.",
            source_id="molit_apt_transaction",
            published_at=datetime.now(UTC),
            region="서울",
            property_type="아파트",
            price=12.5,
            area=84.5,
            raw_data={
                "district": "강남구",
                "trust_tier": "T1_official",
                "info_purpose": "transaction",
                "home_quality": {
                    "verification_state": "official_primary",
                    "verification_role": "official_primary_transaction",
                    "merge_policy": "authoritative_source",
                    "event_model": "transaction_price",
                },
            },
        )

    def test_articles_table_schema(self, store):
        """`_ensure_tables` provisions an articles table with ontology_json TEXT column."""
        with store._connection() as conn:
            cols = conn.execute("PRAGMA table_info('articles')").fetchall()

        col_map = {row[1]: row[2] for row in cols}
        assert "id" in col_map and col_map["id"] == "BIGINT"
        assert "url" in col_map and col_map["url"] == "VARCHAR"
        assert "source_id" in col_map and col_map["source_id"] == "VARCHAR"
        assert "published_at" in col_map and col_map["published_at"] == "TIMESTAMP"
        assert "collected_at" in col_map and col_map["collected_at"] == "TIMESTAMP"
        assert "event_model_id" in col_map and col_map["event_model_id"] == "VARCHAR"
        # builder.py:781 looks for the literal column name "ontology_json".
        assert "ontology_json" in col_map
        assert col_map["ontology_json"] == "VARCHAR"

    def test_add_items_populates_articles_with_valid_json(self, store):
        """add_items writes a row whose ontology_json is valid JSON with domain keys."""
        item = self._full_item()

        result = store.add_items([item])
        assert result["inserted"] == 1

        with store._connection() as conn:
            row = conn.execute(
                """
                SELECT url, source_id, event_model_id, ontology_json
                FROM articles
                WHERE url = ?
                """,
                [item.url],
            ).fetchone()

        assert row is not None
        assert row[0] == item.url
        assert row[1] == "molit_apt_transaction"
        assert row[2] == "transaction_price"

        payload = json.loads(row[3])
        assert isinstance(payload, dict)
        # Domain columns serialised into ontology_json.
        assert payload["region"] == "서울"
        assert payload["district"] == "강남구"
        assert payload["property_type"] == "아파트"
        assert payload["price"] == 12.5
        assert payload["area"] == 84.5
        assert payload["trust_tier"] == "T1_official"
        assert payload["info_purpose"] == "transaction"
        assert payload["verification_state"] == "official_primary"
        assert payload["verification_role"] == "official_primary_transaction"
        # event_model_id mirrors the row-level event_model label (cycle 11 reuse).
        assert payload["event_model_id"] == "transaction_price"
        # source_role_id alias is populated for builder.py source_role_counts.
        assert payload["source_role_id"] == "official_primary_transaction"

    def test_add_items_dual_writes_urls_and_articles(self, store):
        """A single add_items call lands rows in both urls and articles tables."""
        item = self._full_item("https://example.com/articles/dual/1")

        store.add_items([item])

        with store._connection() as conn:
            urls_row = conn.execute(
                "SELECT url, region, event_model FROM urls WHERE url = ?",
                [item.url],
            ).fetchone()
            articles_row = conn.execute(
                "SELECT url, source_id, event_model_id FROM articles WHERE url = ?",
                [item.url],
            ).fetchone()

        assert urls_row is not None
        assert urls_row[0] == item.url
        assert urls_row[1] == "서울"
        assert urls_row[2] == "transaction_price"

        assert articles_row is not None
        assert articles_row[0] == item.url
        assert articles_row[1] == "molit_apt_transaction"
        assert articles_row[2] == "transaction_price"

    def test_articles_insert_failure_preserves_urls_and_logs_warning(
        self, store, caplog
    ):
        """A simulated articles INSERT failure must not break the urls upsert."""
        item = self._full_item("https://example.com/articles/failure/1")

        class _FailingArticlesConnection:
            """Proxy connection that injects an exception on articles INSERTs."""

            def __init__(self, real_conn):
                self._real = real_conn

            def __enter__(self):
                self._real.__enter__()
                return self

            def __exit__(self, exc_type, exc, tb):
                return self._real.__exit__(exc_type, exc, tb)

            def execute(self, query, *args, **kwargs):
                if isinstance(query, str) and "INSERT INTO articles" in query:
                    raise duckdb.Error("simulated articles dual-write failure")
                return self._real.execute(query, *args, **kwargs)

            def __getattr__(self, name):
                return getattr(self._real, name)

        original_connection = store._connection

        def patched_connection():
            return _FailingArticlesConnection(original_connection())

        with caplog.at_level(logging.WARNING, logger="graph.graph_store"):
            with patch.object(store, "_connection", side_effect=patched_connection):
                result = store.add_items([item])

        # urls upsert should have succeeded despite the articles failure.
        assert result == {"inserted": 1, "updated": 0}

        with store._connection() as conn:
            urls_count = conn.execute(
                "SELECT COUNT(*) FROM urls WHERE url = ?", [item.url]
            ).fetchone()[0]
            articles_count = conn.execute(
                "SELECT COUNT(*) FROM articles WHERE url = ?", [item.url]
            ).fetchone()[0]

        assert urls_count == 1
        assert articles_count == 0

        warnings = [
            record
            for record in caplog.records
            if record.levelno == logging.WARNING
            and "HomeRadar articles dual-write failed" in record.getMessage()
        ]
        assert warnings, "expected a warning log message for the failed articles dual-write"
        message = warnings[0].getMessage()
        assert item.source_id in message
        assert item.url in message

    def test_backfill_articles_from_urls_populates_missing_article_rows(self, store):
        """Historical urls rows can be copied into articles without raw replay."""
        published_at = datetime.now(UTC)
        with store._connection() as conn:
            conn.execute(
                """
                INSERT INTO urls (
                    url, title, summary, source_id, published_at,
                    region, district, property_type, price, area,
                    trust_tier, info_purpose,
                    verification_state, verification_role, merge_policy, event_model
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    "https://example.com/backfill/1",
                    "Backfill transaction",
                    "Official transaction record.",
                    "molit_apt_transaction",
                    published_at,
                    "서울",
                    "강남구",
                    "아파트",
                    12.5,
                    84.5,
                    "T1_official",
                    "transaction",
                    "official_primary",
                    "official_primary_transaction",
                    "authoritative_source",
                    "transaction_price",
                ],
            )

        stats = store.backfill_articles_from_urls()

        assert stats == {"scanned": 1, "inserted": 1, "updated": 0}
        with store._connection() as conn:
            row = conn.execute(
                """
                SELECT event_model_id, ontology_json
                FROM articles
                WHERE url = ?
                """,
                ["https://example.com/backfill/1"],
            ).fetchone()

        assert row is not None
        assert row[0] == "transaction_price"
        payload = json.loads(row[1])
        assert payload["region"] == "서울"
        assert payload["district"] == "강남구"
        assert payload["event_model_id"] == "transaction_price"
        assert payload["source_role_id"] == "official_primary_transaction"

    def test_backfill_articles_from_urls_is_idempotent_for_missing_only(self, store):
        """Default backfill skips rows that already exist in articles."""
        item = self._full_item("https://example.com/backfill/idempotent")
        store.add_items([item])

        stats = store.backfill_articles_from_urls()

        assert stats == {"scanned": 0, "inserted": 0, "updated": 0}

    def test_backfill_articles_from_urls_can_refresh_existing_rows(self, store):
        """Overwrite mode refreshes stale articles rows from urls."""
        item = self._full_item("https://example.com/backfill/refresh")
        store.add_items([item])
        with store._connection() as conn:
            conn.execute(
                "UPDATE urls SET title = ?, event_model = ? WHERE url = ?",
                ["Refreshed policy context", "policy_context", item.url],
            )

        stats = store.backfill_articles_from_urls(only_missing=False)

        assert stats == {"scanned": 1, "inserted": 0, "updated": 1}
        with store._connection() as conn:
            row = conn.execute(
                "SELECT title, event_model_id, ontology_json FROM articles WHERE url = ?",
                [item.url],
            ).fetchone()

        assert row[0] == "Refreshed policy context"
        assert row[1] == "policy_context"
        assert json.loads(row[2])["event_model_id"] == "policy_context"


class TestBuildHomeradarOntologyJson:
    """Cycle 14: ontology_json builder helper unit coverage."""

    def test_drops_empty_domain_keys(self):
        item = RawItem(
            url="https://example.com/sparse",
            title="Sparse",
            summary="",
            source_id="hankyung_realestate",
            published_at=datetime.now(UTC),
            region="서울",
            raw_data={},
        )

        payload = json.loads(_build_homeradar_ontology_json(item))

        assert payload == {"region": "서울"}

    def test_event_model_id_falls_back_to_raw_data_label(self):
        item = RawItem(
            url="https://example.com/fallback",
            title="Fallback",
            summary="",
            source_id="hankyung_realestate",
            published_at=datetime.now(UTC),
            raw_data={"event_model": "market_context"},
        )

        payload = json.loads(_build_homeradar_ontology_json(item))

        assert payload["event_model_id"] == "market_context"
