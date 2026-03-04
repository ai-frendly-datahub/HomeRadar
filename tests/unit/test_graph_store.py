"""
Unit tests for GraphStore.
"""

import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from collectors.base import RawItem
from graph.graph_store import GraphStore, init_database


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
    now = datetime.now(timezone.utc)
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

    def test_add_empty_list(self, store):
        """Test adding empty list."""
        result = store.add_items([])

        assert result["inserted"] == 0
        assert result["updated"] == 0


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
        store.add_entities(
            sample_items[0].url, {"complex": ["래미안"], "district": ["강남구"]}
        )

        # Search for complex
        results = store.search_entities("complex", "래미안")
        assert len(results) == 1
        assert results[0]["url"] == sample_items[0].url

        # Search for district
        results = store.search_entities("district", "강남구")
        assert len(results) == 1


class TestGraphStoreQueries:
    """Tests for query methods."""

    def test_get_recent_items(self, store, sample_items):
        """Test getting recent items."""
        store.add_items(sample_items)

        items = store.get_recent_items(limit=10)

        assert len(items) == 2
        # Should be sorted by published_at DESC
        assert items[0]["url"] in [item.url for item in sample_items]

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
            published_at=datetime.now(timezone.utc),
            region=None,  # None value
            property_type=None,
        )

        result = store.add_items([item])
        assert result["inserted"] == 1

        items = store.get_recent_items()
        assert len(items) == 1
        assert items[0]["region"] is None
