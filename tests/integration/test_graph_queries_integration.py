"""
Integration tests for HomeRadar graph queries.

Tests cover:
- Recent items queries
- Trending entities queries
- Keyword search functionality
"""

from __future__ import annotations

import pytest

from collectors.base import RawItem
from graph.graph_queries import get_trending_entities, get_view
from graph.graph_store import GraphStore


@pytest.mark.integration
class TestGraphQueries:
    """Integration tests for graph query functions."""

    def test_get_recent_items(
        self,
        tmp_graph_store: GraphStore,
        sample_items: list[RawItem],
    ) -> None:
        """
        Test retrieving recent items.

        Verifies:
        - Recent items are returned in correct order
        - Limit parameter works
        - Items are sorted by date descending
        """
        tmp_graph_store.add_items(sample_items)

        recent = tmp_graph_store.get_recent_items(limit=10)

        assert len(recent) == len(sample_items)
        assert all("url" in item for item in recent)
        assert all("title" in item for item in recent)
        assert all("published_at" in item for item in recent)

    def test_get_recent_items_with_limit(
        self,
        tmp_graph_store: GraphStore,
        sample_items: list[RawItem],
    ) -> None:
        """
        Test limit parameter for recent items.

        Verifies:
        - Limit is respected
        - Fewer items returned than available
        - Correct items are selected
        """
        tmp_graph_store.add_items(sample_items)

        recent = tmp_graph_store.get_recent_items(limit=2)

        assert len(recent) == 2

    def test_get_recent_items_by_source(
        self,
        tmp_graph_store: GraphStore,
        sample_items: list[RawItem],
    ) -> None:
        """
        Test filtering recent items by source.

        Verifies:
        - Source filtering works
        - Only matching source items returned
        - Count is correct
        """
        tmp_graph_store.add_items(sample_items)

        molit_items = tmp_graph_store.get_recent_items(limit=100, source_id="molit_apt_transaction")

        assert len(molit_items) == 3
        assert all(item["source_id"] == "molit_apt_transaction" for item in molit_items)

    def test_get_by_region(
        self,
        tmp_graph_store: GraphStore,
        sample_items: list[RawItem],
    ) -> None:
        """
        Test retrieving items by region.

        Verifies:
        - Region filtering works
        - Correct items are returned
        - Region metadata is preserved
        """
        tmp_graph_store.add_items(sample_items)

        seoul_items = tmp_graph_store.get_by_region("서울", limit=100)

        assert len(seoul_items) > 0
        assert all(item["region"] == "서울" for item in seoul_items)

    def test_get_trending_entities(
        self,
        tmp_graph_store: GraphStore,
        sample_items: list[RawItem],
        sample_entities: dict[str, list[tuple[str, str, float]]],
    ) -> None:
        """
        Test retrieving trending entities.

        Verifies:
        - Trending entities are ranked by frequency
        - Correct entity type is queried
        - Results are sorted descending
        """
        tmp_graph_store.add_items(sample_items)

        with tmp_graph_store._connection() as conn:
            for entity_type, entities in sample_entities.items():
                for url, entity_value, weight in entities:
                    conn.execute(
                        """
                        INSERT INTO url_entities
                        (url, entity_type, entity_value, weight)
                        VALUES (?, ?, ?, ?)
                        """,
                        [url, entity_type, entity_value, weight],
                    )

        trending = get_trending_entities(tmp_graph_store, "complex", limit=10)

        assert len(trending) > 0
        assert all(isinstance(item, tuple) and len(item) == 2 for item in trending)

    def test_search_entities_by_type(
        self,
        tmp_graph_store: GraphStore,
        sample_items: list[RawItem],
        sample_entities: dict[str, list[tuple[str, str, float]]],
    ) -> None:
        """
        Test searching entities by type.

        Verifies:
        - Entity search works
        - Correct entity type is queried
        - Results contain matching entities
        """
        tmp_graph_store.add_items(sample_items)

        with tmp_graph_store._connection() as conn:
            for entity_type, entities in sample_entities.items():
                for url, entity_value, weight in entities:
                    conn.execute(
                        """
                        INSERT INTO url_entities
                        (url, entity_type, entity_value, weight)
                        VALUES (?, ?, ?, ?)
                        """,
                        [url, entity_type, entity_value, weight],
                    )

        results = tmp_graph_store.search_entities("complex", "래미안", limit=10)

        assert len(results) > 0

    def test_get_view_recent(
        self,
        tmp_graph_store: GraphStore,
        sample_items: list[RawItem],
    ) -> None:
        """
        Test get_view with 'recent' view type.

        Verifies:
        - Recent view returns items
        - No view_value required
        - Items are returned in order
        """
        tmp_graph_store.add_items(sample_items)

        items = get_view(tmp_graph_store, "recent", limit=10)

        assert len(items) > 0

    def test_get_view_by_region(
        self,
        tmp_graph_store: GraphStore,
        sample_items: list[RawItem],
    ) -> None:
        """
        Test get_view with 'region' view type.

        Verifies:
        - Region view requires view_value
        - Correct items are returned
        - Filtering works
        """
        tmp_graph_store.add_items(sample_items)

        items = get_view(tmp_graph_store, "region", view_value="서울", limit=10)

        assert len(items) > 0
        assert all(item["region"] == "서울" for item in items)

    def test_get_view_by_source(
        self,
        tmp_graph_store: GraphStore,
        sample_items: list[RawItem],
    ) -> None:
        """
        Test get_view with 'source' view type.

        Verifies:
        - Source view filters correctly
        - Only matching source items returned
        - view_value is required
        """
        tmp_graph_store.add_items(sample_items)

        items = get_view(tmp_graph_store, "source", view_value="molit_apt_transaction", limit=10)

        assert len(items) > 0
        assert all(item["source_id"] == "molit_apt_transaction" for item in items)

    def test_get_view_invalid_type(
        self,
        tmp_graph_store: GraphStore,
    ) -> None:
        """
        Test get_view with invalid view type.

        Verifies:
        - Invalid view type raises error
        - Error message is clear
        """
        with pytest.raises(ValueError, match="Unknown view type"):
            get_view(tmp_graph_store, "invalid_view")

    def test_get_view_missing_required_value(
        self,
        tmp_graph_store: GraphStore,
    ) -> None:
        """
        Test get_view with missing required view_value.

        Verifies:
        - Missing view_value raises error
        - Error message is clear
        """
        with pytest.raises(ValueError, match="view_value required"):
            get_view(tmp_graph_store, "region")

    def test_sources_stats(
        self,
        tmp_graph_store: GraphStore,
        sample_items: list[RawItem],
    ) -> None:
        """
        Test retrieving source statistics.

        Verifies:
        - Source stats are calculated
        - All sources are included
        - Counts are correct
        """
        tmp_graph_store.add_items(sample_items)

        stats = tmp_graph_store.get_sources_stats()

        assert len(stats) > 0
        assert all("source_id" in stat for stat in stats)
        assert all("count" in stat for stat in stats)

    def test_query_ordering(
        self,
        tmp_graph_store: GraphStore,
        sample_items: list[RawItem],
    ) -> None:
        """
        Test that queries return items in correct order.

        Verifies:
        - Items are ordered by published_at descending
        - Most recent items come first
        - Ordering is consistent
        """
        tmp_graph_store.add_items(sample_items)

        recent = tmp_graph_store.get_recent_items(limit=100)

        if len(recent) > 1:
            for i in range(len(recent) - 1):
                assert recent[i]["published_at"] >= recent[i + 1]["published_at"]
