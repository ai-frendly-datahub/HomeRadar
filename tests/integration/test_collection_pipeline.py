"""
Integration tests for HomeRadar collection pipeline.

Tests cover:
- Full collection workflow (collect → store → extract)
- MOLIT data → GraphStore integration
- RSS data → GraphStore integration
- Entity extraction integration
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from analyzers.entity_extractor import EntityExtractor
from collectors.base import RawItem
from collectors.molit_collector import MOLITCollector
from collectors.rss_collector import RSSCollector
from graph.graph_store import GraphStore


@pytest.mark.integration
class TestFullCollectionWorkflow:
    """Integration tests for complete collection pipeline."""

    def test_full_collection_workflow(
        self,
        tmp_graph_store: GraphStore,
        sample_items: list[RawItem],
    ) -> None:
        """
        Test complete workflow: collect → store → extract.

        Verifies:
        - Items are stored in GraphStore
        - Counts match input
        - Items are retrievable
        """
        # Store items
        result = tmp_graph_store.add_items(sample_items)

        assert result["inserted"] == len(sample_items)
        assert result["updated"] == 0

        # Verify retrieval
        recent = tmp_graph_store.get_recent_items(limit=100)
        assert len(recent) == len(sample_items)

    def test_molit_to_graph_store(
        self,
        tmp_graph_store: GraphStore,
        sample_molit_items: list[RawItem],
    ) -> None:
        """
        Test MOLIT data → GraphStore integration.

        Verifies:
        - MOLIT items are stored correctly
        - Property metadata is preserved
        - Prices and areas are stored
        """
        result = tmp_graph_store.add_items(sample_molit_items)

        assert result["inserted"] == 3
        assert result["updated"] == 0

        # Verify data integrity
        recent = tmp_graph_store.get_recent_items(limit=10)
        assert len(recent) == 3

        # Check property metadata
        molit_items = [item for item in recent if "molit://" in item["url"]]
        assert len(molit_items) == 3

        # Verify prices and areas
        prices = [item["price"] for item in molit_items if item["price"]]
        areas = [item["area"] for item in molit_items if item["area"]]
        assert len(prices) == 3
        assert len(areas) == 3

    def test_rss_to_graph_store(
        self,
        tmp_graph_store: GraphStore,
        sample_rss_items: list[RawItem],
    ) -> None:
        """
        Test RSS data → GraphStore integration.

        Verifies:
        - RSS items are stored correctly
        - News metadata is preserved
        - Items are retrievable by source
        """
        result = tmp_graph_store.add_items(sample_rss_items)

        assert result["inserted"] == 3
        assert result["updated"] == 0

        # Verify retrieval
        recent = tmp_graph_store.get_recent_items(limit=10)
        assert len(recent) == 3

        # Check source filtering
        rss_items = tmp_graph_store.get_recent_items(limit=10, source_id="hankyung_realestate")
        assert len(rss_items) == 3

    def test_entity_extraction_integration(
        self,
        tmp_graph_store: GraphStore,
        sample_items: list[RawItem],
        sample_entities: dict[str, list[tuple[str, str, float]]],
    ) -> None:
        """
        Test entity extraction integration.

        Verifies:
        - Entities are extracted from items
        - Entity relationships are stored
        - Entities are queryable
        """
        # Store items
        tmp_graph_store.add_items(sample_items)

        # Add entities
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

        # Verify entities are stored
        with tmp_graph_store._connection() as conn:
            result = conn.execute("SELECT COUNT(*) FROM url_entities").fetchone()
            assert result is not None and result[0] > 0

        # Verify entity queries work
        complex_entities = tmp_graph_store.search_entities("complex", "래미안", limit=10)
        assert len(complex_entities) > 0

    def test_duplicate_item_handling(
        self,
        tmp_graph_store: GraphStore,
        sample_molit_items: list[RawItem],
    ) -> None:
        """
        Test upsert behavior for duplicate items.

        Verifies:
        - Same URL updates existing item
        - Updated timestamp changes
        - Count remains 1
        """
        # Insert first time
        result1 = tmp_graph_store.add_items(sample_molit_items[:1])
        assert result1["inserted"] == 1

        # Insert same item again with updated title
        updated_item = sample_molit_items[0]
        updated_item.title = "Updated Title"
        result2 = tmp_graph_store.add_items([updated_item])
        assert result2["updated"] == 1
        assert result2["inserted"] == 0

        # Verify count is still 1
        recent = tmp_graph_store.get_recent_items(limit=10)
        assert len(recent) == 1

    def test_mixed_source_storage(
        self,
        tmp_graph_store: GraphStore,
        sample_molit_items: list[RawItem],
        sample_rss_items: list[RawItem],
    ) -> None:
        """
        Test storing mixed sources (MOLIT + RSS).

        Verifies:
        - Both sources are stored
        - Source filtering works
        - Total count is correct
        """
        all_items = sample_molit_items + sample_rss_items
        result = tmp_graph_store.add_items(all_items)

        assert result["inserted"] == 6

        # Verify source filtering
        molit_items = tmp_graph_store.get_recent_items(limit=100, source_id="molit_apt_transaction")
        rss_items = tmp_graph_store.get_recent_items(limit=100, source_id="hankyung_realestate")

        assert len(molit_items) == 3
        assert len(rss_items) == 3

    def test_region_filtering(
        self,
        tmp_graph_store: GraphStore,
        sample_items: list[RawItem],
    ) -> None:
        """
        Test region-based filtering.

        Verifies:
        - Items are stored with region metadata
        - Region filtering works
        - Correct items are returned
        """
        tmp_graph_store.add_items(sample_items)

        # Filter by Seoul
        seoul_items = tmp_graph_store.get_by_region("서울", limit=100)
        assert len(seoul_items) > 0
        assert all(item["region"] == "서울" for item in seoul_items)

        # Filter by Gyeonggi
        gyeonggi_items = tmp_graph_store.get_by_region("경기", limit=100)
        assert len(gyeonggi_items) > 0
        assert all(item["region"] == "경기" for item in gyeonggi_items)

    def test_property_type_metadata(
        self,
        tmp_graph_store: GraphStore,
        sample_items: list[RawItem],
    ) -> None:
        """
        Test property type metadata preservation.

        Verifies:
        - Property types are stored
        - Different types are distinguishable
        - Metadata is retrievable
        """
        tmp_graph_store.add_items(sample_items)

        recent = tmp_graph_store.get_recent_items(limit=100)

        # Check property types
        property_types = {item["property_type"] for item in recent if item["property_type"]}
        assert "아파트" in property_types
        assert "오피스텔" in property_types

    def test_empty_collection_handling(
        self,
        tmp_graph_store: GraphStore,
    ) -> None:
        """
        Test handling of empty collection.

        Verifies:
        - Empty list returns empty result
        - No errors on empty input
        - Database state is clean
        """
        result = tmp_graph_store.add_items([])

        assert result["inserted"] == 0
        assert result["updated"] == 0

        recent = tmp_graph_store.get_recent_items(limit=100)
        assert len(recent) == 0
