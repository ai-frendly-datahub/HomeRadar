"""
Query functions for HomeRadar graph store.

Provides high-level query interfaces for common use cases:
- View by region (Seoul, Gyeonggi, etc.)
- View by property type (Apartment, Villa, etc.)
- View by topic (price surge, GTX, redevelopment)
- View by source trust tier
"""

from typing import Any

from graph.graph_store import GraphStore


def get_view(
    store: GraphStore,
    view_type: str,
    view_value: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """
    Get items by view type and value.

    Args:
        store: GraphStore instance
        view_type: Type of view (region, district, complex, source, etc.)
        view_value: Value to filter by (optional for some views)
        limit: Maximum number of items to return

    Returns:
        List of item dictionaries

    Supported view types:
    - "recent": Recent items (all sources)
    - "region": Items by region (view_value required)
    - "district": Items by district
    - "complex": Items mentioning specific complex
    - "source": Items from specific source
    - "price_change": Items about price changes
    """
    if view_type == "recent":
        return store.get_recent_items(limit=limit)

    if view_type == "region":
        if not view_value:
            raise ValueError("view_value required for region view")
        return store.get_by_region(view_value, limit=limit)

    if view_type == "source":
        if not view_value:
            raise ValueError("view_value required for source view")
        return store.get_recent_items(limit=limit, source_id=view_value)

    if view_type == "complex":
        if not view_value:
            raise ValueError("view_value required for complex view")
        return store.search_entities("complex", view_value, limit=limit)

    if view_type == "district":
        if not view_value:
            raise ValueError("view_value required for district view")
        return store.search_entities("district", view_value, limit=limit)

    if view_type == "project":
        if not view_value:
            raise ValueError("view_value required for project view")
        return store.search_entities("project", view_value, limit=limit)

    raise ValueError(f"Unknown view type: {view_type}")


def get_trending_entities(
    store: GraphStore, entity_type: str, limit: int = 20
) -> list[tuple[str, int]]:
    """
    Get trending entities by type.

    Args:
        store: GraphStore instance
        entity_type: Type of entity (complex, district, project)
        limit: Number of top entities to return

    Returns:
        List of (entity_value, count) tuples, sorted by count descending
    """
    with store._connection() as conn:
        result = conn.execute(
            """
            SELECT entity_value, COUNT(DISTINCT url) as mention_count
            FROM url_entities
            WHERE entity_type = ?
            GROUP BY entity_value
            ORDER BY mention_count DESC
            LIMIT ?
            """,
            [entity_type, limit],
        )
        return result.fetchall()


def get_sources_stats(store: GraphStore) -> list[dict[str, Any]]:
    """
    Get statistics for all sources.

    Returns:
        List of source statistics with counts and latest activity
    """
    with store._connection() as conn:
        result = conn.execute(
            """
            SELECT
                source_id,
                COUNT(*) as item_count,
                MAX(published_at) as latest_published,
                MAX(created_at) as latest_collected
            FROM urls
            GROUP BY source_id
            ORDER BY item_count DESC
            """
        )
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]


def search_by_keyword(
    store: GraphStore, keyword: str, limit: int = 50
) -> list[dict[str, Any]]:
    """
    Search items by keyword in title or summary.

    Args:
        store: GraphStore instance
        keyword: Keyword to search for
        limit: Maximum number of items

    Returns:
        List of matching items
    """
    with store._connection() as conn:
        query = """
            SELECT * FROM urls
            WHERE title LIKE ? OR summary LIKE ?
            ORDER BY published_at DESC
            LIMIT ?
        """
        search_pattern = f"%{keyword}%"
        result = conn.execute(query, [search_pattern, search_pattern, limit])
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]
