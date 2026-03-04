"""
Query functions for HomeRadar graph store.

Provides high-level query interfaces for common use cases:
- View by region (Seoul, Gyeonggi, etc.)
- View by property type (Apartment, Villa, etc.)
- View by topic (price surge, GTX, redevelopment)
- View by source trust tier
"""

import os
from pathlib import Path
from typing import Any

from graph.graph_store import GraphStore
from graph.search_index import SearchIndex


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
    store: GraphStore,
    keyword: str,
    limit: int = 50,
    search_db_path: Path | None = None,
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
    if not keyword.strip() or limit <= 0:
        return []

    resolved_search_db = search_db_path or Path(
        os.getenv("HOMERADAR_SEARCH_DB_PATH", "data/search_index.db")
    )
    index = SearchIndex(resolved_search_db)
    hits = index.search(keyword, limit=limit)

    if not hits:
        return []

    links = [hit.link for hit in hits]
    placeholders = ", ".join("?" for _ in links)

    with store._connection() as conn:
        rows = conn.execute(
            f"""
            SELECT url, title, summary, source_id, published_at, region, property_type, price, area
            FROM urls
            WHERE url IN ({placeholders})
            """,
            links,
        ).fetchall()

    row_map = {str(row[0]): row for row in rows}
    result: list[dict[str, Any]] = []
    for hit in hits:
        row = row_map.get(hit.link)
        if row is None:
            continue
        result.append(
            {
                "url": str(row[0]),
                "title": str(row[1]),
                "summary": str(row[2]),
                "source_id": row[3],
                "published_at": row[4],
                "region": row[5],
                "property_type": row[6],
                "price": row[7],
                "area": row[8],
                "search_rank": hit.rank,
            }
        )
    return result


def get_transactions(
    store: GraphStore,
    region: str | None = None,
    property_type: str | None = None,
    min_price: float | None = None,
    max_price: float | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """
    Query transaction data with various filters.

    Args:
        store: GraphStore instance
        region: Filter by region (e.g., "강남구")
        property_type: Filter by property type (e.g., "아파트")
        min_price: Minimum price filter (in 만원)
        max_price: Maximum price filter (in 만원)
        start_date: Start date for transactions (ISO format or YYYY-MM-DD)
        end_date: End date for transactions (ISO format or YYYY-MM-DD)
        limit: Maximum number of results

    Returns:
        List of transaction records
    """
    with store._connection() as conn:
        conditions = ["price IS NOT NULL"]
        params = []

        if region:
            conditions.append("region LIKE ?")
            params.append(f"%{region}%")

        if property_type:
            conditions.append("property_type = ?")
            params.append(property_type)

        if min_price is not None:
            conditions.append("price >= ?")
            params.append(min_price)

        if max_price is not None:
            conditions.append("price <= ?")
            params.append(max_price)

        if start_date:
            conditions.append("published_at >= ?")
            params.append(start_date)

        if end_date:
            conditions.append("published_at <= ?")
            params.append(end_date)

        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT * FROM urls
            WHERE {where_clause}
            ORDER BY published_at DESC
            LIMIT ?
        """
        params.append(limit)

        result = conn.execute(query, params)
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]


def get_price_statistics(
    store: GraphStore,
    region: str | None = None,
    property_type: str | None = None,
) -> dict[str, Any]:
    """
    Get price statistics for transactions.

    Args:
        store: GraphStore instance
        region: Filter by region
        property_type: Filter by property type

    Returns:
        Dictionary with avg_price, min_price, max_price, count
    """
    with store._connection() as conn:
        conditions = ["price IS NOT NULL"]
        params = []

        if region:
            conditions.append("region LIKE ?")
            params.append(f"%{region}%")

        if property_type:
            conditions.append("property_type = ?")
            params.append(property_type)

        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT
                AVG(price) as avg_price,
                MIN(price) as min_price,
                MAX(price) as max_price,
                COUNT(*) as count
            FROM urls
            WHERE {where_clause}
        """

        result = conn.execute(query, params)
        row = result.fetchone()

        if row:
            return {
                "avg_price": row[0],
                "min_price": row[1],
                "max_price": row[2],
                "count": row[3],
            }

        return {"avg_price": None, "min_price": None, "max_price": None, "count": 0}
