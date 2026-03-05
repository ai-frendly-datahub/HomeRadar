# -*- coding: utf-8 -*-
"""HTML 리포트 생성 (HomeRadar)."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

from graph.graph_store import GraphStore


class HtmlReporter:
    """HomeRadar HTML 리포트 생성기."""

    def __init__(self, template_dir: str | Path = "reporters/templates"):
        """
        Initialize HTML reporter with Jinja2 environment.

        Args:
            template_dir: Path to templates directory
        """
        self.template_dir = Path(template_dir)
        self.env = Environment(
            loader=FileSystemLoader(str(self.template_dir)),
            autoescape=select_autoescape(["html", "xml"]),
        )

    def generate_report(
        self,
        store: GraphStore,
        output_path: Path,
        stats: dict[str, Any] | None = None,
    ) -> Path:
        """
        Generate daily HTML report from graph store.

        Args:
            store: GraphStore instance with collected data
            output_path: Path to write HTML report
            stats: Optional statistics dictionary

        Returns:
            Path to generated HTML file
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Fetch data from store
        recent_items = store.get_recent_items(limit=100)
        trending_entities = self._get_trending_entities(store)
        sources_stats = store.get_sources_stats()

        # Generate chart data
        chart_data = self._generate_chart_data(recent_items, trending_entities)

        # Count entities
        entity_counts = self._count_entities(recent_items)

        # Prepare stats
        if stats is None:
            stats = {
                "collected": len(recent_items),
                "sources": len(sources_stats),
                "entities": len(entity_counts),
            }

        # Load and render template
        template = self.env.get_template("daily_report.html")
        rendered = template.render(
            items=recent_items,
            items_json=[self._item_to_dict(item) for item in recent_items],
            entity_counts=entity_counts,
            trending_entities=trending_entities,
            sources_stats=sources_stats,
            chart_data=chart_data,
            generated_at=datetime.now(timezone.utc),
            stats=stats,
        )

        output_path.write_text(rendered, encoding="utf-8")
        return output_path

    def _generate_chart_data(
        self,
        items: list[dict[str, Any]],
        trending_entities: list[tuple[str, int]],
    ) -> dict[str, Any]:
        """
        Prepare data for Chart.js visualization.

        Args:
            items: List of items from store
            trending_entities: List of (entity, count) tuples

        Returns:
            Dictionary with chart datasets
        """
        # Entity distribution (bar chart)
        entity_labels = [e[0] for e in trending_entities[:12]]
        entity_values = [e[1] for e in trending_entities[:12]]

        # Timeline (line chart)
        timeline_data = self._build_timeline(items)

        # Source distribution (doughnut chart)
        source_counts = Counter(item.get("source_id", "unknown") for item in items)
        source_labels = list(source_counts.keys())
        source_values = list(source_counts.values())

        return {
            "entity_distribution": {
                "labels": entity_labels,
                "values": entity_values,
            },
            "timeline": timeline_data,
            "source_distribution": {
                "labels": source_labels,
                "values": source_values,
            },
        }

    def _build_timeline(self, items: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Build timeline data from items.

        Args:
            items: List of items with published_at timestamps

        Returns:
            Dictionary with labels and values for line chart
        """
        date_counts: dict[str, int] = {}

        for item in items:
            published = item.get("published_at")
            if not published:
                continue

            # Parse ISO format or datetime
            if isinstance(published, str):
                try:
                    dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
                except (ValueError, AttributeError):
                    continue
            else:
                dt = published

            date_key = dt.date().isoformat()
            date_counts[date_key] = date_counts.get(date_key, 0) + 1

        # Sort by date
        sorted_dates = sorted(date_counts.keys())
        return {
            "labels": sorted_dates,
            "values": [date_counts[d] for d in sorted_dates],
        }

    def _count_entities(self, items: list[dict[str, Any]]) -> Counter[str]:
        """
        Count entity occurrences across items.

        Args:
            items: List of items with entities

        Returns:
            Counter of entity names
        """
        counter: Counter[str] = Counter()

        for item in items:
            entities = item.get("entities", {})
            if isinstance(entities, dict):
                for entity_type, entity_list in entities.items():
                    if isinstance(entity_list, list):
                        counter.update(entity_list)

        return counter

    def _get_trending_entities(self, store: GraphStore, limit: int = 20) -> list[tuple[str, int]]:
        """
        Get trending entities from store.

        Args:
            store: GraphStore instance
            limit: Number of top entities to return

        Returns:
            List of (entity, count) tuples
        """
        try:
            # Try to get complex entities (most common)
            return store.get_trending_entities("complex", limit=limit)
        except Exception:
            # Fallback to empty list if query fails
            return []

    def _item_to_dict(self, item: dict[str, Any]) -> dict[str, Any]:
        """
        Convert item to JSON-serializable dictionary.

        Args:
            item: Item from store

        Returns:
            JSON-serializable dictionary
        """
        return {
            "url": item.get("url", ""),
            "title": item.get("title", ""),
            "summary": item.get("summary", ""),
            "source_id": item.get("source_id", ""),
            "published_at": (
                item.get("published_at").isoformat()
                if hasattr(item.get("published_at"), "isoformat")
                else str(item.get("published_at", ""))
            ),
            "region": item.get("region", ""),
            "district": item.get("district", ""),
            "entities": item.get("entities", {}),
        }
