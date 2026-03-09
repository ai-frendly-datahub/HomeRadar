# -*- coding: utf-8 -*-
"""HTML 리포트 생성 (HomeRadar)."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import html
import json
from pathlib import Path
from typing import Any, Optional
from urllib.request import urlopen

from jinja2 import Environment, FileSystemLoader, select_autoescape

from graph.graph_store import GraphStore


KOREA_GEOJSON_URLS = (
    "https://raw.githubusercontent.com/southkorea/southkorea-maps/master/kostat/2013/json/skorea_provinces_geo.json",
)

SIDO_FULL_NAMES = {
    "서울": "서울특별시",
    "부산": "부산광역시",
    "대구": "대구광역시",
    "인천": "인천광역시",
    "광주": "광주광역시",
    "대전": "대전광역시",
    "울산": "울산광역시",
    "세종": "세종특별자치시",
    "경기": "경기도",
    "강원": "강원도",
    "충북": "충청북도",
    "충남": "충청남도",
    "전북": "전라북도",
    "전남": "전라남도",
    "경북": "경상북도",
    "경남": "경상남도",
    "제주": "제주특별자치도",
}

SIDO_ALIASES = {
    "서울": "서울",
    "서울시": "서울",
    "서울특별시": "서울",
    "부산": "부산",
    "부산시": "부산",
    "부산광역시": "부산",
    "대구": "대구",
    "대구시": "대구",
    "대구광역시": "대구",
    "인천": "인천",
    "인천시": "인천",
    "인천광역시": "인천",
    "광주": "광주",
    "광주시": "광주",
    "광주광역시": "광주",
    "대전": "대전",
    "대전시": "대전",
    "대전광역시": "대전",
    "울산": "울산",
    "울산시": "울산",
    "울산광역시": "울산",
    "세종": "세종",
    "세종시": "세종",
    "세종특별자치시": "세종",
    "경기": "경기",
    "경기도": "경기",
    "강원": "강원",
    "강원도": "강원",
    "충북": "충북",
    "충청북도": "충북",
    "충남": "충남",
    "충청남도": "충남",
    "전북": "전북",
    "전라북도": "전북",
    "전남": "전남",
    "전라남도": "전남",
    "경북": "경북",
    "경상북도": "경북",
    "경남": "경남",
    "경상남도": "경남",
    "제주": "제주",
    "제주시": "제주",
    "제주도": "제주",
    "제주특별자치도": "제주",
}


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
        regional_distribution = self._get_sido_distribution(store)
        regional_distribution_html, regional_distribution_fallback = (
            self._render_region_distribution(regional_distribution)
        )

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
            regional_distribution=regional_distribution,
            regional_distribution_html=regional_distribution_html,
            regional_distribution_fallback=regional_distribution_fallback,
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
            with store._connection() as conn:
                rows = conn.execute(
                    """
                    SELECT entity_value, COUNT(DISTINCT url) AS mention_count
                    FROM url_entities
                    WHERE entity_type = 'complex'
                    GROUP BY entity_value
                    ORDER BY mention_count DESC
                    LIMIT ?
                    """,
                    [limit],
                ).fetchall()

            return [(str(row[0]), int(row[1])) for row in rows]
        except Exception:
            return []

    def _get_sido_distribution(self, store: GraphStore) -> list[dict[str, Any]]:
        region_rows = self._query_region_nodes(store)
        if not region_rows:
            region_rows = self._query_url_regions(store)

        distribution_counter: dict[str, int] = {}
        for region_name, count in region_rows:
            sido = self._normalize_to_sido(region_name)
            if sido is None:
                continue
            distribution_counter[sido] = distribution_counter.get(sido, 0) + int(count)

        ordered_distribution = sorted(
            distribution_counter.items(),
            key=lambda item: item[1],
            reverse=True,
        )
        return [
            {
                "sido": sido,
                "sido_full": SIDO_FULL_NAMES[sido],
                "count": count,
            }
            for sido, count in ordered_distribution
        ]

    def _query_region_nodes(self, store: GraphStore) -> list[tuple[str, int]]:
        with store._connection() as conn:
            rows = conn.execute(
                """
                SELECT region_name, mention_count
                FROM (
                    SELECT
                        COALESCE(NULLIF(TRIM(u.region), ''), NULLIF(TRIM(e.entity_value), '')) AS region_name,
                        COUNT(DISTINCT e.url) AS mention_count
                    FROM url_entities e
                    LEFT JOIN urls u ON e.url = u.url
                    WHERE e.entity_type = 'district'
                    GROUP BY 1
                ) node_counts
                WHERE region_name IS NOT NULL
                ORDER BY mention_count DESC
                """
            ).fetchall()

        return [(str(row[0]), int(row[1])) for row in rows if row[0] is not None]

    def _query_url_regions(self, store: GraphStore) -> list[tuple[str, int]]:
        with store._connection() as conn:
            rows = conn.execute(
                """
                SELECT TRIM(region) AS region_name, COUNT(DISTINCT url) AS mention_count
                FROM urls
                WHERE region IS NOT NULL AND TRIM(region) != ''
                GROUP BY 1
                ORDER BY mention_count DESC
                """
            ).fetchall()

        return [(str(row[0]), int(row[1])) for row in rows if row[0] is not None]

    def _normalize_to_sido(self, region_name: str) -> Optional[str]:
        cleaned = region_name.strip()
        if not cleaned:
            return None

        if cleaned in SIDO_ALIASES:
            return SIDO_ALIASES[cleaned]

        for alias, sido in SIDO_ALIASES.items():
            if cleaned.startswith(alias):
                return sido

        return None

    def _render_region_distribution(
        self,
        regional_distribution: list[dict[str, Any]],
    ) -> tuple[str, bool]:
        if not regional_distribution:
            return self._render_region_fallback_table(regional_distribution), True

        try:
            geojson = self._load_korea_geojson()
            choropleth_html = self._build_korea_choropleth_html(regional_distribution, geojson)
            return choropleth_html, False
        except Exception:
            fallback_html = self._render_region_fallback_table(regional_distribution)
            return fallback_html, True

    def _load_korea_geojson(self) -> dict[str, Any]:
        for geojson_url in KOREA_GEOJSON_URLS:
            try:
                with urlopen(geojson_url, timeout=15) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                features = payload.get("features")
                if isinstance(features, list) and features:
                    return payload
            except Exception:
                continue

        raise RuntimeError("Unable to load Korea GeoJSON data")

    def _build_korea_choropleth_html(
        self,
        regional_distribution: list[dict[str, Any]],
        geojson: dict[str, Any],
    ) -> str:
        try:
            import plotly.express as px
        except ImportError as error:
            raise RuntimeError("plotly dependency is missing") from error

        data_frame = {
            "sido": [str(item["sido"]) for item in regional_distribution],
            "sido_full": [str(item["sido_full"]) for item in regional_distribution],
            "count": [int(item["count"]) for item in regional_distribution],
        }

        fig = px.choropleth_mapbox(
            data_frame=data_frame,
            geojson=geojson,
            locations="sido_full",
            featureidkey="properties.name",
            color="count",
            color_continuous_scale="Tealgrn",
            hover_name="sido",
            hover_data={"count": True, "sido_full": False},
            mapbox_style="carto-darkmatter",
            center={"lat": 36.4, "lon": 127.9},
            zoom=5,
            opacity=0.82,
        )
        fig.update_layout(
            margin={"r": 0, "t": 0, "l": 0, "b": 0},
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            coloraxis_colorbar={"title": "건수", "thickness": 14},
        )
        fig.update_traces(marker_line_width=0.8, marker_line_color="rgba(5,7,12,0.9)")

        return fig.to_html(full_html=False, include_plotlyjs="cdn")

    def _render_region_fallback_table(self, regional_distribution: list[dict[str, Any]]) -> str:
        if not regional_distribution:
            return (
                '<p class="muted small" style="margin:0">'
                "No regional distribution data available for this run."
                "</p>"
            )

        max_count = max(int(item["count"]) for item in regional_distribution)
        rows: list[str] = []
        for item in regional_distribution:
            sido = html.escape(str(item["sido"]))
            sido_full = html.escape(str(item["sido_full"]))
            count = int(item["count"])
            ratio = count / max_count if max_count else 0.0
            width = max(10, int(ratio * 100))
            alpha = 0.2 + ratio * 0.45

            rows.append(
                "<tr>"
                f'<td><span class="mono">{sido}</span> '
                f'<span class="muted small">{sido_full}</span></td>'
                f'<td class="region-bar-cell"><span class="region-bar" style="width:{width}%; '
                f'background:rgba(51,214,197,{alpha:.2f})"></span></td>'
                f'<td class="mono">{count}</td>'
                "</tr>"
            )

        body_rows = "".join(rows)
        return (
            '<div class="region-table-wrap">'
            '<table class="region-table" aria-label="Regional real estate distribution">'
            "<thead><tr><th>지역</th><th>분포</th><th>건수</th></tr></thead>"
            f"<tbody>{body_rows}</tbody>"
            "</table></div>"
        )

    def _item_to_dict(self, item: dict[str, Any]) -> dict[str, Any]:
        """
        Convert item to JSON-serializable dictionary.

        Args:
            item: Item from store

        Returns:
            JSON-serializable dictionary
        """
        published_at_value = item.get("published_at")
        isoformat_fn = getattr(published_at_value, "isoformat", None)
        if callable(isoformat_fn):
            published_at = str(isoformat_fn())
        else:
            published_at = str(published_at_value or "")

        return {
            "url": item.get("url", ""),
            "title": item.get("title", ""),
            "summary": item.get("summary", ""),
            "source_id": item.get("source_id", ""),
            "published_at": published_at,
            "region": item.get("region", ""),
            "district": item.get("district", ""),
            "entities": item.get("entities", {}),
        }


def generate_index_html(report_dir: Path) -> Path:
    """Generate an index.html that lists all available report files."""
    report_dir.mkdir(parents=True, exist_ok=True)

    html_files = sorted(
        [f for f in report_dir.glob("*.html") if f.name != "index.html"],
        key=lambda p: p.name,
    )

    reports = []
    for html_file in html_files:
        name = html_file.stem
        display_name = name.replace("_report", "").replace("_", " ").title()
        reports.append({"filename": html_file.name, "display_name": display_name})

    generated_at = datetime.now(timezone.utc).isoformat()

    if reports:
        cards_html = "\n    ".join(
            f'<div class="card"><a href="{r["filename"]}"><strong>{r["display_name"]}</strong></a></div>'
            for r in reports
        )
        body_content = f'<div class="reports">\n    {cards_html}\n  </div>'
    else:
        body_content = '<div class="empty">No reports available yet.</div>'

    html_content = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Radar Reports</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; padding: 24px; background: #f6f8fb; color: #0f172a; }}
    h1 {{ margin: 0 0 8px 0; }}
    .muted {{ color: #475569; font-size: 13px; margin-bottom: 24px; }}
    .reports {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 12px; }}
    .card {{ background: white; border: 1px solid #e2e8f0; border-radius: 10px; padding: 16px; box-shadow: 0 1px 2px rgba(0,0,0,0.04); transition: box-shadow 0.2s; }}
    .card:hover {{ box-shadow: 0 4px 6px rgba(0,0,0,0.08); }}
    a {{ color: #0f172a; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    .empty {{ text-align: center; color: #64748b; padding: 48px; }}
  </style>
</head>
<body>
  <h1>Radar Reports</h1>
  <div class="muted">Generated at {generated_at} (UTC)</div>

  {body_content}
</body>
</html>"""

    index_path = report_dir / "index.html"
    index_path.write_text(html_content, encoding="utf-8")
    return index_path
