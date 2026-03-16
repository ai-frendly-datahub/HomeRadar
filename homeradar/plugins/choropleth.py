"""Plotly choropleth plugin for HomeRadar — Korean regional distribution map."""

from __future__ import annotations

import html
from typing import TYPE_CHECKING, Any
from urllib.request import urlopen

if TYPE_CHECKING:
    pass


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


def _normalize_to_sido(region_name: str) -> str | None:
    cleaned = region_name.strip()
    if not cleaned:
        return None
    if cleaned in SIDO_ALIASES:
        return SIDO_ALIASES[cleaned]
    for alias, sido in SIDO_ALIASES.items():
        if cleaned.startswith(alias):
            return sido
    return None


def _query_region_nodes(store: Any) -> list[tuple[str, int]]:
    try:
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
    except Exception:
        return []


def _query_url_regions(store: Any) -> list[tuple[str, int]]:
    try:
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
    except Exception:
        return []


def _get_sido_distribution(store: Any) -> list[dict[str, Any]]:
    region_rows = _query_region_nodes(store)
    if not region_rows:
        region_rows = _query_url_regions(store)

    distribution_counter: dict[str, int] = {}
    for region_name, count in region_rows:
        sido = _normalize_to_sido(region_name)
        if sido is None:
            continue
        distribution_counter[sido] = distribution_counter.get(sido, 0) + int(count)

    ordered = sorted(distribution_counter.items(), key=lambda x: x[1], reverse=True)
    return [
        {"sido": sido, "sido_full": SIDO_FULL_NAMES[sido], "count": count}
        for sido, count in ordered
        if sido in SIDO_FULL_NAMES
    ]


def _load_korea_geojson() -> dict[str, Any]:
    import json

    for url in KOREA_GEOJSON_URLS:
        try:
            with urlopen(url, timeout=15) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            if isinstance(payload.get("features"), list) and payload["features"]:
                return payload
        except Exception:
            continue
    raise RuntimeError("Unable to load Korea GeoJSON data")


def _build_korea_choropleth_html(
    regional_distribution: list[dict[str, Any]],
    geojson: dict[str, Any],
) -> str:
    try:
        import plotly.express as px
    except ImportError as err:
        raise RuntimeError("plotly dependency is missing") from err

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


def _render_fallback_table(regional_distribution: list[dict[str, Any]]) -> str:
    if not regional_distribution:
        return '<p class="muted small">No regional distribution data available.</p>'
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
            f"<tr><td><span class='mono'>{sido}</span> "
            f"<span class='muted small'>{sido_full}</span></td>"
            f"<td><span style='display:inline-block;width:{width}%;background:rgba(51,214,197,{alpha:.2f})'>&nbsp;</span></td>"
            f"<td class='mono'>{count}</td></tr>"
        )
    return (
        '<div class="region-table-wrap">'
        '<table class="region-table">'
        "<thead><tr><th>지역</th><th>분포</th><th>건수</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table></div>"
    )


def get_chart_config(store: Any, articles: Any = None) -> dict[str, Any] | None:
    """Generate Plotly choropleth chart config for plugin slot.

    Args:
        store: GraphStore instance with collected data
        articles: Unused (for API compatibility)

    Returns:
        Plugin config dict with id, title, config_json — or None if unavailable
    """
    try:
        regional_distribution = _get_sido_distribution(store)
        if not regional_distribution:
            return None
        try:
            geojson = _load_korea_geojson()
            chart_html = _build_korea_choropleth_html(regional_distribution, geojson)
        except Exception:
            chart_html = _render_fallback_table(regional_distribution)
        return {
            "id": "choropleth",
            "title": "지역별 분포 (Regional Distribution)",
            "config_json": chart_html,
        }
    except Exception:
        return None
