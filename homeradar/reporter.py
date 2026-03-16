from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from radar_core.report_utils import (
    generate_index_html as _core_generate_index_html,
    generate_report as _core_generate_report,
)

from .models import Article, CategoryConfig


def generate_report(
    *,
    category: CategoryConfig,
    articles: Iterable[Article],
    output_path: Path,
    stats: dict[str, int],
    errors: list[str] | None = None,
    store=None,
) -> Path:
    """Generate HTML report (delegates to radar-core with choropleth plugin)."""
    plugin_charts = []
    if store is not None:
        try:
            from homeradar.plugins.choropleth import get_chart_config

            chart = get_chart_config(store)
            if chart is not None:
                plugin_charts.append(chart)
        except Exception:
            pass

    return _core_generate_report(
        category=category,
        articles=articles,
        output_path=output_path,
        stats=stats,
        errors=errors,
        plugin_charts=plugin_charts if plugin_charts else None,
    )


def generate_index_html(
    report_dir: Path,
    summaries_dir: Path | None = None,
) -> Path:
    """Generate index.html (delegates to radar-core)."""
    return _core_generate_index_html(report_dir, "Home Radar")
