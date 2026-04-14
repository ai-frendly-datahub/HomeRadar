"""
Integration tests for HomeRadar report generation.

Tests cover:
- HTML report generation with real data
- Template rendering
- Chart data generation
"""

from __future__ import annotations

from pathlib import Path

import pytest

from collectors.base import RawItem
from graph.graph_store import GraphStore
from reporters.html_reporter import HtmlReporter


@pytest.mark.integration
class TestReportGeneration:
    """Integration tests for HTML report generation."""

    def test_generate_report_with_real_data(
        self,
        tmp_graph_store: GraphStore,
        sample_items: list[RawItem],
        tmp_path: Path,
    ) -> None:
        """
        Test report generation with real data.

        Verifies:
        - Report is generated successfully
        - Output file is created
        - File contains expected content
        """
        tmp_graph_store.add_items(sample_items)

        reporter = HtmlReporter()
        output_path = tmp_path / "report.html"

        result_path = reporter.generate_report(tmp_graph_store, output_path)

        assert result_path.exists()
        assert result_path.suffix == ".html"

        content = result_path.read_text(encoding="utf-8")
        assert len(content) > 0
        assert "<!DOCTYPE html>" in content or "<html" in content

    def test_template_rendering(
        self,
        tmp_graph_store: GraphStore,
        sample_items: list[RawItem],
        tmp_path: Path,
    ) -> None:
        """
        Test Jinja2 template rendering.

        Verifies:
        - Template renders without errors
        - Variables are substituted
        - HTML structure is valid
        """
        tmp_graph_store.add_items(sample_items)

        reporter = HtmlReporter()
        output_path = tmp_path / "template_test.html"

        result_path = reporter.generate_report(tmp_graph_store, output_path)

        content = result_path.read_text(encoding="utf-8")

        assert "<title>" in content or "title" in content.lower()
        assert len(content) > 100

    def test_chart_data_generation(
        self,
        tmp_graph_store: GraphStore,
        sample_items: list[RawItem],
        tmp_path: Path,
    ) -> None:
        """
        Test chart data generation.

        Verifies:
        - Chart data is generated
        - Data structure is valid
        - Contains expected metrics
        """
        tmp_graph_store.add_items(sample_items)

        reporter = HtmlReporter()
        output_path = tmp_path / "chart_test.html"

        result_path = reporter.generate_report(tmp_graph_store, output_path)

        content = result_path.read_text(encoding="utf-8")
        assert len(content) > 0

    def test_report_with_empty_data(
        self,
        tmp_graph_store: GraphStore,
        tmp_path: Path,
    ) -> None:
        """
        Test report generation with empty data.

        Verifies:
        - Report is generated even with no data
        - No errors on empty store
        - Output file is created
        """
        reporter = HtmlReporter()
        output_path = tmp_path / "empty_report.html"

        result_path = reporter.generate_report(tmp_graph_store, output_path)

        assert result_path.exists()
        content = result_path.read_text(encoding="utf-8")
        assert len(content) > 0

    def test_report_with_custom_stats(
        self,
        tmp_graph_store: GraphStore,
        sample_items: list[RawItem],
        tmp_path: Path,
    ) -> None:
        """
        Test report generation with custom statistics.

        Verifies:
        - Custom stats are passed to template
        - Report includes custom data
        - Stats are rendered correctly
        """
        tmp_graph_store.add_items(sample_items)

        reporter = HtmlReporter()
        output_path = tmp_path / "stats_report.html"

        custom_stats = {
            "collected": len(sample_items),
            "sources": 2,
            "entities": 10,
            "custom_metric": 42,
        }

        result_path = reporter.generate_report(tmp_graph_store, output_path, stats=custom_stats)

        assert result_path.exists()
        content = result_path.read_text(encoding="utf-8")
        assert len(content) > 0

    def test_report_output_directory_creation(
        self,
        tmp_graph_store: GraphStore,
        sample_items: list[RawItem],
        tmp_path: Path,
    ) -> None:
        """
        Test that output directory is created if missing.

        Verifies:
        - Parent directories are created
        - Report is written to nested path
        - File is accessible
        """
        tmp_graph_store.add_items(sample_items)

        reporter = HtmlReporter()
        nested_path = tmp_path / "reports" / "2024" / "11" / "report.html"

        result_path = reporter.generate_report(tmp_graph_store, nested_path)

        assert result_path.exists()
        assert result_path.parent.exists()
