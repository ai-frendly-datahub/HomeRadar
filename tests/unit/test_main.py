"""
Unit tests for main.py orchestration.
"""

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, mock_open, patch

import pytest

from collectors.base import RawItem
from main import (
    collect_from_sources,
    collect_molit,
    load_sources,
    missing_required_env,
    run_collection_cycle,
    store_and_extract,
)


class TestLoadSources:
    """Tests for load_sources function."""

    def test_load_sources_success(self):
        """Test loading sources from YAML file."""
        yaml_content = """
sources:
  - id: test_source
    name: Test Source
    type: rss
    enabled: true
"""
        with patch("builtins.open", mock_open(read_data=yaml_content)):
            config = load_sources("test.yaml")

            assert "sources" in config
            assert len(config["sources"]) == 1
            assert config["sources"][0]["id"] == "test_source"

    def test_load_sources_file_not_found(self):
        """Test error handling when file not found."""
        with patch("builtins.open", side_effect=FileNotFoundError):
            with pytest.raises(FileNotFoundError):
                load_sources("nonexistent.yaml")

    def test_real_sources_include_data_quality_metadata(self):
        """HomeRadar P0 official sources should expose freshness and canonical keys."""
        config_path = Path(__file__).resolve().parents[2] / "config" / "sources.yaml"
        config = load_sources(str(config_path))

        assert "data_quality" in config
        assert config["data_quality"]["weakest_dimension"] == "verification"
        assert "verification_matrix" in config["data_quality"]
        backlog = config.get("source_backlog", {}).get("listing_inventory_candidates", [])
        assert {candidate["id"] for candidate in backlog} >= {
            "naver_land_main",
            "zigbang_browser",
            "kb_kbland",
        }

        sources = {source["id"]: source for source in config["sources"]}
        assert sources["hankyung_realestate"]["scope_filter"]["mode"] == (
            "require_home_entity"
        )
        assert sources["korea_pressrelease_news"]["scope_filter"] == {
            "mode": "require_home_entity",
            "apply_to_collection": True,
            "apply_to_report": True,
            "reason": (
                "Broad government press-release feed; keep only housing, construction, "
                "policy, transaction, subscription, or development items."
            ),
        }
        for source_id in (
            "molit_apt_transaction",
            "molit_apt_rent",
            "reb_subscription",
            "onbid_auction",
        ):
            source = sources[source_id]
            assert source["enabled"] is True
            assert source["freshness_sla_days"] >= 1
            assert source["observed_date_field"] == "collected_at"
            assert source["canonical_key_fields"]
            assert source["verification_role"].startswith("official_primary")


class TestCollectFromSources:
    """Tests for collect_from_sources function."""

    @pytest.fixture
    def sample_sources(self):
        """Sample source configurations."""
        return [
            {
                "id": "test_rss",
                "name": "Test RSS",
                "type": "rss",
                "enabled": True,
                "url": "http://example.com/rss",
            },
            {
                "id": "test_disabled",
                "name": "Disabled Source",
                "type": "rss",
                "enabled": False,
                "url": "http://example.com/disabled",
            },
        ]

    @pytest.fixture
    def sample_items(self):
        """Sample RawItem objects."""
        return [
            RawItem(
                url="http://example.com/1",
                title="Test 1",
                summary="Summary 1",
                source_id="test_rss",
                published_at=datetime.now(tz=UTC),
                region=None,
                property_type=None,
                price=None,
                area=None,
            ),
            RawItem(
                url="http://example.com/2",
                title="Test 2",
                summary="Summary 2",
                source_id="test_rss",
                published_at=datetime.now(tz=UTC),
                region=None,
                property_type=None,
                price=None,
                area=None,
            ),
        ]

    def test_collect_enabled_only(self, sample_sources, sample_items):
        """Test collecting from enabled sources only."""
        with patch("main.CollectorRegistry.create_collector") as mock_create:
            mock_collector = Mock()
            mock_collector.collect.return_value = sample_items
            mock_create.return_value = mock_collector

            items = collect_from_sources(sample_sources, enabled_only=True)

            assert len(items) == 2
            # Should only create collector for enabled source
            assert mock_create.call_count == 1

    def test_collect_with_source_filter(self, sample_sources, sample_items):
        """Test collecting with source filter."""
        with patch("main.CollectorRegistry.create_collector") as mock_create:
            mock_collector = Mock()
            mock_collector.collect.return_value = sample_items
            mock_create.return_value = mock_collector

            _ = collect_from_sources(
                sample_sources, enabled_only=False, source_filter=["test_disabled"]
            )

            # Should collect from filtered source even if disabled
            assert mock_create.call_count == 1
            mock_create.assert_called_with("test_disabled", sample_sources[1])

    def test_collect_handles_collector_error(self, sample_sources):
        """Test error handling when collector fails."""
        with patch("main.CollectorRegistry.create_collector") as mock_create:
            mock_create.side_effect = Exception("Collector error")

            # Should not raise, just log error and continue
            items = collect_from_sources(sample_sources, enabled_only=True)

            assert items == []

    def test_collect_logs_raw_items_per_source(self, sample_sources, sample_items):
        """Test that raw logger is called per source collection."""
        with patch("main.CollectorRegistry.create_collector") as mock_create:
            mock_collector = Mock()
            mock_collector.collect.return_value = sample_items
            mock_create.return_value = mock_collector

            with patch("main.RawLogger") as mock_logger_class:
                mock_logger = Mock()
                mock_logger_class.return_value = mock_logger

                _ = collect_from_sources(sample_sources, enabled_only=True)

                mock_logger.log.assert_called_once_with(sample_items, source_name="test_rss")

    def test_collect_skips_subscription_source_without_api_key(self):
        """Official sources without required secrets should be skipped cleanly."""
        sources = [
            {
                "id": "reb_subscription",
                "name": "REB Subscription",
                "type": "subscription",
                "enabled": True,
            }
        ]

        with patch("main.CollectorRegistry.create_collector") as mock_create:
            with patch.dict("os.environ", {}, clear=True):
                items = collect_from_sources(sources, enabled_only=True)

        assert items == []
        mock_create.assert_not_called()

    def test_collect_applies_home_scope_filter(self):
        """Broad official feeds keep only rows with HomeRadar entities."""
        source = {
            "id": "korea_pressrelease_news",
            "name": "Korea Press Release",
            "type": "rss",
            "enabled": True,
            "url": "https://example.com/rss",
            "scope_filter": {
                "mode": "require_home_entity",
                "apply_to_collection": True,
                "apply_to_report": True,
            },
        }
        kept = RawItem(
            url="https://example.com/home",
            title="강남구 아파트 공급 정책 발표",
            summary="신규 주택 공급 계획",
            source_id="korea_pressrelease_news",
            published_at=datetime.now(tz=UTC),
        )
        skipped = RawItem(
            url="https://example.com/tour",
            title="국가관광전략회의 대통령 소속으로 격상",
            summary="관광기본법 개정안 국무회의 통과",
            source_id="korea_pressrelease_news",
            published_at=datetime.now(tz=UTC),
        )

        with patch("main.CollectorRegistry.create_collector") as mock_create:
            mock_collector = Mock()
            mock_collector.collect.return_value = [kept, skipped]
            mock_create.return_value = mock_collector

            with patch("main.RawLogger") as mock_logger_class:
                mock_logger = Mock()
                mock_logger_class.return_value = mock_logger

                items = collect_from_sources([source], enabled_only=True)

        assert items == [kept]
        mock_logger.log.assert_called_once_with([kept], source_name="korea_pressrelease_news")


class TestMissingRequiredEnv:
    """Tests for source secret detection."""

    def test_missing_required_env_for_subscription(self):
        source = {"id": "reb_subscription", "type": "subscription"}

        with patch.dict("os.environ", {}, clear=True):
            assert missing_required_env(source) == "SUBSCRIPTION_API_KEY"

    def test_missing_required_env_returns_none_when_env_present(self):
        source = {"id": "onbid_auction", "type": "onbid"}

        with patch.dict("os.environ", {"ONBID_API_KEY": "test-key"}, clear=True):
            assert missing_required_env(source) is None


class TestCollectMOLIT:
    """Tests for collect_molit function."""

    @pytest.fixture
    def molit_source(self):
        """MOLIT source configuration."""
        return {
            "id": "molit_apt",
            "name": "MOLIT Apartment",
            "type": "api",
            "service_key": "test_key",
            "lawd_cd": "11680",
            "deal_ymd": "202411",
        }

    def test_collect_molit_with_config(self, molit_source):
        """Test MOLIT collection with config parameters."""
        mock_collector = Mock()
        mock_collector.collect.return_value = []

        items = collect_molit(mock_collector, molit_source)

        mock_collector.collect.assert_called_once_with("11680", "202411")
        assert items == []

    def test_collect_molit_with_env_service_key(self):
        """Test MOLIT collection with service key from environment."""
        source = {
            "id": "molit_apt",
            "type": "api",
        }

        mock_collector = Mock()
        mock_collector.collect.return_value = []

        with patch.dict("os.environ", {"MOLIT_SERVICE_KEY": "env_key"}):
            _ = collect_molit(mock_collector, source)

            # Should add service_key from environment
            assert source["service_key"] == "env_key"

    def test_collect_molit_without_service_key(self):
        """Test MOLIT collection without service key."""
        source = {
            "id": "molit_apt",
            "type": "api",
        }

        mock_collector = Mock()

        with patch.dict("os.environ", {}, clear=True):
            items = collect_molit(mock_collector, source)

            # Should return empty list without service key
            assert items == []
            mock_collector.collect.assert_not_called()

    def test_collect_molit_handles_api_error(self, molit_source):
        """Test error handling when MOLIT API fails."""
        mock_collector = Mock()
        mock_collector.collect.side_effect = RuntimeError("API error")

        items = collect_molit(mock_collector, molit_source)

        # Should return empty list on error
        assert items == []


class TestStoreAndExtract:
    """Tests for store_and_extract function."""

    @pytest.fixture
    def sample_items(self):
        """Sample RawItem objects."""
        return [
            RawItem(
                url="http://example.com/1",
                title="강남구 래미안 아파트 급등",
                summary="가격이 상승했습니다",
                source_id="test",
                published_at=datetime.now(tz=UTC),
                region=None,
                property_type=None,
                price=None,
                area=None,
            ),
        ]

    def test_store_and_extract_success(self, sample_items):
        """Test successful store and extract."""
        mock_store = Mock()
        mock_store.add_items.return_value = {"inserted": 1, "updated": 0}
        mock_store.add_entities.return_value = 3

        with patch("main.EntityExtractor") as mock_extractor_class:
            mock_extractor = Mock()
            mock_extractor.extract_from_item.return_value = {
                "complex": ["래미안"],
                "district": ["강남구"],
                "keyword": ["급등"],
            }
            mock_extractor_class.return_value = mock_extractor

            stats = store_and_extract(sample_items, mock_store)

            assert stats["stored"] == 1
            assert stats["entities"] == 3
            assert stats["matched"] == 1
            mock_store.add_items.assert_called_once_with(sample_items)

    def test_store_and_extract_empty_items(self):
        """Test with empty items list."""
        mock_store = Mock()

        stats = store_and_extract([], mock_store)

        assert stats["stored"] == 0
        assert stats["entities"] == 0
        assert stats["matched"] == 0
        mock_store.add_items.assert_not_called()

    def test_store_and_extract_handles_entity_error(self, sample_items):
        """Test error handling during entity extraction."""
        mock_store = Mock()
        mock_store.add_items.return_value = {"inserted": 1, "updated": 0}
        mock_store.add_entities.side_effect = Exception("Entity error")

        with patch("main.EntityExtractor") as mock_extractor_class:
            mock_extractor = Mock()
            mock_extractor.extract_from_item.return_value = {"complex": ["래미안"]}
            mock_extractor_class.return_value = mock_extractor

            # Should not raise, just log error
            stats = store_and_extract(sample_items, mock_store)

            assert stats["stored"] == 1
            assert stats["entities"] == 0
            assert stats["matched"] == 0

    def test_store_and_extract_syncs_items_to_search_index(self, sample_items):
        """Test that stored items are upserted into search index."""
        mock_store = Mock()
        mock_store.add_items.return_value = {"inserted": 1, "updated": 0}
        mock_store.add_entities.return_value = 0

        with patch("main.EntityExtractor") as mock_extractor_class:
            mock_extractor = Mock()
            mock_extractor.extract_from_item.return_value = {}
            mock_extractor_class.return_value = mock_extractor

            with patch("main.SearchIndex") as mock_index_class:
                mock_index = Mock()
                mock_index_class.return_value = mock_index

                _ = store_and_extract(sample_items, mock_store)

                mock_index.upsert.assert_called_once_with(
                    sample_items[0].url,
                    sample_items[0].title,
                    sample_items[0].summary,
                )


class TestRunCollectionCycle:
    """Tests for run_collection_cycle function."""

    @pytest.fixture
    def sample_config(self):
        """Sample configuration."""
        return {
            "sources": [
                {
                    "id": "test_source",
                    "name": "Test",
                    "type": "rss",
                    "enabled": True,
                    "url": "http://example.com/rss",
                }
            ]
        }

    def test_run_collection_cycle_success(self, sample_config, tmp_path):
        """Test successful collection cycle."""
        settings = SimpleNamespace(
            database_path=tmp_path / "data" / "homeradar.duckdb",
            report_dir=tmp_path / "reports",
            raw_data_dir=tmp_path / "raw",
            search_db_path=tmp_path / "search_index.db",
        )
        sample_items = [
            RawItem(
                url="http://example.com/1",
                title="Test",
                summary="Summary",
                source_id="test",
                published_at=datetime.now(tz=UTC),
                region=None,
                property_type="apartment",
                price=None,
                area=None,
            )
        ]

        with patch("main.load_settings", return_value=settings):
            with patch("main.GraphStore") as mock_store_class:
                mock_store = Mock()
                mock_store.db_path = settings.database_path
                mock_store.get_stats.return_value = {
                    "total_urls": 1,
                    "total_entities": 0,
                }
                mock_store_class.return_value = mock_store

                with patch("main.collect_from_sources", return_value=sample_items):
                    with patch(
                        "main.store_and_extract",
                        return_value={"stored": 1, "entities": 0, "matched": 0},
                    ):
                        result = run_collection_cycle(sample_config)

                        assert result["success"] is True
                        assert result["items_collected"] == 1
                        assert result["items_stored"] == 1
                        assert "duration_seconds" in result
                        assert Path(result["quality_report_path"]).exists()

    def test_run_collection_cycle_writes_dated_report_and_snapshot(
        self, sample_config, tmp_path
    ):
        """Daily collection should preserve date-stamped report and DB snapshot."""
        db_path = tmp_path / "data" / "homeradar.duckdb"
        db_path.parent.mkdir(parents=True)
        db_path.write_text("fake duckdb")
        settings = SimpleNamespace(
            database_path=db_path,
            report_dir=tmp_path / "reports",
            raw_data_dir=tmp_path / "raw",
            search_db_path=tmp_path / "search_index.db",
        )
        sample_items = [
            RawItem(
                url="http://example.com/1",
                title="Test",
                summary="Summary",
                source_id="test_source",
                published_at=datetime.now(tz=UTC),
                region=None,
                property_type="apartment",
                price=None,
                area=None,
            )
        ]

        def _write_report(_store, output_path, stats=None, quality_report=None):
            _ = stats
            assert quality_report is not None
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text("<html>daily</html>", encoding="utf-8")
            return output_path

        with patch("main.load_settings", return_value=settings):
            with patch("main.GraphStore") as mock_store_class:
                mock_store = Mock()
                mock_store.db_path = db_path
                mock_store.delete_older_than.return_value = 0
                mock_store.get_stats.return_value = {
                    "total_urls": 1,
                    "total_entities": 0,
                }
                mock_store_class.return_value = mock_store

                with patch("main.collect_from_sources", return_value=sample_items):
                    with patch(
                        "main.store_and_extract",
                        return_value={"stored": 1, "entities": 0, "matched": 1},
                    ):
                        with patch("main.HtmlReporter") as mock_reporter_class:
                            mock_reporter = Mock()
                            mock_reporter.generate_report.side_effect = _write_report
                            mock_reporter_class.return_value = mock_reporter
                            with patch("main.generate_index_html") as mock_generate_index:
                                mock_generate_index.return_value = settings.report_dir / "index.html"

                                result = run_collection_cycle(
                                    sample_config,
                                    generate_report=True,
                                    snapshot_db=True,
                                )

        report_path = Path(result["report_path"])
        latest_report_path = Path(result["latest_report_path"])
        snapshot_path = Path(str(result["date_storage"]["snapshot_path"]))
        quality_path = Path(result["quality_report_path"])
        dated_quality_path = Path(result["dated_quality_report_path"])
        summary_path = Path(result["summary_report_path"])

        assert result["success"] is True
        assert report_path.name.startswith("daily_report_")
        assert report_path.name.endswith(".html")
        assert report_path.exists()
        assert latest_report_path == settings.report_dir / "daily_report.html"
        assert latest_report_path.exists()
        assert snapshot_path.exists()
        assert snapshot_path.parent.name == datetime.now(UTC).date().isoformat()
        assert quality_path == settings.report_dir / "home_quality.json"
        assert quality_path.exists()
        assert dated_quality_path.exists()
        assert summary_path.name.startswith("home_")
        assert summary_path.name.endswith("_summary.json")
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        assert summary["category"] == "home"
        assert summary["article_count"] == 1
        assert summary["source_count"] == 1
        assert summary["matched_count"] == 1
        assert summary["ontology"]["repo"] == "HomeRadar"
        assert summary["ontology"]["ontology_version"] == "0.1.0"
        assert "home.transaction_price" in summary["ontology"]["event_model_ids"]
        assert summary["quality_summary"]["official_primary_status"] == "not_configured"
        assert summary["quality_summary"]["verification_state_count"] == 0
        assert summary["quality_summary"]["skipped_sources"] == 0

    def test_run_collection_cycle_handles_error(self, sample_config):
        """Test error handling in collection cycle."""
        with patch("main.GraphStore", side_effect=Exception("Database error")):
            result = run_collection_cycle(sample_config)

            assert result["success"] is False
            assert "error" in result
            assert result["error"] == "Database error"


class TestSummaryArticlesEventModelWiring:
    """Cycle 11 partial wiring: ``_summary_articles`` enriches each article with
    a contract-bound ``event_model_payload`` whenever the storage row carries an
    ``event_model`` column matching the HomeRadar runtime contract."""

    def _make_store(self, rows):
        store = Mock()
        store.get_recent_items.return_value = rows
        store._connection.return_value.__enter__ = Mock(
            return_value=Mock(
                execute=Mock(return_value=Mock(fetchall=Mock(return_value=[])))
            )
        )
        store._connection.return_value.__exit__ = Mock(return_value=False)
        return store

    def test_summary_articles_attaches_full_payload_for_policy_context(self):
        from main import _summary_articles

        rows = [
            {
                "url": "https://policy.example/release/1",
                "title": "Tax revision announced",
                "summary": "context",
                "source_id": "korea_policy_news",
                "published_at": "2026-04-15T00:00:00Z",
                "created_at": "2026-04-16T00:00:00Z",
                "event_model": "policy_context",
                "region": "서울",
            }
        ]
        articles = _summary_articles(self._make_store(rows), [], limit=10)
        assert len(articles) == 1
        payload = articles[0].get("event_model_payload")
        assert payload is not None, "policy_context should yield a full payload"
        assert payload == {
            "title": "Tax revision announced",
            "published_date": "2026-04-15T00:00:00Z",
            "source_url": "https://policy.example/release/1",
        }

    def test_summary_articles_partial_payload_for_transaction_price(self):
        from main import _summary_articles

        rows = [
            {
                "url": "https://land.example/deal/9",
                "title": "Apt sale",
                "summary": "",
                "source_id": "molit_apt_transaction",
                "published_at": "2026-04-22T00:00:00Z",
                "event_model": "transaction_price",
            }
        ]
        articles = _summary_articles(self._make_store(rows), [], limit=10)
        payload = articles[0]["event_model_payload"]
        # lawd_cd / deal_date are absent on the row → opt-in partial payload
        # carrying source_url only (still emitter-meaningful for traceability).
        assert payload == {"source_url": "https://land.example/deal/9"}

    def test_summary_articles_skips_payload_without_event_model_column(self):
        from main import _summary_articles

        rows = [
            {
                "url": "https://misc.example/x",
                "title": "Untagged",
                "summary": "",
                "source_id": "misc",
                "published_at": "2026-04-23T00:00:00Z",
                "event_model": None,
            }
        ]
        articles = _summary_articles(self._make_store(rows), [], limit=10)
        # No event_model on the row → no enrichment key added.
        assert "event_model_payload" not in articles[0]

    def test_summary_articles_skips_payload_when_listing_inventory_unmappable(self):
        from main import _summary_articles

        # listing_inventory requires platform/region_code/observed_at — none of
        # which exist on the storage row schema → build_event_model_payload
        # returns an empty dict and the helper skips the key.
        rows = [
            {
                "url": "https://naver.land.example/x/4",
                "title": "Listing",
                "summary": "",
                "source_id": "naver_realty",
                "published_at": "2026-04-23T00:00:00Z",
                "event_model": "listing_inventory",
                "region": "서울",
            }
        ]
        articles = _summary_articles(self._make_store(rows), [], limit=10)
        assert "event_model_payload" not in articles[0]

    def test_summary_articles_filters_broad_press_release_without_home_entity(self):
        from main import _summary_articles

        rows = [
            {
                "url": "https://press.example/home",
                "title": "강남구 아파트 공급 정책 발표",
                "summary": "신규 주택 공급 계획",
                "source_id": "korea_pressrelease_news",
                "published_at": "2026-04-23T00:00:00Z",
                "event_model": "policy_context",
            },
            {
                "url": "https://press.example/tour",
                "title": "국가관광전략회의 대통령 소속으로 격상",
                "summary": "관광기본법 개정안 국무회의 통과",
                "source_id": "korea_pressrelease_news",
                "published_at": "2026-04-23T00:00:00Z",
                "event_model": "policy_context",
            },
        ]
        source_configs = {
            "korea_pressrelease_news": {
                "scope_filter": {
                    "mode": "require_home_entity",
                    "apply_to_report": True,
                }
            }
        }

        articles = _summary_articles(
            self._make_store(rows),
            [],
            source_configs=source_configs,
            limit=10,
        )

        assert len(articles) == 1
        assert articles[0]["link"] == "https://press.example/home"
        assert articles[0]["matched_entities"]["district"] == ["강남구"]
