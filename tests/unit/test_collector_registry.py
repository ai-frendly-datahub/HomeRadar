"""
Integration tests for collector registry with new collectors.
"""

from unittest.mock import patch

import pytest

from collectors.onbid_collector import OnbidCollector
from collectors.registry import CollectorRegistry
from collectors.subscription_collector import SubscriptionCollector


class TestCollectorRegistry:
    """Tests for CollectorRegistry with new collectors."""

    def test_registry_has_onbid_collector(self):
        """Test that registry includes onbid collector."""
        assert "onbid" in CollectorRegistry._COLLECTORS
        assert CollectorRegistry._COLLECTORS["onbid"] == OnbidCollector

    def test_registry_has_subscription_collector(self):
        """Test that registry includes subscription collector."""
        assert "subscription" in CollectorRegistry._COLLECTORS
        assert CollectorRegistry._COLLECTORS["subscription"] == SubscriptionCollector

    def test_create_onbid_collector(self):
        """Test creating OnbidCollector through registry."""
        source_config = {
            "type": "onbid",
            "api_key": "test_key",
        }

        collector = CollectorRegistry.create_collector("onbid_test", source_config)

        assert isinstance(collector, OnbidCollector)
        assert collector.source_id == "onbid_test"
        assert collector.api_key == "test_key"

    def test_create_subscription_collector(self):
        """Test creating SubscriptionCollector through registry."""
        source_config = {
            "type": "subscription",
            "api_key": "test_key",
        }

        collector = CollectorRegistry.create_collector("subscription_test", source_config)

        assert isinstance(collector, SubscriptionCollector)
        assert collector.source_id == "subscription_test"
        assert collector.api_key == "test_key"

    def test_create_collector_case_insensitive(self):
        """Test that collector type is case-insensitive."""
        source_config = {
            "type": "ONBID",
            "api_key": "test_key",
        }

        collector = CollectorRegistry.create_collector("onbid_test", source_config)

        assert isinstance(collector, OnbidCollector)

    def test_create_collector_with_env_api_key(self):
        """Test creating collector with API key from environment."""
        source_config = {
            "type": "onbid",
        }

        with patch.dict("os.environ", {"ONBID_API_KEY": "env_key"}):
            collector = CollectorRegistry.create_collector("onbid_test", source_config)

            assert isinstance(collector, OnbidCollector)
            assert collector.api_key == "env_key"

    def test_create_unsupported_collector_raises_error(self):
        """Test that unsupported collector type raises ValueError."""
        source_config = {
            "type": "unsupported_type",
        }

        with pytest.raises(ValueError, match="Unsupported source type"):
            CollectorRegistry.create_collector("test", source_config)

    def test_register_custom_collector(self):
        """Test registering a custom collector."""
        from collectors.base import BaseCollector

        class CustomCollector(BaseCollector):
            def collect(self):
                return []

        CollectorRegistry.register_collector("custom", CustomCollector)

        assert "custom" in CollectorRegistry._COLLECTORS
        assert CollectorRegistry._COLLECTORS["custom"] == CustomCollector

        # Clean up
        del CollectorRegistry._COLLECTORS["custom"]


class TestOnbidCollectorIntegration:
    """Integration tests for OnbidCollector."""

    def test_onbid_collector_with_mock_api(self):
        """Test OnbidCollector with mocked API response."""
        source_config = {
            "type": "onbid",
            "api_key": "test_key",
        }

        collector = CollectorRegistry.create_collector("onbid_test", source_config)

        mock_response = {
            "response": {
                "body": {
                    "items": [
                        {
                            "cltrNo": "2024001",
                            "cltrNm": "서울 강남구 아파트",
                            "location": "서울 강남구",
                            "propertyType": "아파트",
                            "appraisalPrice": "500000000",
                            "winningBidPrice": "480000000",
                            "area": "84.5",
                            "bidDate": "2024-03-01",
                        }
                    ]
                }
            }
        }

        with patch.object(collector, "_make_request", return_value=mock_response):
            items = collector.collect()

            assert len(items) == 1
            assert items[0].title == "서울 강남구 아파트 - 서울 강남구"
            assert items[0].price == 480000000


class TestSubscriptionCollectorIntegration:
    """Integration tests for SubscriptionCollector."""

    def test_subscription_collector_with_mock_api(self):
        """Test SubscriptionCollector with mocked API response."""
        source_config = {
            "type": "subscription",
            "api_key": "test_key",
        }

        collector = CollectorRegistry.create_collector("subscription_test", source_config)

        mock_response = {
            "response": {
                "body": {
                    "items": [
                        {
                            "prjNo": "2024001",
                            "prjNm": "서울 강남구 래미안",
                            "location": "서울 강남구",
                            "region": "서울",
                            "propertyType": "아파트",
                            "noticeDate": "2024-03-01",
                            "subscriptionStartDate": "2024-03-05",
                            "subscriptionEndDate": "2024-03-07",
                            "competitionRate": "3.5:1",
                            "supplyPrice": "500000000",
                            "area": "84.5",
                        }
                    ]
                }
            }
        }

        with patch.object(collector, "_make_request", return_value=mock_response):
            items = collector.collect()

            assert len(items) == 1
            assert items[0].title == "서울 강남구 래미안 - 서울 강남구"
            assert items[0].price == 500000000
