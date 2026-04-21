"""
Unit tests for SubscriptionCollector.
"""

from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from collectors.subscription_collector import SubscriptionCollector


class TestSubscriptionCollectorInit:
    """Tests for SubscriptionCollector initialization."""

    def test_init_with_api_key_from_config(self):
        """Test initialization with API key in config."""
        source = {
            "api_key": "test_key",
            "base_url": "https://api.odcloud.kr/api/15101046/v1",
        }

        collector = SubscriptionCollector("subscription_test", source)

        assert collector.source_id == "subscription_test"
        assert collector.api_key == "test_key"

    def test_init_with_api_key_from_env(self):
        """Test initialization with API key from environment."""
        source = {"base_url": "https://api.odcloud.kr/api/15101046/v1"}

        with patch.dict("os.environ", {"SUBSCRIPTION_API_KEY": "env_key"}):
            collector = SubscriptionCollector("subscription_test", source)
            assert collector.api_key == "env_key"

    def test_init_without_api_key_raises_error(self):
        """Test that initialization without API key raises ValueError."""
        source = {"base_url": "https://api.odcloud.kr/api/15101046/v1"}

        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(ValueError, match="requires 'api_key'"):
                SubscriptionCollector("subscription_test", source)

    def test_init_with_custom_num_of_rows(self):
        """Test initialization with custom num_of_rows."""
        source = {
            "api_key": "test_key",
            "num_of_rows": 50,
        }

        collector = SubscriptionCollector("subscription_test", source)

        assert collector.num_of_rows == 50

    def test_init_default_num_of_rows(self):
        """Test default num_of_rows is 100."""
        source = {"api_key": "test_key"}

        collector = SubscriptionCollector("subscription_test", source)

        assert collector.num_of_rows == 100


class TestSubscriptionCollectorCollect:
    """Tests for SubscriptionCollector.collect() method."""

    @pytest.fixture
    def collector(self):
        """Create SubscriptionCollector instance."""
        source = {
            "api_key": "test_key",
            "base_url": "https://api.odcloud.kr/api/15101046/v1",
        }
        return SubscriptionCollector("subscription_test", source)

    @pytest.fixture
    def sample_json_response(self):
        """Sample JSON response from Subscription API."""
        return {
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
                        },
                        {
                            "prjNo": "2024002",
                            "prjNm": "경기 성남시 힐스테이트",
                            "location": "경기 성남시",
                            "region": "경기",
                            "propertyType": "아파트",
                            "noticeDate": "2024-03-02",
                            "subscriptionStartDate": "2024-03-08",
                            "subscriptionEndDate": "2024-03-10",
                            "competitionRate": "2.1:1",
                            "supplyPrice": "300000000",
                            "area": "59.5",
                        },
                    ]
                }
            }
        }

    def test_collect_successful_request(self, collector, sample_json_response):
        """Test successful API request and parsing."""
        with patch.object(collector, "_make_request", return_value=sample_json_response):
            items = collector.collect()

            assert len(items) == 2
            assert items[0].title.startswith("서울 강남구 래미안")
            assert items[1].title.startswith("경기 성남시 힐스테이트")

    def test_collect_parses_prices_correctly(self, collector, sample_json_response):
        """Test that prices are parsed correctly."""
        with patch.object(collector, "_make_request", return_value=sample_json_response):
            items = collector.collect()

            assert items[0].price == 500000000
            assert items[1].price == 300000000

    def test_collect_returns_empty_list_for_no_items(self, collector):
        """Test that empty response returns empty list."""
        empty_response = {"response": {"body": {"items": []}}}

        with patch.object(collector, "_make_request", return_value=empty_response):
            items = collector.collect()

            assert items == []

    def test_collect_handles_network_error(self, collector):
        """Test handling of network errors."""
        with patch.object(collector, "_make_request", side_effect=Exception("Network error")):
            with pytest.raises(Exception):  # noqa: B017
                collector.collect()

    def test_collect_with_missing_optional_fields(self, collector):
        """Test parsing items with missing optional fields."""
        response = {
            "response": {
                "body": {
                    "items": [
                        {
                            "prjNo": "2024001",
                            "prjNm": "서울 강남구 래미안",
                            "location": "서울 강남구",
                            "region": "서울",
                            "propertyType": "아파트",
                        }
                    ]
                }
            }
        }

        with patch.object(collector, "_make_request", return_value=response):
            items = collector.collect()

            assert len(items) == 1
            assert items[0].price is None
            assert items[0].area is None


class TestSubscriptionCollectorParseItem:
    """Tests for SubscriptionCollector._parse_item() method."""

    @pytest.fixture
    def collector(self):
        """Create SubscriptionCollector instance."""
        source = {"api_key": "test_key"}
        return SubscriptionCollector("subscription_test", source)

    def test_parse_item_with_complete_data(self, collector):
        """Test parsing item with all fields present."""
        item_data = {
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

        raw_item = collector._parse_item(item_data)

        assert raw_item is not None
        assert raw_item.title == "서울 강남구 래미안 - 서울 강남구"
        assert raw_item.price == 500000000
        assert raw_item.area == 84.5
        assert raw_item.property_type == "아파트"
        assert raw_item.region == "서울"

    def test_parse_item_with_missing_essential_fields(self, collector):
        """Test that items missing essential fields return None."""
        item_data = {
            "location": "서울 강남구",
            "region": "서울",
        }

        raw_item = collector._parse_item(item_data)

        assert raw_item is None

    def test_parse_item_builds_correct_url(self, collector):
        """Test that URL is built correctly."""
        item_data = {
            "prjNo": "2024001",
            "prjNm": "서울 강남구 래미안",
        }

        raw_item = collector._parse_item(item_data)

        assert raw_item.url == "subscription://project/2024001"

    def test_parse_item_with_invalid_price(self, collector):
        """Test parsing invalid price format."""
        item_data = {
            "prjNo": "2024001",
            "prjNm": "서울 강남구 래미안",
            "supplyPrice": "invalid_price",
        }

        raw_item = collector._parse_item(item_data)

        assert raw_item is not None
        assert raw_item.price is None

    def test_parse_item_with_invalid_area(self, collector):
        """Test parsing invalid area format."""
        item_data = {
            "prjNo": "2024001",
            "prjNm": "서울 강남구 래미안",
            "area": "invalid_area",
        }

        raw_item = collector._parse_item(item_data)

        assert raw_item is not None
        assert raw_item.area is None

    def test_parse_item_with_area_units(self, collector):
        """Test parsing area with units."""
        item_data = {
            "prjNo": "2024001",
            "prjNm": "서울 강남구 래미안",
            "area": "84.5㎡",
        }

        raw_item = collector._parse_item(item_data)

        assert raw_item.area == 84.5

    def test_parse_item_raw_data_completeness(self, collector):
        """Test that raw_data contains all expected fields."""
        item_data = {
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

        raw_item = collector._parse_item(item_data)

        assert "prjNo" in raw_item.raw_data
        assert "prjNm" in raw_item.raw_data
        assert "supplyPrice" in raw_item.raw_data
        assert "competitionRate" in raw_item.raw_data


class TestSubscriptionCollectorDateParsing:
    """Tests for date parsing in SubscriptionCollector."""

    @pytest.fixture
    def collector(self):
        """Create SubscriptionCollector instance."""
        source = {"api_key": "test_key"}
        return SubscriptionCollector("subscription_test", source)

    def test_parse_date_with_hyphen_format(self, collector):
        """Test parsing date with hyphen format."""
        date_str = "2024-03-01"
        result = collector._parse_date(date_str)

        assert result.year == 2024
        assert result.month == 3
        assert result.day == 1

    def test_parse_date_with_slash_format(self, collector):
        """Test parsing date with slash format."""
        date_str = "2024/03/01"
        result = collector._parse_date(date_str)

        assert result.year == 2024
        assert result.month == 3
        assert result.day == 1

    def test_parse_date_with_compact_format(self, collector):
        """Test parsing date with compact format."""
        date_str = "20240301"
        result = collector._parse_date(date_str)

        assert result.year == 2024
        assert result.month == 3
        assert result.day == 1

    def test_parse_date_with_empty_string(self, collector):
        """Test parsing empty date string returns current time."""
        result = collector._parse_date("")

        assert isinstance(result, datetime)
        assert result.tzinfo == UTC

    def test_parse_date_with_invalid_format(self, collector):
        """Test parsing invalid date format returns current time."""
        result = collector._parse_date("invalid_date")

        assert isinstance(result, datetime)
        assert result.tzinfo == UTC


class TestSubscriptionCollectorAreaParsing:
    """Tests for area parsing in SubscriptionCollector."""

    @pytest.fixture
    def collector(self):
        """Create SubscriptionCollector instance."""
        source = {"api_key": "test_key"}
        return SubscriptionCollector("subscription_test", source)

    def test_parse_area_with_square_meter_unit(self, collector):
        """Test parsing area with ㎡ unit."""
        result = collector._parse_area("84.5㎡")
        assert result == 84.5

    def test_parse_area_with_m2_unit(self, collector):
        """Test parsing area with m² unit."""
        result = collector._parse_area("84.5m²")
        assert result == 84.5

    def test_parse_area_without_unit(self, collector):
        """Test parsing area without unit."""
        result = collector._parse_area("84.5")
        assert result == 84.5

    def test_parse_area_with_empty_string(self, collector):
        """Test parsing empty area string."""
        result = collector._parse_area("")
        assert result is None

    def test_parse_area_with_invalid_format(self, collector):
        """Test parsing invalid area format."""
        result = collector._parse_area("invalid_area")
        assert result is None
