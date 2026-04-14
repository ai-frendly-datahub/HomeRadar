"""
Unit tests for MOLITCollector.
"""

import xml.etree.ElementTree as ET
from unittest.mock import Mock, patch

import pytest
import requests

from collectors.molit_collector import MOLITCollector


class TestMOLITCollectorInit:
    """Tests for MOLITCollector initialization."""

    def test_init_with_service_key(self):
        """Test initialization with valid service key."""
        source = {
            "service_key": "test_key",
            "url": "http://api.example.com",
        }

        collector = MOLITCollector("test_id", source)

        assert collector.source_id == "test_id"
        assert collector.service_key == "test_key"
        assert collector.api_url == "http://api.example.com"

    def test_init_without_service_key_raises_error(self):
        """Test that initialization without service key raises ValueError."""
        source = {"url": "http://api.example.com"}

        with pytest.raises(ValueError, match="requires 'service_key'"):
            MOLITCollector("test_id", source)

    def test_init_with_custom_num_of_rows(self):
        """Test initialization with custom num_of_rows."""
        source = {
            "service_key": "test_key",
            "url": "http://api.example.com",
            "num_of_rows": 500,
        }

        collector = MOLITCollector("test_id", source)

        assert collector.num_of_rows == 500

    def test_init_default_num_of_rows(self):
        """Test default num_of_rows is 1000."""
        source = {
            "service_key": "test_key",
            "url": "http://api.example.com",
        }

        collector = MOLITCollector("test_id", source)

        assert collector.num_of_rows == 1000


class TestMOLITCollectorCollect:
    """Tests for MOLITCollector.collect() method."""

    @pytest.fixture
    def collector(self):
        """Create MOLITCollector instance."""
        source = {
            "service_key": "test_key",
            "url": "http://api.example.com",
        }
        return MOLITCollector("molit_test", source)

    @pytest.fixture
    def sample_xml_response(self):
        """Sample XML response from MOLIT API."""
        return """<?xml version="1.0" encoding="UTF-8"?>
<response>
    <header>
        <resultCode>00</resultCode>
        <resultMsg>NORMAL SERVICE.</resultMsg>
    </header>
    <body>
        <items>
            <item>
                <aptNm>래미안</aptNm>
                <dealAmount>    100,000</dealAmount>
                <dealYear>2024</dealYear>
                <dealMonth>11</dealMonth>
                <dealDay>15</dealDay>
                <excluUseAr>84.5</excluUseAr>
                <floor>10</floor>
                <buildYear>2010</buildYear>
                <umdNm>역삼동</umdNm>
                <jibun>123-45</jibun>
                <sggCd>11680</sggCd>
            </item>
            <item>
                <aptNm>힐스테이트</aptNm>
                <dealAmount>    80,000</dealAmount>
                <dealYear>2024</dealYear>
                <dealMonth>11</dealMonth>
                <dealDay>16</dealDay>
                <excluUseAr>59.5</excluUseAr>
                <floor>5</floor>
                <buildYear>2015</buildYear>
                <umdNm>삼성동</umdNm>
                <jibun>456-78</jibun>
                <sggCd>11680</sggCd>
            </item>
        </items>
    </body>
</response>
"""

    def test_collect_successful_request(self, collector, sample_xml_response):
        """Test successful API request and parsing."""
        with patch("requests.get") as mock_get:
            mock_response = Mock()
            mock_response.content = sample_xml_response.encode("utf-8")
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            items = collector.collect("11680", "202411")

            assert len(items) == 2
            assert items[0].title.startswith("래미안")
            assert items[1].title.startswith("힐스테이트")

    def test_collect_with_api_parameters(self, collector):
        """Test that correct parameters are sent to API."""
        with patch("requests.get") as mock_get:
            mock_response = Mock()
            mock_response.content = b"""<?xml version="1.0" encoding="UTF-8"?>
<response><header><resultCode>00</resultCode></header><body></body></response>
"""
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            collector.collect("11110", "202401", page_no=2)

            # Check that requests.get was called with correct parameters
            call_args = mock_get.call_args
            assert call_args[0][0] == collector.api_url
            params = call_args[1]["params"]
            assert params["LAWD_CD"] == "11110"
            assert params["DEAL_YMD"] == "202401"
            assert params["pageNo"] == 2
            assert params["serviceKey"] == "test_key"

    def test_collect_handles_network_error(self, collector):
        """Test handling of network errors."""
        with patch("requests.get", side_effect=requests.exceptions.ConnectionError):
            with pytest.raises(RuntimeError, match="MOLIT API request failed"):
                collector.collect("11680", "202411")

    def test_collect_handles_invalid_xml(self, collector):
        """Test handling of invalid XML response."""
        with patch("requests.get") as mock_get:
            mock_response = Mock()
            mock_response.content = b"Invalid XML <>"
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            with pytest.raises(RuntimeError, match="Failed to parse"):
                collector.collect("11680", "202411")

    def test_collect_handles_api_error_code(self, collector):
        """Test handling of API error responses."""
        error_xml = """<?xml version="1.0" encoding="UTF-8"?>
<response>
    <header>
        <resultCode>03</resultCode>
        <resultMsg>INVALID SERVICE KEY</resultMsg>
    </header>
</response>
"""
        with patch("requests.get") as mock_get:
            mock_response = Mock()
            mock_response.content = error_xml.encode("utf-8")
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            with pytest.raises(RuntimeError, match="MOLIT API error: 03"):
                collector.collect("11680", "202411")

    def test_collect_returns_empty_list_for_no_items(self, collector):
        """Test that empty response returns empty list."""
        empty_xml = """<?xml version="1.0" encoding="UTF-8"?>
<response>
    <header>
        <resultCode>00</resultCode>
        <resultMsg>NORMAL SERVICE.</resultMsg>
    </header>
    <body>
        <items></items>
    </body>
</response>
"""
        with patch("requests.get") as mock_get:
            mock_response = Mock()
            mock_response.content = empty_xml.encode("utf-8")
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            items = collector.collect("11680", "202411")

            assert items == []


class TestMOLITCollectorParseItem:
    """Tests for MOLITCollector._parse_item() method."""

    @pytest.fixture
    def collector(self):
        """Create MOLITCollector instance."""
        source = {
            "service_key": "test_key",
            "url": "http://api.example.com",
        }
        return MOLITCollector("molit_test", source)

    def test_parse_item_with_complete_data(self, collector):
        """Test parsing item with all fields present."""
        xml_str = """
<item>
    <aptNm>래미안</aptNm>
    <dealAmount>100,000</dealAmount>
    <dealYear>2024</dealYear>
    <dealMonth>11</dealMonth>
    <dealDay>15</dealDay>
    <excluUseAr>84.5</excluUseAr>
    <floor>10</floor>
    <buildYear>2010</buildYear>
    <umdNm>역삼동</umdNm>
    <jibun>123-45</jibun>
    <sggCd>11680</sggCd>
</item>
"""
        item_node = ET.fromstring(xml_str)
        raw_item = collector._parse_item(item_node, "11680", "202411")

        assert raw_item is not None
        assert raw_item.title.startswith("래미안")
        assert raw_item.price == 100000
        assert raw_item.area == 84.5
        assert raw_item.property_type == "아파트"
        assert raw_item.region == "역삼동"

    def test_parse_item_with_missing_essential_fields(self, collector):
        """Test that items missing essential fields return None."""
        xml_str = """
<item>
    <dealYear>2024</dealYear>
    <dealMonth>11</dealMonth>
</item>
"""
        item_node = ET.fromstring(xml_str)
        raw_item = collector._parse_item(item_node, "11680", "202411")

        assert raw_item is None

    def test_parse_item_builds_correct_url(self, collector):
        """Test that pseudo-URL is built correctly."""
        xml_str = """
<item>
    <aptNm>래미안</aptNm>
    <dealAmount>100,000</dealAmount>
    <dealYear>2024</dealYear>
    <dealMonth>11</dealMonth>
    <dealDay>15</dealDay>
    <excluUseAr>84.5</excluUseAr>
    <floor>10</floor>
</item>
"""
        item_node = ET.fromstring(xml_str)
        raw_item = collector._parse_item(item_node, "11680", "202411")

        assert raw_item.url.startswith("molit://transaction/")
        assert "11680" in raw_item.url
        assert "202411" in raw_item.url

    def test_parse_item_handles_invalid_date(self, collector):
        """Test handling of invalid date values."""
        xml_str = """
<item>
    <aptNm>래미안</aptNm>
    <dealAmount>100,000</dealAmount>
    <dealYear>invalid</dealYear>
    <dealMonth>11</dealMonth>
    <dealDay>15</dealDay>
</item>
"""
        item_node = ET.fromstring(xml_str)
        raw_item = collector._parse_item(item_node, "11680", "202411")

        # Should use fallback date from deal_ymd
        assert raw_item is not None
        assert raw_item.published_at.year == 2024
        assert raw_item.published_at.month == 11


class TestMOLITCollectorMultipleMonths:
    """Tests for MOLITCollector.collect_multiple_months() method."""

    @pytest.fixture
    def collector(self):
        """Create MOLITCollector instance."""
        source = {
            "service_key": "test_key",
            "url": "http://api.example.com",
        }
        return MOLITCollector("molit_test", source)

    def test_collect_multiple_months_calls_collect_for_each_month(self, collector):
        """Test that collect is called for each month in range."""
        with patch.object(collector, "collect", return_value=[]) as mock_collect:
            collector.collect_multiple_months("11680", "202401", "202403", delay=0)

            # Should call for Jan, Feb, Mar (3 times)
            assert mock_collect.call_count == 3
            mock_collect.assert_any_call("11680", "202401")
            mock_collect.assert_any_call("11680", "202402")
            mock_collect.assert_any_call("11680", "202403")

    def test_collect_multiple_months_handles_year_boundary(self, collector):
        """Test collecting across year boundary."""
        with patch.object(collector, "collect", return_value=[]) as mock_collect:
            collector.collect_multiple_months("11680", "202312", "202402", delay=0)

            # Should call for Dec 2023, Jan 2024, Feb 2024
            assert mock_collect.call_count == 3
            mock_collect.assert_any_call("11680", "202312")
            mock_collect.assert_any_call("11680", "202401")
            mock_collect.assert_any_call("11680", "202402")

    def test_collect_multiple_months_continues_on_error(self, collector):
        """Test that collection continues even if one month fails."""

        def side_effect(lawd_cd, deal_ymd):
            if deal_ymd == "202402":
                raise RuntimeError("API error")
            return []

        with patch.object(collector, "collect", side_effect=side_effect) as mock_collect:
            items = collector.collect_multiple_months("11680", "202401", "202403", delay=0)

            # Should still call for all 3 months
            assert mock_collect.call_count == 3
            assert items == []  # Empty because all returned empty lists


class TestMOLITCollectorEdgeCases:
    """Tests for edge cases and error conditions."""

    @pytest.fixture
    def collector(self):
        """Create MOLITCollector instance."""
        source = {
            "service_key": "test_key",
            "url": "http://api.example.com",
        }
        return MOLITCollector("molit_test", source)

    def test_collect_with_timeout(self, collector):
        """Test handling of request timeout."""
        with patch("requests.get", side_effect=requests.exceptions.Timeout):
            with pytest.raises(RuntimeError, match="MOLIT API request failed"):
                collector.collect("11680", "202411")

    def test_collect_with_http_error(self, collector):
        """Test handling of HTTP errors."""
        with patch("requests.get") as mock_get:
            mock_response = Mock()
            mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
                "404 Not Found"
            )
            mock_get.return_value = mock_response

            with pytest.raises(RuntimeError, match="MOLIT API request failed"):
                collector.collect("11680", "202411")

    def test_parse_item_with_whitespace_in_amount(self, collector):
        """Test parsing amount with extra whitespace."""
        xml_str = """
<item>
    <aptNm>래미안</aptNm>
    <dealAmount>    100,000    </dealAmount>
    <dealYear>2024</dealYear>
    <dealMonth>11</dealMonth>
    <dealDay>15</dealDay>
</item>
"""
        item_node = ET.fromstring(xml_str)
        raw_item = collector._parse_item(item_node, "11680", "202411")

        assert raw_item is not None
        assert raw_item.price == 100000

    def test_parse_item_with_invalid_amount(self, collector):
        """Test parsing invalid amount format."""
        xml_str = """
<item>
    <aptNm>래미안</aptNm>
    <dealAmount>invalid_amount</dealAmount>
    <dealYear>2024</dealYear>
    <dealMonth>11</dealMonth>
    <dealDay>15</dealDay>
</item>
"""
        item_node = ET.fromstring(xml_str)
        raw_item = collector._parse_item(item_node, "11680", "202411")

        assert raw_item is not None
        assert raw_item.price is None

    def test_parse_item_with_invalid_area(self, collector):
        """Test parsing invalid area format."""
        xml_str = """
<item>
    <aptNm>래미안</aptNm>
    <dealAmount>100,000</dealAmount>
    <dealYear>2024</dealYear>
    <dealMonth>11</dealMonth>
    <dealDay>15</dealDay>
    <excluUseAr>invalid_area</excluUseAr>
</item>
"""
        item_node = ET.fromstring(xml_str)
        raw_item = collector._parse_item(item_node, "11680", "202411")

        assert raw_item is not None
        assert raw_item.area is None

    def test_parse_item_with_zero_floor(self, collector):
        """Test parsing item with floor 0 (ground floor)."""
        xml_str = """
<item>
    <aptNm>래미안</aptNm>
    <dealAmount>100,000</dealAmount>
    <dealYear>2024</dealYear>
    <dealMonth>11</dealMonth>
    <dealDay>15</dealDay>
    <floor>0</floor>
</item>
"""
        item_node = ET.fromstring(xml_str)
        raw_item = collector._parse_item(item_node, "11680", "202411")

        assert raw_item is not None
        assert "0층" in raw_item.title

    def test_parse_item_with_missing_optional_fields(self, collector):
        """Test parsing item with only required fields."""
        xml_str = """
<item>
    <aptNm>래미안</aptNm>
    <dealAmount>100,000</dealAmount>
    <dealYear>2024</dealYear>
    <dealMonth>11</dealMonth>
    <dealDay>15</dealDay>
</item>
"""
        item_node = ET.fromstring(xml_str)
        raw_item = collector._parse_item(item_node, "11680", "202411")

        assert raw_item is not None
        assert raw_item.title == "래미안"
        assert raw_item.area is None
        assert raw_item.region == ""

    def test_parse_item_with_empty_apt_name(self, collector):
        """Test that empty apartment name returns None."""
        xml_str = """
<item>
    <aptNm>   </aptNm>
    <dealAmount>100,000</dealAmount>
    <dealYear>2024</dealYear>
    <dealMonth>11</dealMonth>
    <dealDay>15</dealDay>
</item>
"""
        item_node = ET.fromstring(xml_str)
        raw_item = collector._parse_item(item_node, "11680", "202411")

        assert raw_item is None

    def test_parse_item_with_empty_deal_amount(self, collector):
        """Test that empty deal amount returns None."""
        xml_str = """
<item>
    <aptNm>래미안</aptNm>
    <dealAmount>   </dealAmount>
    <dealYear>2024</dealYear>
    <dealMonth>11</dealMonth>
    <dealDay>15</dealDay>
</item>
"""
        item_node = ET.fromstring(xml_str)
        raw_item = collector._parse_item(item_node, "11680", "202411")

        assert raw_item is None

    def test_parse_item_with_missing_day_pads_zero(self, collector):
        """Test that missing day is padded with zero."""
        xml_str = """
<item>
    <aptNm>래미안</aptNm>
    <dealAmount>100,000</dealAmount>
    <dealYear>2024</dealYear>
    <dealMonth>1</dealMonth>
    <dealDay>5</dealDay>
</item>
"""
        item_node = ET.fromstring(xml_str)
        raw_item = collector._parse_item(item_node, "11680", "202401")

        assert raw_item is not None
        assert raw_item.published_at.day == 5
        assert raw_item.published_at.month == 1

    def test_collect_with_response_no_body(self, collector):
        """Test handling of response with no body element."""
        xml_no_body = """<?xml version="1.0" encoding="UTF-8"?>
<response>
    <header>
        <resultCode>00</resultCode>
        <resultMsg>NORMAL SERVICE.</resultMsg>
    </header>
</response>
"""
        with patch("requests.get") as mock_get:
            mock_response = Mock()
            mock_response.content = xml_no_body.encode("utf-8")
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            items = collector.collect("11680", "202411")

            assert items == []

    def test_collect_with_response_no_header(self, collector):
        """Test handling of response with no header element."""
        xml_no_header = """<?xml version="1.0" encoding="UTF-8"?>
<response>
    <body>
        <items>
            <item>
                <aptNm>래미안</aptNm>
                <dealAmount>100,000</dealAmount>
                <dealYear>2024</dealYear>
                <dealMonth>11</dealMonth>
                <dealDay>15</dealDay>
            </item>
        </items>
    </body>
</response>
"""
        with patch("requests.get") as mock_get:
            mock_response = Mock()
            mock_response.content = xml_no_header.encode("utf-8")
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            items = collector.collect("11680", "202411")

            # Should still parse items even without header
            assert len(items) == 1

    def test_parse_item_with_large_amount(self, collector):
        """Test parsing very large transaction amounts."""
        xml_str = """
<item>
    <aptNm>럭셔리 펜트하우스</aptNm>
    <dealAmount>10,000,000</dealAmount>
    <dealYear>2024</dealYear>
    <dealMonth>11</dealMonth>
    <dealDay>15</dealDay>
</item>
"""
        item_node = ET.fromstring(xml_str)
        raw_item = collector._parse_item(item_node, "11680", "202411")

        assert raw_item is not None
        assert raw_item.price == 10000000

    def test_parse_item_with_decimal_area(self, collector):
        """Test parsing area with decimal places."""
        xml_str = """
<item>
    <aptNm>래미안</aptNm>
    <dealAmount>100,000</dealAmount>
    <dealYear>2024</dealYear>
    <dealMonth>11</dealMonth>
    <dealDay>15</dealDay>
    <excluUseAr>84.567</excluUseAr>
</item>
"""
        item_node = ET.fromstring(xml_str)
        raw_item = collector._parse_item(item_node, "11680", "202411")

        assert raw_item is not None
        assert raw_item.area == 84.567

    def test_parse_item_with_special_characters_in_name(self, collector):
        """Test parsing apartment names with special characters."""
        xml_str = """
<item>
    <aptNm>래미안 &amp; 힐스테이트</aptNm>
    <dealAmount>100,000</dealAmount>
    <dealYear>2024</dealYear>
    <dealMonth>11</dealMonth>
    <dealDay>15</dealDay>
</item>
"""
        item_node = ET.fromstring(xml_str)
        raw_item = collector._parse_item(item_node, "11680", "202411")

        assert raw_item is not None
        assert "래미안" in raw_item.title

    def test_collect_with_multiple_items_partial_failure(self, collector):
        """Test that collection continues even if some items fail to parse."""
        xml_mixed = """<?xml version="1.0" encoding="UTF-8"?>
<response>
    <header>
        <resultCode>00</resultCode>
        <resultMsg>NORMAL SERVICE.</resultMsg>
    </header>
    <body>
        <items>
            <item>
                <aptNm>래미안</aptNm>
                <dealAmount>100,000</dealAmount>
                <dealYear>2024</dealYear>
                <dealMonth>11</dealMonth>
                <dealDay>15</dealDay>
            </item>
            <item>
                <aptNm></aptNm>
                <dealAmount>80,000</dealAmount>
            </item>
            <item>
                <aptNm>힐스테이트</aptNm>
                <dealAmount>80,000</dealAmount>
                <dealYear>2024</dealYear>
                <dealMonth>11</dealMonth>
                <dealDay>16</dealDay>
            </item>
        </items>
    </body>
</response>
"""
        with patch("requests.get") as mock_get:
            mock_response = Mock()
            mock_response.content = xml_mixed.encode("utf-8")
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            items = collector.collect("11680", "202411")

            # Should have 2 valid items (skipping the one with empty name)
            assert len(items) == 2
            assert items[0].title.startswith("래미안")
            assert items[1].title.startswith("힐스테이트")

    def test_get_text_with_none_element(self, collector):
        """Test _get_text with non-existent element."""
        xml_str = "<item><aptNm>래미안</aptNm></item>"
        item_node = ET.fromstring(xml_str)

        result = collector._get_text(item_node, "nonexistent", "default_value")

        assert result == "default_value"

    def test_get_text_with_empty_element(self, collector):
        """Test _get_text with empty element."""
        xml_str = "<item><aptNm></aptNm></item>"
        item_node = ET.fromstring(xml_str)

        result = collector._get_text(item_node, "aptNm", "default_value")

        assert result == "default_value"

    def test_get_text_with_whitespace_only(self, collector):
        """Test _get_text with whitespace-only element."""
        xml_str = "<item><aptNm>   </aptNm></item>"
        item_node = ET.fromstring(xml_str)

        result = collector._get_text(item_node, "aptNm", "default_value")

        assert result == ""  # Returns empty string after strip

    def test_parse_item_raw_data_completeness(self, collector):
        """Test that raw_data contains all expected fields."""
        xml_str = """
<item>
    <aptNm>래미안</aptNm>
    <dealAmount>100,000</dealAmount>
    <dealYear>2024</dealYear>
    <dealMonth>11</dealMonth>
    <dealDay>15</dealDay>
    <excluUseAr>84.5</excluUseAr>
    <floor>10</floor>
    <buildYear>2010</buildYear>
    <umdNm>역삼동</umdNm>
    <jibun>123-45</jibun>
    <sggCd>11680</sggCd>
    <aptDong>101동</aptDong>
</item>
"""
        item_node = ET.fromstring(xml_str)
        raw_item = collector._parse_item(item_node, "11680", "202411")

        assert raw_item is not None
        assert "aptNm" in raw_item.raw_data
        assert "dealAmount" in raw_item.raw_data
        assert "excluUseAr" in raw_item.raw_data
        assert "floor" in raw_item.raw_data
        assert "buildYear" in raw_item.raw_data
        assert "lawd_cd" in raw_item.raw_data
        assert "deal_ymd" in raw_item.raw_data

    def test_collect_pagination_parameter(self, collector):
        """Test that pagination parameter is correctly passed."""
        with patch("requests.get") as mock_get:
            mock_response = Mock()
            mock_response.content = b"""<?xml version="1.0" encoding="UTF-8"?>
<response><header><resultCode>00</resultCode></header><body></body></response>
"""
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            collector.collect("11680", "202411", page_no=5)

            call_args = mock_get.call_args
            params = call_args[1]["params"]
            assert params["pageNo"] == 5

    def test_parse_item_with_february_29_leap_year(self, collector):
        """Test parsing date on leap year February 29."""
        xml_str = """
<item>
    <aptNm>래미안</aptNm>
    <dealAmount>100,000</dealAmount>
    <dealYear>2024</dealYear>
    <dealMonth>2</dealMonth>
    <dealDay>29</dealDay>
</item>
"""
        item_node = ET.fromstring(xml_str)
        raw_item = collector._parse_item(item_node, "11680", "202402")

        assert raw_item is not None
        assert raw_item.published_at.day == 29
        assert raw_item.published_at.month == 2

    def test_parse_item_with_invalid_february_29_non_leap_year(self, collector):
        """Test parsing invalid date (Feb 29 on non-leap year) falls back to deal_ymd."""
        xml_str = """
<item>
    <aptNm>래미안</aptNm>
    <dealAmount>100,000</dealAmount>
    <dealYear>2023</dealYear>
    <dealMonth>2</dealMonth>
    <dealDay>29</dealDay>
</item>
"""
        item_node = ET.fromstring(xml_str)
        raw_item = collector._parse_item(item_node, "11680", "202302")

        assert raw_item is not None
        # Should use fallback date from deal_ymd
        assert raw_item.published_at.year == 2023
        assert raw_item.published_at.month == 2

    def test_collect_multiple_months_single_month(self, collector):
        """Test collect_multiple_months with same start and end month."""
        with patch.object(collector, "collect", return_value=[]) as mock_collect:
            collector.collect_multiple_months("11680", "202411", "202411", delay=0)

            # Should call once for the same month
            assert mock_collect.call_count == 1
            mock_collect.assert_called_with("11680", "202411")

    def test_collect_multiple_months_aggregates_items(self, collector):
        """Test that collect_multiple_months aggregates items from all months."""

        def mock_collect_side_effect(lawd_cd, deal_ymd):
            if deal_ymd == "202401":
                return [Mock(title="Item1")]
            elif deal_ymd == "202402":
                return [Mock(title="Item2"), Mock(title="Item3")]
            return []

        with patch.object(collector, "collect", side_effect=mock_collect_side_effect):
            items = collector.collect_multiple_months("11680", "202401", "202402", delay=0)

            assert len(items) == 3

    def test_parse_item_with_amount_no_commas(self, collector):
        """Test parsing amount without comma separators."""
        xml_str = """
<item>
    <aptNm>래미안</aptNm>
    <dealAmount>100000</dealAmount>
    <dealYear>2024</dealYear>
    <dealMonth>11</dealMonth>
    <dealDay>15</dealDay>
</item>
"""
        item_node = ET.fromstring(xml_str)
        raw_item = collector._parse_item(item_node, "11680", "202411")

        assert raw_item is not None
        assert raw_item.price == 100000

    def test_parse_item_summary_format(self, collector):
        """Test that summary is formatted correctly."""
        xml_str = """
<item>
    <aptNm>래미안</aptNm>
    <dealAmount>100,000</dealAmount>
    <dealYear>2024</dealYear>
    <dealMonth>11</dealMonth>
    <dealDay>15</dealDay>
    <excluUseAr>84.5</excluUseAr>
    <umdNm>역삼동</umdNm>
    <jibun>123-45</jibun>
    <buildYear>2010</buildYear>
</item>
"""
        item_node = ET.fromstring(xml_str)
        raw_item = collector._parse_item(item_node, "11680", "202411")

        assert raw_item is not None
        assert "거래금액" in raw_item.summary
        assert "100,000" in raw_item.summary
        assert "위치" in raw_item.summary
        assert "역삼동" in raw_item.summary
        assert "건축년도" in raw_item.summary

    def test_parse_item_url_uniqueness(self, collector):
        """Test that generated URLs are unique for different items."""
        xml_str1 = """
<item>
    <aptNm>래미안</aptNm>
    <dealAmount>100,000</dealAmount>
    <dealYear>2024</dealYear>
    <dealMonth>11</dealMonth>
    <dealDay>15</dealDay>
    <excluUseAr>84.5</excluUseAr>
    <floor>10</floor>
</item>
"""
        xml_str2 = """
<item>
    <aptNm>래미안</aptNm>
    <dealAmount>100,000</dealAmount>
    <dealYear>2024</dealYear>
    <dealMonth>11</dealMonth>
    <dealDay>15</dealDay>
    <excluUseAr>84.5</excluUseAr>
    <floor>11</floor>
</item>
"""
        item_node1 = ET.fromstring(xml_str1)
        item_node2 = ET.fromstring(xml_str2)

        raw_item1 = collector._parse_item(item_node1, "11680", "202411")
        raw_item2 = collector._parse_item(item_node2, "11680", "202411")

        assert raw_item1.url != raw_item2.url
