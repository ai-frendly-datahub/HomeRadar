"""
Unit tests for MOLITCollector.
"""

import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from unittest.mock import Mock, patch

import pytest
import requests

from collectors.molit_collector import MOLITCollector


class TestMOLITCollectorInit:
    """Tests for MOLITCollector initialization."""

    def test_init_with_service_key(self):
        """Test initialization with valid service key."""
        source = {
            'service_key': 'test_key',
            'url': 'http://api.example.com',
        }

        collector = MOLITCollector('test_id', source)

        assert collector.source_id == 'test_id'
        assert collector.service_key == 'test_key'
        assert collector.api_url == 'http://api.example.com'

    def test_init_without_service_key_raises_error(self):
        """Test that initialization without service key raises ValueError."""
        source = {'url': 'http://api.example.com'}

        with pytest.raises(ValueError, match="requires 'service_key'"):
            MOLITCollector('test_id', source)

    def test_init_with_custom_num_of_rows(self):
        """Test initialization with custom num_of_rows."""
        source = {
            'service_key': 'test_key',
            'url': 'http://api.example.com',
            'num_of_rows': 500,
        }

        collector = MOLITCollector('test_id', source)

        assert collector.num_of_rows == 500

    def test_init_default_num_of_rows(self):
        """Test default num_of_rows is 1000."""
        source = {
            'service_key': 'test_key',
            'url': 'http://api.example.com',
        }

        collector = MOLITCollector('test_id', source)

        assert collector.num_of_rows == 1000


class TestMOLITCollectorCollect:
    """Tests for MOLITCollector.collect() method."""

    @pytest.fixture
    def collector(self):
        """Create MOLITCollector instance."""
        source = {
            'service_key': 'test_key',
            'url': 'http://api.example.com',
        }
        return MOLITCollector('molit_test', source)

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
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.content = sample_xml_response.encode('utf-8')
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            items = collector.collect('11680', '202411')

            assert len(items) == 2
            assert items[0].title.startswith('래미안')
            assert items[1].title.startswith('힐스테이트')

    def test_collect_with_api_parameters(self, collector):
        """Test that correct parameters are sent to API."""
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.content = b"""<?xml version="1.0" encoding="UTF-8"?>
<response><header><resultCode>00</resultCode></header><body></body></response>
"""
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            collector.collect('11110', '202401', page_no=2)

            # Check that requests.get was called with correct parameters
            call_args = mock_get.call_args
            assert call_args[0][0] == collector.api_url
            params = call_args[1]['params']
            assert params['LAWD_CD'] == '11110'
            assert params['DEAL_YMD'] == '202401'
            assert params['pageNo'] == 2
            assert params['serviceKey'] == 'test_key'

    def test_collect_handles_network_error(self, collector):
        """Test handling of network errors."""
        with patch('requests.get', side_effect=requests.exceptions.ConnectionError):
            with pytest.raises(RuntimeError, match="MOLIT API request failed"):
                collector.collect('11680', '202411')

    def test_collect_handles_invalid_xml(self, collector):
        """Test handling of invalid XML response."""
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.content = b"Invalid XML <>"
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            with pytest.raises(RuntimeError, match="Failed to parse"):
                collector.collect('11680', '202411')

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
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.content = error_xml.encode('utf-8')
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            with pytest.raises(RuntimeError, match="MOLIT API error: 03"):
                collector.collect('11680', '202411')

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
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.content = empty_xml.encode('utf-8')
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            items = collector.collect('11680', '202411')

            assert items == []


class TestMOLITCollectorParseItem:
    """Tests for MOLITCollector._parse_item() method."""

    @pytest.fixture
    def collector(self):
        """Create MOLITCollector instance."""
        source = {
            'service_key': 'test_key',
            'url': 'http://api.example.com',
        }
        return MOLITCollector('molit_test', source)

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
        raw_item = collector._parse_item(item_node, '11680', '202411')

        assert raw_item is not None
        assert raw_item.title.startswith('래미안')
        assert raw_item.price == 100000
        assert raw_item.area == 84.5
        assert raw_item.property_type == '아파트'
        assert raw_item.region == '역삼동'

    def test_parse_item_with_missing_essential_fields(self, collector):
        """Test that items missing essential fields return None."""
        xml_str = """
<item>
    <dealYear>2024</dealYear>
    <dealMonth>11</dealMonth>
</item>
"""
        item_node = ET.fromstring(xml_str)
        raw_item = collector._parse_item(item_node, '11680', '202411')

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
        raw_item = collector._parse_item(item_node, '11680', '202411')

        assert raw_item.url.startswith('molit://transaction/')
        assert '11680' in raw_item.url
        assert '202411' in raw_item.url

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
        raw_item = collector._parse_item(item_node, '11680', '202411')

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
            'service_key': 'test_key',
            'url': 'http://api.example.com',
        }
        return MOLITCollector('molit_test', source)

    def test_collect_multiple_months_calls_collect_for_each_month(self, collector):
        """Test that collect is called for each month in range."""
        with patch.object(collector, 'collect', return_value=[]) as mock_collect:
            collector.collect_multiple_months('11680', '202401', '202403', delay=0)

            # Should call for Jan, Feb, Mar (3 times)
            assert mock_collect.call_count == 3
            mock_collect.assert_any_call('11680', '202401')
            mock_collect.assert_any_call('11680', '202402')
            mock_collect.assert_any_call('11680', '202403')

    def test_collect_multiple_months_handles_year_boundary(self, collector):
        """Test collecting across year boundary."""
        with patch.object(collector, 'collect', return_value=[]) as mock_collect:
            collector.collect_multiple_months('11680', '202312', '202402', delay=0)

            # Should call for Dec 2023, Jan 2024, Feb 2024
            assert mock_collect.call_count == 3
            mock_collect.assert_any_call('11680', '202312')
            mock_collect.assert_any_call('11680', '202401')
            mock_collect.assert_any_call('11680', '202402')

    def test_collect_multiple_months_continues_on_error(self, collector):
        """Test that collection continues even if one month fails."""
        def side_effect(lawd_cd, deal_ymd):
            if deal_ymd == '202402':
                raise RuntimeError("API error")
            return []

        with patch.object(collector, 'collect', side_effect=side_effect) as mock_collect:
            items = collector.collect_multiple_months('11680', '202401', '202403', delay=0)

            # Should still call for all 3 months
            assert mock_collect.call_count == 3
            assert items == []  # Empty because all returned empty lists
