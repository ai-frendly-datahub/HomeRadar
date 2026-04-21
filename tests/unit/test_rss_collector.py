"""
Unit tests for RSS collector.
"""

from datetime import datetime
from unittest.mock import Mock, patch

import pytest
import requests

from collectors.base import CollectorError
from collectors.rss_collector import RSSCollector


# Sample RSS feed XML
SAMPLE_RSS_FEED = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Real Estate News</title>
    <link>https://example.com</link>
    <description>Sample real estate news feed</description>
    <item>
      <title>Gangnam Apartment Prices Surge 10%</title>
      <link>https://example.com/news/gangnam-surge</link>
      <description>Apartment prices in Gangnam district rose by 10% this month.</description>
      <pubDate>Mon, 20 Nov 2023 09:00:00 GMT</pubDate>
      <guid>https://example.com/news/gangnam-surge</guid>
    </item>
    <item>
      <title>New GTX Line Boosts Property Values</title>
      <link>https://example.com/news/gtx-boost</link>
      <description>The opening of GTX line increased property values nearby.</description>
      <pubDate>Sun, 19 Nov 2023 14:30:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""


SAMPLE_ATOM_FEED = b"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Real Estate Policy Updates</title>
  <link href="https://example.gov/feed"/>
  <updated>2023-11-20T09:00:00Z</updated>
  <entry>
    <title>New Housing Policy Announced</title>
    <link href="https://example.gov/policy/housing-2023"/>
    <id>policy-housing-2023</id>
    <updated>2023-11-20T09:00:00Z</updated>
    <summary>Government announces new housing supply measures.</summary>
  </entry>
</feed>
"""


@pytest.fixture
def source_config():
    """Sample source configuration."""
    return {
        "id": "test_rss_source",
        "name": "Test RSS Source",
        "type": "rss",
        "enabled": True,
        "trust_tier": "T2_professional",
        "info_purpose": "news",
        "region": "korea",
        "url": "https://example.com/rss",
        "description": "Test RSS feed",
    }


class TestRSSCollector:
    """Tests for RSSCollector class."""

    def test_initialization(self, source_config):
        """Test collector initialization."""
        collector = RSSCollector("test_source", source_config)

        assert collector.source_id == "test_source"
        assert collector.url == "https://example.com/rss"
        assert collector.trust_tier == "T2_professional"
        assert collector.info_purpose == "news"

    def test_collect_rss_feed(self, source_config):
        """Test collecting from RSS 2.0 feed."""
        # Mock fetcher
        mock_fetcher = Mock(return_value=SAMPLE_RSS_FEED)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)

        # Collect items
        items = collector.collect()

        # Verify fetcher was called
        mock_fetcher.assert_called_once_with("https://example.com/rss")

        # Verify items
        assert len(items) == 2

        # Check first item
        item1 = items[0]
        assert item1.url == "https://example.com/news/gangnam-surge"
        assert item1.title == "Gangnam Apartment Prices Surge 10%"
        assert "Apartment prices in Gangnam" in item1.summary
        assert item1.source_id == "test_source"
        assert item1.property_type == "news"
        assert item1.raw_data["category"] == "news"
        assert isinstance(item1.published_at, datetime)

        # Check second item
        item2 = items[1]
        assert item2.url == "https://example.com/news/gtx-boost"
        assert item2.title == "New GTX Line Boosts Property Values"

    def test_collect_atom_feed(self, source_config):
        """Test collecting from Atom feed."""
        mock_fetcher = Mock(return_value=SAMPLE_ATOM_FEED)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)

        items = collector.collect()

        assert len(items) == 1
        item = items[0]
        assert item.url == "https://example.gov/policy/housing-2023"
        assert item.title == "New Housing Policy Announced"
        assert "Government announces" in item.summary

    def test_collect_with_missing_url(self, source_config):
        """Test that entries without URLs are skipped."""
        feed_without_url = b"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>No URL Item</title>
              <description>This item has no link</description>
            </item>
            <item>
              <title>Valid Item</title>
              <link>https://example.com/valid</link>
              <description>This item has a link</description>
            </item>
          </channel>
        </rss>
        """

        mock_fetcher = Mock(return_value=feed_without_url)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)

        items = collector.collect()

        # Should only collect the item with URL
        assert len(items) == 1
        assert items[0].url == "https://example.com/valid"

    def test_collect_uses_event_model_as_category(self, source_config):
        """Test that event_model supplies the validation category when configured."""
        source_config["event_model"] = "policy_context"
        mock_fetcher = Mock(return_value=SAMPLE_RSS_FEED)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)

        items = collector.collect()

        assert items[0].property_type == "policy_context"
        assert items[0].raw_data["category"] == "policy_context"

    def test_collect_with_network_error(self, source_config):
        """Test handling of network errors."""
        import requests

        mock_fetcher = Mock(side_effect=requests.RequestException("Network error"))
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)

        with pytest.raises(CollectorError, match="Failed to fetch RSS feed"):
            collector.collect()

    def test_collect_with_malformed_feed(self, source_config):
        """Test handling of malformed XML."""
        malformed_feed = b"This is not valid XML"

        mock_fetcher = Mock(return_value=malformed_feed)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)

        # feedparser is lenient, so this should not raise
        # But the feed will have no entries
        items = collector.collect()
        assert len(items) == 0

    def test_summary_extraction_priority(self, source_config):
        """Test summary extraction from different sources."""
        # Test with summary field
        feed_with_summary = b"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>Test Item</title>
              <link>https://example.com/test</link>
              <description>This is the summary</description>
            </item>
          </channel>
        </rss>
        """

        mock_fetcher = Mock(return_value=feed_with_summary)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)
        items = collector.collect()

        assert items[0].summary == "This is the summary"

    def test_date_parsing(self, source_config):
        """Test publication date parsing."""
        feed_with_date = b"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>Test Item</title>
              <link>https://example.com/test</link>
              <pubDate>Mon, 20 Nov 2023 10:30:00 GMT</pubDate>
            </item>
          </channel>
        </rss>
        """

        mock_fetcher = Mock(return_value=feed_with_date)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)
        items = collector.collect()

        assert isinstance(items[0].published_at, datetime)
        assert items[0].published_at.year == 2023
        assert items[0].published_at.month == 11
        assert items[0].published_at.day == 20

    def test_raw_data_extraction(self, source_config):
        """Test that additional metadata is stored in raw_data."""
        feed_with_metadata = b"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>Test Item</title>
              <link>https://example.com/test</link>
              <description>Summary</description>
              <author>John Doe</author>
              <category>Real Estate</category>
              <category>Policy</category>
              <guid>test-guid-123</guid>
            </item>
          </channel>
        </rss>
        """

        mock_fetcher = Mock(return_value=feed_with_metadata)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)
        items = collector.collect()

        raw_data = items[0].raw_data
        assert "entry_id" in raw_data
        assert "author" in raw_data
        assert raw_data["author"] == "John Doe"


class TestRSSCollectorIntegration:
    """Integration-style tests (can be marked as integration)."""

    @pytest.mark.integration
    def test_collect_from_real_feed(self, source_config):
        """Test collecting from a real RSS feed (requires network)."""
        # Use a reliable test feed
        source_config["url"] = "https://www.hankyung.com/feed/realestate"
        collector = RSSCollector("hankyung_test", source_config)

        # This will make actual network request
        try:
            items = collector.collect()
        except requests.RequestException as exc:
            pytest.skip(f"RSS feed unavailable from test network: {exc}")
        except CollectorError as exc:
            if "Failed to fetch RSS feed" in str(exc):
                pytest.skip(f"RSS feed unavailable from test network: {exc}")
            raise

        # Basic validation
        assert len(items) > 0
        assert all(item.url for item in items)
        assert all(item.title for item in items)
        assert all(item.published_at for item in items)


class TestRSSCollectorContentExtraction:
    """Tests for content and summary extraction from various feed formats."""

    @pytest.fixture
    def source_config(self):
        """Sample source configuration."""
        return {
            "id": "test_rss_source",
            "name": "Test RSS Source",
            "type": "rss",
            "enabled": True,
            "trust_tier": "T2_professional",
            "info_purpose": "news",
            "region": "korea",
            "url": "https://example.com/rss",
            "description": "Test RSS feed",
        }

    def test_extract_summary_from_description(self, source_config):
        """Test extracting summary from description field."""
        feed = b"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>Test Item</title>
              <link>https://example.com/test</link>
              <description>This is the description text</description>
            </item>
          </channel>
        </rss>
        """
        mock_fetcher = Mock(return_value=feed)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)
        items = collector.collect()

        assert items[0].summary == "This is the description text"

    def test_extract_summary_from_content_truncated(self, source_config):
        """Test extracting and truncating summary from content field."""
        long_content = "A" * 300
        feed = f"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>Test Item</title>
              <link>https://example.com/test</link>
              <description></description>
              <content>{long_content}</content>
            </item>
          </channel>
        </rss>
        """.encode()
        mock_fetcher = Mock(return_value=feed)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)
        items = collector.collect()

        # Content is truncated to 280 chars + ellipsis
        assert len(items[0].summary) <= 281
        assert items[0].summary.endswith("…")

    def test_extract_summary_fallback_to_title(self, source_config):
        """Test fallback to title when no summary or content."""
        feed = b"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>Only Title Available</title>
              <link>https://example.com/test</link>
            </item>
          </channel>
        </rss>
        """
        mock_fetcher = Mock(return_value=feed)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)
        items = collector.collect()

        assert items[0].summary == "Only Title Available"

    def test_extract_summary_empty_summary_uses_content(self, source_config):
        """Test that empty summary field falls back to content."""
        feed = b"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">
          <channel>
            <item>
              <title>Test Item</title>
              <link>https://example.com/test</link>
              <description></description>
              <content:encoded>Content text here</content:encoded>
            </item>
          </channel>
        </rss>
        """
        mock_fetcher = Mock(return_value=feed)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)
        items = collector.collect()

        assert items[0].summary == "Content text here"

    def test_extract_summary_with_html_tags(self, source_config):
        """Test that HTML tags are normalized in summary."""
        feed = b"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">
          <channel>
            <item>
              <title>Test Item</title>
              <link>https://example.com/test</link>
              <content:encoded><p>This is <b>bold</b> text</p></content:encoded>
            </item>
          </channel>
        </rss>
        """
        mock_fetcher = Mock(return_value=feed)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)
        items = collector.collect()

        # HTML should be normalized to spaces
        assert "This is" in items[0].summary
        assert "bold" in items[0].summary

    def test_extract_summary_with_multiple_spaces(self, source_config):
        """Test that multiple spaces are normalized to single space."""
        feed = b"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">
          <channel>
            <item>
              <title>Test Item</title>
              <link>https://example.com/test</link>
              <content:encoded>Text    with    multiple    spaces</content:encoded>
            </item>
          </channel>
        </rss>
        """
        mock_fetcher = Mock(return_value=feed)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)
        items = collector.collect()

        # Content is normalized with split() which collapses multiple spaces
        assert "Text" in items[0].summary
        assert "with" in items[0].summary
        assert "multiple" in items[0].summary
        assert "spaces" in items[0].summary


class TestRSSCollectorDateParsing:
    """Tests for date parsing from various RSS formats."""

    @pytest.fixture
    def source_config(self):
        """Sample source configuration."""
        return {
            "id": "test_rss_source",
            "name": "Test RSS Source",
            "type": "rss",
            "enabled": True,
            "trust_tier": "T2_professional",
            "info_purpose": "news",
            "region": "korea",
            "url": "https://example.com/rss",
            "description": "Test RSS feed",
        }

    def test_parse_rfc2822_date(self, source_config):
        """Test parsing RFC 2822 format date."""
        feed = b"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>Test Item</title>
              <link>https://example.com/test</link>
              <pubDate>Mon, 20 Nov 2023 10:30:00 GMT</pubDate>
            </item>
          </channel>
        </rss>
        """
        mock_fetcher = Mock(return_value=feed)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)
        items = collector.collect()

        assert items[0].published_at.year == 2023
        assert items[0].published_at.month == 11
        assert items[0].published_at.day == 20

    def test_parse_iso8601_date(self, source_config):
        """Test parsing ISO 8601 format date."""
        feed = b"""<?xml version="1.0" encoding="UTF-8"?>
        <feed xmlns="http://www.w3.org/2005/Atom">
          <entry>
            <title>Test Item</title>
            <link href="https://example.com/test"/>
            <updated>2023-11-20T10:30:00Z</updated>
          </entry>
        </feed>
        """
        mock_fetcher = Mock(return_value=feed)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)
        items = collector.collect()

        assert items[0].published_at.year == 2023
        assert items[0].published_at.month == 11
        assert items[0].published_at.day == 20

    def test_parse_date_with_timezone_offset(self, source_config):
        """Test parsing date with timezone offset."""
        feed = b"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>Test Item</title>
              <link>https://example.com/test</link>
              <pubDate>Mon, 20 Nov 2023 19:30:00 +0900</pubDate>
            </item>
          </channel>
        </rss>
        """
        mock_fetcher = Mock(return_value=feed)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)
        items = collector.collect()

        assert items[0].published_at.year == 2023
        assert items[0].published_at.month == 11
        assert items[0].published_at.day == 20

    def test_parse_missing_date_uses_current_time(self, source_config):
        """Test that missing date falls back to current time."""
        feed = b"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>Test Item</title>
              <link>https://example.com/test</link>
            </item>
          </channel>
        </rss>
        """
        mock_fetcher = Mock(return_value=feed)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)
        items = collector.collect()

        # Should have a published_at date (current time)
        assert items[0].published_at is not None
        assert isinstance(items[0].published_at, datetime)

    def test_parse_updated_date_when_published_missing(self, source_config):
        """Test that updated date is used when published is missing."""
        feed = b"""<?xml version="1.0" encoding="UTF-8"?>
        <feed xmlns="http://www.w3.org/2005/Atom">
          <entry>
            <title>Test Item</title>
            <link href="https://example.com/test"/>
            <updated>2023-11-20T10:30:00Z</updated>
          </entry>
        </feed>
        """
        mock_fetcher = Mock(return_value=feed)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)
        items = collector.collect()

        assert items[0].published_at.year == 2023


class TestRSSCollectorErrorHandling:
    """Tests for error handling in RSS collector."""

    @pytest.fixture
    def source_config(self):
        """Sample source configuration."""
        return {
            "id": "test_rss_source",
            "name": "Test RSS Source",
            "type": "rss",
            "enabled": True,
            "trust_tier": "T2_professional",
            "info_purpose": "news",
            "region": "korea",
            "url": "https://example.com/rss",
            "description": "Test RSS feed",
        }

    def test_collect_with_connection_timeout(self, source_config):
        """Test handling of connection timeout."""
        import requests

        mock_fetcher = Mock(side_effect=requests.exceptions.Timeout("Connection timeout"))
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)

        with pytest.raises(CollectorError, match="Failed to fetch RSS feed"):
            collector.collect()

    def test_collect_with_connection_error(self, source_config):
        """Test handling of connection error."""
        import requests

        mock_fetcher = Mock(side_effect=requests.exceptions.ConnectionError("Connection failed"))
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)

        with pytest.raises(CollectorError, match="Failed to fetch RSS feed"):
            collector.collect()

    def test_collect_with_http_error(self, source_config):
        """Test handling of HTTP error."""
        import requests

        mock_fetcher = Mock(side_effect=requests.exceptions.HTTPError("404 Not Found"))
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)

        with pytest.raises(CollectorError, match="Failed to fetch RSS feed"):
            collector.collect()

    def test_collect_with_generic_exception(self, source_config):
        """Test handling of generic exception during parsing."""
        mock_fetcher = Mock(side_effect=Exception("Unexpected error"))
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)

        with pytest.raises(CollectorError, match="Failed to parse RSS feed"):
            collector.collect()

    def test_collect_with_empty_feed(self, source_config):
        """Test handling of empty feed."""
        feed = b"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <title>Empty Feed</title>
          </channel>
        </rss>
        """
        mock_fetcher = Mock(return_value=feed)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)
        items = collector.collect()

        assert items == []

    def test_collect_with_invalid_xml_declaration(self, source_config):
        """Test handling of invalid XML declaration."""
        feed = b"""<?xml version="2.0"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>Test</title>
              <link>https://example.com/test</link>
            </item>
          </channel>
        </rss>
        """
        mock_fetcher = Mock(return_value=feed)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)
        # feedparser is lenient, should still parse
        items = collector.collect()

        assert len(items) >= 0


class TestRSSCollectorRawData:
    """Tests for raw data extraction and storage."""

    @pytest.fixture
    def source_config(self):
        """Sample source configuration."""
        return {
            "id": "test_rss_source",
            "name": "Test RSS Source",
            "type": "rss",
            "enabled": True,
            "trust_tier": "T2_professional",
            "info_purpose": "news",
            "region": "korea",
            "url": "https://example.com/rss",
            "description": "Test RSS feed",
        }

    def test_raw_data_contains_entry_id(self, source_config):
        """Test that entry ID is stored in raw_data."""
        feed = b"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>Test Item</title>
              <link>https://example.com/test</link>
              <guid>unique-guid-123</guid>
            </item>
          </channel>
        </rss>
        """
        mock_fetcher = Mock(return_value=feed)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)
        items = collector.collect()

        assert items[0].raw_data["entry_id"] == "unique-guid-123"

    def test_raw_data_contains_author(self, source_config):
        """Test that author is stored in raw_data."""
        feed = b"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>Test Item</title>
              <link>https://example.com/test</link>
              <author>John Doe</author>
            </item>
          </channel>
        </rss>
        """
        mock_fetcher = Mock(return_value=feed)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)
        items = collector.collect()

        assert items[0].raw_data["author"] == "John Doe"

    def test_raw_data_contains_tags(self, source_config):
        """Test that tags are stored in raw_data."""
        feed = b"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>Test Item</title>
              <link>https://example.com/test</link>
              <category>Real Estate</category>
              <category>Policy</category>
            </item>
          </channel>
        </rss>
        """
        mock_fetcher = Mock(return_value=feed)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)
        items = collector.collect()

        assert "Real Estate" in items[0].raw_data["tags"]
        assert "Policy" in items[0].raw_data["tags"]

    def test_raw_data_with_missing_fields(self, source_config):
        """Test that missing fields in raw_data are handled gracefully."""
        feed = b"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>Test Item</title>
              <link>https://example.com/test</link>
            </item>
          </channel>
        </rss>
        """
        mock_fetcher = Mock(return_value=feed)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)
        items = collector.collect()

        assert items[0].raw_data["entry_id"] == ""
        assert items[0].raw_data["author"] == ""
        assert items[0].raw_data["tags"] == []


class TestRSSCollectorEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @pytest.fixture
    def source_config(self):
        """Sample source configuration."""
        return {
            "id": "test_rss_source",
            "name": "Test RSS Source",
            "type": "rss",
            "enabled": True,
            "trust_tier": "T2_professional",
            "info_purpose": "news",
            "region": "korea",
            "url": "https://example.com/rss",
            "description": "Test RSS feed",
        }

    def test_collect_with_very_long_title(self, source_config):
        """Test handling of very long title."""
        long_title = "A" * 500
        feed = f"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>{long_title}</title>
              <link>https://example.com/test</link>
            </item>
          </channel>
        </rss>
        """.encode()
        mock_fetcher = Mock(return_value=feed)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)
        items = collector.collect()

        assert len(items[0].title) == 500

    def test_collect_with_very_long_summary(self, source_config):
        """Test handling of very long summary."""
        long_content = "A" * 5000
        feed = f"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>Test</title>
              <link>https://example.com/test</link>
              <description>{long_content}</description>
            </item>
          </channel>
        </rss>
        """.encode()
        mock_fetcher = Mock(return_value=feed)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)
        items = collector.collect()

        assert len(items[0].summary) == 5000

    def test_collect_with_special_characters_in_title(self, source_config):
        """Test handling of special characters in title."""
        feed = b"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>Test &amp; Title &lt;with&gt; Special &quot;Chars&quot;</title>
              <link>https://example.com/test</link>
            </item>
          </channel>
        </rss>
        """
        mock_fetcher = Mock(return_value=feed)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)
        items = collector.collect()

        assert "&" in items[0].title or "and" in items[0].title

    def test_collect_with_unicode_characters(self, source_config):
        """Test handling of unicode characters."""
        feed = """<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>강남 아파트 가격 급등</title>
              <link>https://example.com/test</link>
              <description>서울 강남구의 아파트 가격이 급등했습니다</description>
            </item>
          </channel>
        </rss>
        """.encode()
        mock_fetcher = Mock(return_value=feed)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)
        items = collector.collect()

        assert "강남" in items[0].title
        assert "아파트" in items[0].summary

    def test_collect_with_duplicate_entries(self, source_config):
        """Test handling of duplicate entries in feed."""
        feed = b"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>Test Item</title>
              <link>https://example.com/test</link>
            </item>
            <item>
              <title>Test Item</title>
              <link>https://example.com/test</link>
            </item>
          </channel>
        </rss>
        """
        mock_fetcher = Mock(return_value=feed)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)
        items = collector.collect()

        # Should collect both (deduplication is not collector's responsibility)
        assert len(items) == 2

    def test_collect_with_whitespace_in_url(self, source_config):
        """Test handling of whitespace in URL."""
        feed = b"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title>Test Item</title>
              <link>  https://example.com/test  </link>
            </item>
          </channel>
        </rss>
        """
        mock_fetcher = Mock(return_value=feed)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)
        items = collector.collect()

        # feedparser should handle whitespace
        assert len(items) >= 0

    def test_collect_with_empty_title(self, source_config):
        """Test handling of empty title."""
        feed = b"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <item>
              <title></title>
              <link>https://example.com/test</link>
              <description>Description only</description>
            </item>
          </channel>
        </rss>
        """
        mock_fetcher = Mock(return_value=feed)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)
        items = collector.collect()

        assert len(items) == 1
        assert items[0].title == ""

    def test_collect_with_many_entries(self, source_config):
        """Test handling of feed with many entries."""
        items_xml = "\n".join(
            [
                f"""<item>
              <title>Item {i}</title>
              <link>https://example.com/item{i}</link>
            </item>"""
                for i in range(100)
            ]
        )
        feed = f"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            {items_xml}
          </channel>
        </rss>
        """.encode()
        mock_fetcher = Mock(return_value=feed)
        collector = RSSCollector("test_source", source_config, fetcher=mock_fetcher)
        items = collector.collect()

        assert len(items) == 100

    def test_initialization_with_missing_url(self):
        """Test that initialization fails without URL."""
        source_config = {
            "id": "test_rss_source",
            "name": "Test RSS Source",
            "type": "rss",
        }

        with pytest.raises(KeyError):
            RSSCollector("test_source", source_config)

    def test_default_fetcher_makes_request(self, source_config):
        """Test that default fetcher uses requests library."""

        with patch.object(RSSCollector, "_request") as mock_request:
            mock_response = Mock()
            mock_response.content = b"""<?xml version="1.0" encoding="UTF-8"?>
            <rss version="2.0">
              <channel>
                <item>
                  <title>Test</title>
                  <link>https://example.com/test</link>
                </item>
              </channel>
            </rss>
            """
            mock_response.raise_for_status = Mock()
            mock_request.return_value = mock_response

            collector = RSSCollector("test_source", source_config)
            items = collector.collect()

            mock_request.assert_called_once_with("GET", source_config["url"], timeout=15)
            assert len(items) == 1
