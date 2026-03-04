"""
Unit tests for RSS collector.
"""

from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

from collectors.rss_collector import RSSCollector
from collectors.base import CollectorError


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
        items = collector.collect()

        # Basic validation
        assert len(items) > 0
        assert all(item.url for item in items)
        assert all(item.title for item in items)
        assert all(item.published_at for item in items)
