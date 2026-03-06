"""
Collector registry for HomeRadar.

Maps source types to collector classes and provides factory methods.
"""

from typing import Any

from collectors.base import BaseCollector
from collectors.molit_collector import MOLITCollector
from collectors.onbid_collector import OnbidCollector
from collectors.rss_collector import RSSCollector
from collectors.subscription_collector import SubscriptionCollector


class CollectorRegistry:
    """
    Registry for mapping source types to collector classes.
    """

    _COLLECTORS = {
        "rss": RSSCollector,
        "api": MOLITCollector,
        "onbid": OnbidCollector,
        "subscription": SubscriptionCollector,
        # "html": HTMLCollector,  # To be implemented
    }

    @classmethod
    def create_collector(
        cls, source_id: str, source_config: dict[str, Any]
    ) -> BaseCollector:
        """
        Create a collector instance based on source configuration.

        Args:
            source_id: Unique identifier for the source
            source_config: Configuration dict from sources.yaml

        Returns:
            Collector instance

        Raises:
            ValueError: If source type is not supported
        """
        source_type = source_config.get("type", "").lower()

        if source_type not in cls._COLLECTORS:
            raise ValueError(
                f"Unsupported source type: {source_type}. "
                f"Supported types: {list(cls._COLLECTORS.keys())}"
            )

        collector_class = cls._COLLECTORS[source_type]
        return collector_class(source_id, source_config)

    @classmethod
    def register_collector(cls, source_type: str, collector_class: type) -> None:
        """
        Register a new collector type.

        Args:
            source_type: Type identifier (e.g., 'rss', 'api')
            collector_class: Collector class to register
        """
        cls._COLLECTORS[source_type] = collector_class
