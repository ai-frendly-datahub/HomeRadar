"""
HomeRadar main entry point.

This script orchestrates data collection, analysis, and reporting:
1. Collect from enabled sources (RSS news, MOLIT API, etc.)
2. Extract entities and store in graph database
3. Generate reports (optional)
4. Run as one-time job or scheduled daemon

Usage:
    python main.py --mode once                    # Run once and exit
    python main.py --mode scheduler --interval 24 # Run every 24 hours
    python main.py --sources molit,rss            # Run specific sources only
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from analyzers import EntityExtractor
from collectors import CollectorRegistry, RawItem
from graph import GraphStore
from graph.search_index import SearchIndex
from raw_logger import RawLogger
from reporters.html_reporter import HtmlReporter

# Logger (configured in setup_logging())
logger = logging.getLogger(__name__)


def setup_logging():
    """Configure logging with file and console handlers."""
    # Create logs directory
    Path("logs").mkdir(exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("logs/homeradar.log", encoding="utf-8"),
        ],
    )


def load_sources(config_path: str = "config/sources.yaml") -> dict[str, Any]:
    """
    Load source configuration from YAML file.

    Args:
        config_path: Path to sources.yaml

    Returns:
        Configuration dictionary
    """
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        logger.error(f"Failed to load sources config: {e}")
        raise


def collect_from_sources(
    sources: list[dict[str, Any]],
    enabled_only: bool = True,
    source_filter: list[str] | None = None,
) -> list[RawItem]:
    """
    Collect data from all configured sources.

    Args:
        sources: List of source configurations
        enabled_only: Only collect from enabled sources
        source_filter: Optional list of source IDs to collect from

    Returns:
        List of collected RawItem objects
    """
    all_items = []

    # Filter sources
    filtered_sources = sources
    if enabled_only:
        filtered_sources = [s for s in filtered_sources if s.get("enabled", False)]
    if source_filter:
        filtered_sources = [s for s in filtered_sources if s["id"] in source_filter]

    logger.info(f"Collecting from {len(filtered_sources)} sources...")
    raw_logger = RawLogger(Path("data") / "raw")

    for source in filtered_sources:
        source_id = source["id"]
        source_name = source.get("name", source_id)
        source_type = source["type"]

        logger.info(f"  [{source_type}] {source_name} ({source_id})")

        try:
            # Create collector
            collector = CollectorRegistry.create_collector(source_id, source)

            # Special handling for MOLIT API (requires parameters)
            if source_type == "api" and source_id.startswith("molit"):
                items = collect_molit(collector, source)
            else:
                # Regular collection (RSS, etc.)
                items = collector.collect()

            logger.info(f"    Collected {len(items)} items")
            raw_logger.log(items, source_name=source_id)
            all_items.extend(items)

        except Exception as e:
            logger.error(f"    Failed to collect from {source_id}: {e}")
            continue

    logger.info(f"Total items collected: {len(all_items)}")
    return all_items


def collect_molit(collector: Any, source: dict[str, Any]) -> list[RawItem]:
    """
    Collect from MOLIT API with proper parameters.

    Args:
        collector: MOLIT collector instance
        source: Source configuration

    Returns:
        List of collected items
    """
    # Get MOLIT-specific config
    lawd_cd = source.get("lawd_cd", "11680")  # Default: Gangnam-gu
    deal_ymd = source.get("deal_ymd", datetime.now().strftime("%Y%m"))

    # Check for service key
    if "service_key" not in source:
        service_key = os.environ.get("MOLIT_SERVICE_KEY")
        if not service_key:
            logger.warning(f"    Skipping MOLIT: MOLIT_SERVICE_KEY not set")
            return []
        source["service_key"] = service_key

    logger.info(f"    Region: {lawd_cd}, Period: {deal_ymd}")

    try:
        items = collector.collect(lawd_cd, deal_ymd)
        return items
    except Exception as e:
        logger.error(f"    MOLIT API error: {e}")
        return []


def store_and_extract(items: list[RawItem], store: GraphStore) -> dict[str, int]:
    """
    Store items in database and extract entities.

    Args:
        items: List of RawItem objects
        store: GraphStore instance

    Returns:
        Dictionary with statistics
    """
    if not items:
        logger.info("No items to store")
        return {"stored": 0, "entities": 0}

    logger.info(f"Storing {len(items)} items...")

    # Store items
    result = store.add_items(items)
    logger.info(f"  Inserted: {result['inserted']}, Updated: {result['updated']}")

    search_index_path = Path(os.getenv("HOMERADAR_SEARCH_DB_PATH", "data/search_index.db"))
    search_index = SearchIndex(search_index_path)
    for item in items:
        search_index.upsert(item.url, item.title, item.summary)

    # Extract and store entities
    logger.info("Extracting entities...")
    extractor = EntityExtractor()
    total_entities = 0

    for item in items:
        try:
            # Convert RawItem to dict
            item_dict = {
                "title": item.title,
                "summary": item.summary,
            }

            entities = extractor.extract_from_item(item_dict)

            if entities:
                count = store.add_entities(item.url, entities)
                total_entities += count

        except Exception as e:
            logger.error(f"  Failed to extract entities for {item.url}: {e}")
            continue

    logger.info(f"  Entities extracted: {total_entities}")

    return {
        "stored": result["inserted"] + result["updated"],
        "entities": total_entities,
    }


def run_collection_cycle(
    config: dict[str, Any],
    source_filter: list[str] | None = None,
    generate_report: bool = False,
) -> dict[str, Any]:
    """
    Run one complete collection cycle.

    Args:
        config: Configuration dictionary
        source_filter: Optional source filter
        generate_report: Whether to generate HTML report after collection

    Returns:
        Statistics dictionary
    """
    cycle_start = datetime.now()
    logger.info("=" * 80)
    logger.info(f"Starting collection cycle at {cycle_start}")
    logger.info("=" * 80)

    try:
        # Initialize storage
        store = GraphStore()

        # Collect from sources
        sources = config.get("sources", [])
        items = collect_from_sources(sources, enabled_only=True, source_filter=source_filter)

        # Store and extract
        stats = store_and_extract(items, store)

        # Get database stats
        db_stats = store.get_stats()

        # Generate report if requested
        report_path = None
        if generate_report:
            try:
                logger.info("Generating HTML report...")
                reporter = HtmlReporter()
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                report_file = Path("reports") / f"daily_{timestamp}.html"
                report_path = reporter.generate_report(store, report_file, stats=stats)
                logger.info(f"  Report saved to {report_path}")
            except Exception as e:
                logger.error(f"Failed to generate report: {e}")

        cycle_end = datetime.now()
        duration = (cycle_end - cycle_start).total_seconds()

        result = {
            "start_time": cycle_start.isoformat(),
            "end_time": cycle_end.isoformat(),
            "duration_seconds": duration,
            "items_collected": len(items),
            "items_stored": stats["stored"],
            "entities_extracted": stats["entities"],
            "total_urls": db_stats["total_urls"],
            "total_entities": db_stats["total_entities"],
            "success": True,
        }
        if report_path:
            result["report_path"] = str(report_path)

        logger.info("=" * 80)
        logger.info(f"Collection cycle completed in {duration:.1f}s")
        logger.info(f"  Items collected: {result['items_collected']}")
        logger.info(f"  Items stored: {result['items_stored']}")
        logger.info(f"  Entities extracted: {result['entities_extracted']}")
        logger.info(
            f"  Total in DB: {result['total_urls']} URLs, {result['total_entities']} entities"
        )
        if report_path:
            logger.info(f"  Report: {report_path}")
        logger.info("=" * 80)

        return result

    except Exception as e:
        logger.error(f"Collection cycle failed: {e}", exc_info=True)
        return {
            "start_time": cycle_start.isoformat(),
            "end_time": datetime.now().isoformat(),
            "success": False,
            "error": str(e),
        }

        logger.info("=" * 80)
        logger.info(f"Collection cycle completed in {duration:.1f}s")
        logger.info(f"  Items collected: {result['items_collected']}")
        logger.info(f"  Items stored: {result['items_stored']}")
        logger.info(f"  Entities extracted: {result['entities_extracted']}")
        logger.info(
            f"  Total in DB: {result['total_urls']} URLs, {result['total_entities']} entities"
        )
        logger.info("=" * 80)

        return result

    except Exception as e:
        logger.error(f"Collection cycle failed: {e}", exc_info=True)
        return {
            "start_time": cycle_start.isoformat(),
            "end_time": datetime.now().isoformat(),
            "success": False,
            "error": str(e),
        }


def run_once(
    config: dict[str, Any],
    source_filter: list[str] | None = None,
    generate_report: bool = False,
) -> int:
    """
    Run collection once and exit.

    Args:
        config: Configuration dictionary
        source_filter: Optional source filter
        generate_report: Whether to generate HTML report

    Returns:
        Exit code (0 = success, 1 = failure)
    """
    logger.info("Running in ONCE mode")

    result = run_collection_cycle(config, source_filter, generate_report)

    if result.get("success"):
        return 0
    else:
        return 1


def run_scheduler(
    config: dict[str, Any],
    interval_hours: int = 24,
    source_filter: list[str] | None = None,
    generate_report: bool = False,
) -> None:
    """
    Run collection on a schedule.

    Args:
        config: Configuration dictionary
        interval_hours: Hours between collection cycles
        source_filter: Optional source filter
        generate_report: Whether to generate HTML report after each cycle
    """
    logger.info(f"Running in SCHEDULER mode (interval: {interval_hours}h)")

    while True:
        try:
            result = run_collection_cycle(config, source_filter, generate_report)

            # Wait for next cycle
            sleep_seconds = interval_hours * 3600
            next_run = datetime.now().timestamp() + sleep_seconds

            logger.info(f"Next run scheduled at {datetime.fromtimestamp(next_run)}")
            logger.info(f"Sleeping for {interval_hours} hours...")

            time.sleep(sleep_seconds)

        except KeyboardInterrupt:
            logger.info("Scheduler interrupted by user")
            break
        except Exception as e:
            logger.error(f"Scheduler error: {e}", exc_info=True)
            logger.info("Retrying in 5 minutes...")
            time.sleep(300)  # Wait 5 minutes on error


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="HomeRadar - Real Estate Market Data Collector",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run once
  python main.py --mode once

  # Run scheduler (every 24 hours)
  python main.py --mode scheduler --interval 24

  # Run specific sources only
  python main.py --sources molit_apt_transaction,hankyung_realestate

  # Run with MOLIT API key
  set MOLIT_SERVICE_KEY=your_key_here
  python main.py --mode once
        """,
    )

    parser.add_argument(
        "--mode",
        choices=["once", "scheduler"],
        default="once",
        help="Execution mode (default: once)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=24,
        help="Hours between collection cycles in scheduler mode (default: 24)",
    )
    parser.add_argument(
        "--sources",
        type=str,
        help="Comma-separated list of source IDs to collect from (default: all enabled)",
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config/sources.yaml",
        help="Path to sources configuration file (default: config/sources.yaml)",
    )
    parser.add_argument(
        "--generate-report", action="store_true", help="Generate HTML report after collection"
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging()

    # Load configuration
    try:
        config = load_sources(args.config)
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        return 1

    # Parse source filter
    source_filter = None
    if args.sources:
        source_filter = [s.strip() for s in args.sources.split(",")]
        logger.info(f"Source filter: {source_filter}")

    # Run based on mode
    if args.mode == "once":
        exit_code = run_once(config, source_filter, args.generate_report)
        return exit_code
    elif args.mode == "scheduler":
        run_scheduler(config, args.interval, source_filter, args.generate_report)
        return 0


if __name__ == "__main__":
    sys.exit(main())
