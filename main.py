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
import json
import logging
import os
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import yaml

from analyzers import EntityExtractor
from collectors import CollectorRegistry, RawItem
from collectors.base import resolve_max_workers
from config_loader import load_notification_config
from date_storage import apply_date_storage_policy
from graph import GraphStore
from graph.search_index import SearchIndex
from homeradar.config_loader import load_settings
from homeradar.common.validators import (
    validate_area,
    validate_article,
    validate_location,
    validate_price,
)
from homeradar.home_signals import build_source_lookup, enrich_home_verification_fields
from homeradar.quality_report import build_quality_report, write_quality_report
from notifier import (
    CompositeNotifier,
    EmailNotifier,
    NotificationConfig,
    NotificationPayload,
    WebhookNotifier,
    detect_home_notifications,
)
from raw_logger import RawLogger
from homeradar.reporter import generate_index_html
from radar_core.report_utils import generate_summary_json as _generate_summary_json
from reporters.html_reporter import HtmlReporter


# Logger (configured in setup_logging())
logger = logging.getLogger(__name__)


def _daily_report_path(cycle_start: datetime, report_dir: Path) -> Path:
    """Return the date-stamped daily report path for a collection cycle."""
    stamp = cycle_start.astimezone(UTC).strftime("%Y%m%d")
    return report_dir / f"daily_report_{stamp}.html"


def _update_latest_report(report_path: Path) -> Path:
    """Keep a stable latest-report filename while preserving the dated report."""
    latest_path = report_path.parent / "daily_report.html"
    if report_path != latest_path:
        shutil.copy2(report_path, latest_path)
    return latest_path


def _summary_entity_map(store: GraphStore, urls: list[str]) -> dict[str, dict[str, list[str]]]:
    """Return report-summary entity matches keyed by URL."""
    if not urls:
        return {}

    placeholders = ", ".join("?" for _ in urls)
    query = f"""
        SELECT url, entity_type, entity_value
        FROM url_entities
        WHERE url IN ({placeholders})
        ORDER BY url, entity_type, entity_value
    """

    try:
        with store._connection() as conn:
            rows = conn.execute(query, urls).fetchall()
    except Exception as exc:
        logger.warning("Failed to load HomeRadar summary entities: %s", exc)
        return {}

    entity_map: dict[str, dict[str, list[str]]] = {}
    for url, entity_type, entity_value in rows:
        url_key = str(url)
        entity_key = str(entity_type)
        value = str(entity_value)
        entity_map.setdefault(url_key, {}).setdefault(entity_key, []).append(value)
    return entity_map


def _item_summary_payload(item: RawItem, matched_entities: dict[str, list[str]]) -> dict[str, Any]:
    published_at = item.published_at.isoformat() if item.published_at else None
    collected_at = item.collected_at.isoformat() if item.collected_at else None
    return {
        "title": item.title,
        "link": item.url,
        "source": item.source_id,
        "summary": item.summary,
        "published_at": published_at,
        "collected_at": collected_at,
        "matched_entities": matched_entities,
    }


def _row_summary_payload(
    row: dict[str, Any],
    matched_entities: dict[str, list[str]],
) -> dict[str, Any]:
    published_at = row.get("published_at")
    collected_at = row.get("created_at") or row.get("updated_at")
    published_value = (
        published_at.isoformat() if hasattr(published_at, "isoformat") else published_at
    )
    collected_value = (
        collected_at.isoformat() if hasattr(collected_at, "isoformat") else collected_at
    )
    return {
        "title": row.get("title", ""),
        "link": row.get("url", ""),
        "source": row.get("source_id", ""),
        "summary": row.get("summary", ""),
        "published_at": published_value,
        "collected_at": collected_value,
        "matched_entities": matched_entities,
    }


def _summary_articles(
    store: GraphStore,
    fallback_items: list[RawItem],
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Build the standard report summary article payload from the DB report window."""
    try:
        recent_items = store.get_recent_items(limit=limit)
    except Exception as exc:
        logger.warning("Failed to load HomeRadar summary rows: %s", exc)
        recent_items = []

    if not isinstance(recent_items, list):
        recent_items = []

    recent_rows = [item for item in recent_items if isinstance(item, dict)]
    if recent_rows:
        urls = [str(item.get("url", "")) for item in recent_rows if item.get("url")]
        entity_map = _summary_entity_map(store, urls)
        return [
            _row_summary_payload(item, entity_map.get(str(item.get("url", "")), {}))
            for item in recent_rows
        ]

    urls = [item.url for item in fallback_items]
    entity_map = _summary_entity_map(store, urls)
    return [_item_summary_payload(item, entity_map.get(item.url, {})) for item in fallback_items]


def _write_summary_report(
    *,
    store: GraphStore,
    report_dir: Path,
    fallback_items: list[RawItem],
    stats: dict[str, int],
    quality_report: dict[str, Any] | None = None,
) -> Path:
    """Write the radar-core compatible HomeRadar summary JSON."""
    articles = _summary_articles(store, fallback_items)
    source_count = len(
        {
            str(article.get("source", ""))
            for article in articles
            if str(article.get("source", "")).strip()
        }
    )
    computed_matched = sum(1 for article in articles if article.get("matched_entities"))
    matched_count = computed_matched or int(stats.get("matched", 0))

    summary_stats = {
        "article_count": len(articles),
        "source_count": source_count,
        "matched_count": matched_count,
    }
    summary_path = _generate_summary_json("home", articles, summary_stats, report_dir)
    _augment_summary_with_quality(summary_path, quality_report)
    return summary_path


def _augment_summary_with_quality(
    summary_path: Path,
    quality_report: dict[str, Any] | None,
) -> None:
    quality_payload = _summary_quality_payload(quality_report)
    if not quality_payload:
        return

    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Failed to augment HomeRadar summary with quality data: %s", exc)
        return

    warnings = list(summary.get("warnings") or [])
    warnings.extend(quality_payload.pop("warnings", []))
    summary.update(quality_payload)
    if warnings:
        summary["warnings"] = warnings

    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _summary_quality_payload(quality_report: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(quality_report, dict):
        return {}
    summary = quality_report.get("summary")
    if not isinstance(summary, dict):
        return {}

    quality_keys = [
        "total_sources",
        "enabled_sources",
        "tracked_sources",
        "fresh_sources",
        "stale_sources",
        "stale_source_ids",
        "missing_sources",
        "missing_source_ids",
        "unknown_event_date_sources",
        "unknown_event_date_source_ids",
        "skipped_sources",
        "skipped_disabled_sources",
        "skipped_disabled_source_ids",
        "skipped_missing_env_sources",
        "skipped_missing_env_source_ids",
        "verification_state_count",
        "official_primary_status",
        "official_primary_sources",
        "official_primary_fresh_sources",
        "official_primary_credentialed_sources",
        "official_primary_blocked_sources",
        "official_primary_blocked_source_ids",
        "official_primary_required_env",
        "corroboration_only_items",
        "official_primary_items",
    ]
    quality_summary = {key: summary[key] for key in quality_keys if key in summary}
    if not quality_summary:
        return {}

    verification_states = quality_report.get("verification_states")
    warnings: list[str] = []
    if summary.get("official_primary_status") == "blocked_missing_env":
        blocked_sources = ", ".join(summary.get("official_primary_blocked_source_ids") or [])
        required_env = ", ".join(summary.get("official_primary_required_env") or [])
        warnings.append(
            "official primary HomeRadar sources blocked by missing env"
            f" ({blocked_sources}; required: {required_env})"
        )
    if summary.get("stale_sources") or summary.get("missing_sources") or summary.get(
        "unknown_event_date_sources"
    ):
        warnings.append(
            "tracked HomeRadar sources need review"
            f" (stale={summary.get('stale_sources', 0)},"
            f" missing={summary.get('missing_sources', 0)},"
            f" unknown_event_date={summary.get('unknown_event_date_sources', 0)})"
        )

    payload = {
        "quality_summary": quality_summary,
        "warnings": warnings,
    }
    if isinstance(verification_states, dict) and verification_states:
        payload["verification_states"] = verification_states
    return payload


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
        with open(config_path, encoding="utf-8") as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        logger.error(f"Failed to load sources config: {e}")
        raise


def missing_required_env(source: dict[str, Any]) -> str | None:
    """Return the missing env var required for a source, if any."""
    source_id = str(source.get("id", "")).strip()
    source_type = str(source.get("type", "")).strip().lower()

    if source_type == "api" and source_id.startswith("molit"):
        if source.get("service_key") or os.environ.get("MOLIT_SERVICE_KEY"):
            return None
        return "MOLIT_SERVICE_KEY"

    if source_type == "subscription":
        if source.get("api_key") or os.environ.get("SUBSCRIPTION_API_KEY"):
            return None
        return "SUBSCRIPTION_API_KEY"

    if source_type == "onbid":
        if source.get("api_key") or os.environ.get("ONBID_API_KEY"):
            return None
        return "ONBID_API_KEY"

    return None


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

    workers = resolve_max_workers()

    def _collect_for_source(
        source: dict[str, Any],
    ) -> tuple[str, str, str, list[RawItem], str | None]:
        source_id = source["id"]
        source_name = source.get("name", source_id)
        source_type = source["type"]
        try:
            missing_env = missing_required_env(source)
            if missing_env is not None:
                logger.warning(f"    Skipping {source_id}: {missing_env} not set")
                return source_id, source_name, source_type, [], None

            collector = CollectorRegistry.create_collector(source_id, source)
            if source_type == "api" and source_id.startswith("molit"):
                items = collect_molit(collector, source)
            else:
                items = collector.collect()
            return source_id, source_name, source_type, items, None
        except Exception as exc:
            return source_id, source_name, source_type, [], str(exc)

    if workers == 1:
        results = [_collect_for_source(source) for source in filtered_sources]
    else:
        results = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(_collect_for_source, source) for source in filtered_sources]
            for future in as_completed(futures):
                results.append(future.result())

    for source_id, source_name, source_type, items, error in results:
        logger.info(f"  [{source_type}] {source_name} ({source_id})")
        if error is not None:
            logger.error(f"    Failed to collect from {source_id}: {error}")
            continue
        logger.info(f"    Collected {len(items)} items")
        raw_logger.log(items, source_name=source_id)
        all_items.extend(items)

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
    deal_ymd = source.get("deal_ymd", datetime.now(tz=UTC).strftime("%Y%m"))

    # Check for service key
    if "service_key" not in source:
        service_key = os.environ.get("MOLIT_SERVICE_KEY")
        if not service_key:
            logger.warning("    Skipping MOLIT: MOLIT_SERVICE_KEY not set")
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
        return {"stored": 0, "entities": 0, "matched": 0}

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
    matched_items = 0

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
                if count > 0:
                    matched_items += 1

        except Exception as e:
            logger.error(f"  Failed to extract entities for {item.url}: {e}")
            continue

    logger.info(f"  Entities extracted: {total_entities}")

    return {
        "stored": result["inserted"] + result["updated"],
        "entities": total_entities,
        "matched": matched_items,
    }


def run_collection_cycle(
    config: dict[str, Any],
    source_filter: list[str] | None = None,
    generate_report: bool = False,
    notifier: CompositeNotifier | None = None,
    notification_rules: dict[str, Any] | None = None,
    keep_days: int = 90,
    keep_raw_days: int = 180,
    keep_report_days: int = 90,
    snapshot_db: bool = False,
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
    cycle_start = datetime.now(tz=UTC)
    logger.info("=" * 80)
    logger.info(f"Starting collection cycle at {cycle_start}")
    logger.info("=" * 80)

    try:
        settings = load_settings()
        # Initialize storage
        store = GraphStore()

        # Collect from sources
        sources = config.get("sources", [])
        items = collect_from_sources(sources, enabled_only=True, source_filter=source_filter)
        source_lookup = build_source_lookup(sources)
        items = enrich_home_verification_fields(items, source_lookup)

        known_urls = _get_existing_urls(store)
        previous_region_prices = _get_region_price_baseline(store)

        validated_items: list[RawItem] = []
        for item in items:
            is_valid, validation_errors = validate_article(item)

            normalized_price = int(item.price) if item.price is not None else None
            if not validate_price(normalized_price):
                validation_errors.append(f"price out of range: {item.price}")
            if not validate_area(item.area):
                validation_errors.append(f"area out of range: {item.area}")
            if item.region is not None and not validate_location(item.region):
                validation_errors.append(f"invalid location: {item.region}")

            if validation_errors:
                logger.warning(
                    "Skipping invalid item %s: %s",
                    item.url,
                    "; ".join(validation_errors),
                )
                continue

            if is_valid:
                validated_items.append(item)

        # Store and extract
        stats = store_and_extract(validated_items, store)

        deleted = store.delete_older_than(keep_days)
        if deleted:
            logger.info(f"Deleted {deleted} records older than {keep_days} days")

        if notifier is not None and validated_items:
            events = detect_home_notifications(
                validated_items,
                previous_region_prices=previous_region_prices,
                known_urls=known_urls,
                rules=notification_rules or {},
            )
            for event in events:
                notifier.send(
                    NotificationPayload(
                        category_name=event.title,
                        sources_count=0,
                        collected_count=0,
                        matched_count=0,
                        errors_count=0,
                        timestamp=datetime.now(tz=UTC),
                    )
                )

        # Get database stats
        db_stats = store.get_stats()

        quality_report = build_quality_report(
            sources=sources,
            store=store,
            quality_config=config,
            generated_at=cycle_start,
        )
        quality_report_paths = write_quality_report(
            quality_report,
            output_dir=settings.report_dir,
            category_name="home",
        )
        logger.info("Quality report saved to %s", quality_report_paths["latest"])

        # Generate report if requested
        report_path = None
        latest_report_path = None
        summary_report_path = None
        if generate_report:
            try:
                logger.info("Generating HTML report...")
                reporter = HtmlReporter()
                report_file = _daily_report_path(cycle_start, settings.report_dir)
                report_path = reporter.generate_report(
                    store,
                    report_file,
                    stats=stats,
                    quality_report=quality_report,
                )
                logger.info(f"  Report saved to {report_path}")
                latest_report_path = _update_latest_report(Path(report_path))
                logger.info(f"  Latest report updated at {latest_report_path}")
                # Generate index.html
                index_path = generate_index_html(settings.report_dir)
                logger.info(f"  Index generated at {index_path}")
                summary_report_path = _write_summary_report(
                    store=store,
                    report_dir=settings.report_dir,
                    fallback_items=validated_items,
                    stats=stats,
                    quality_report=quality_report,
                )
                logger.info(f"  Summary saved to {summary_report_path}")
            except Exception as e:
                logger.error(f"Failed to generate report: {e}")

        date_storage = apply_date_storage_policy(
            database_path=Path(store.db_path),
            raw_data_dir=settings.raw_data_dir,
            report_dir=settings.report_dir,
            keep_raw_days=keep_raw_days,
            keep_report_days=keep_report_days,
            snapshot_db=snapshot_db,
        )
        snapshot_path = date_storage.get("snapshot_path")
        if isinstance(snapshot_path, str) and snapshot_path:
            logger.info("Snapshot saved to %s", snapshot_path)

        cycle_end = datetime.now(tz=UTC)
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
        if latest_report_path:
            result["latest_report_path"] = str(latest_report_path)
        if summary_report_path:
            result["summary_report_path"] = str(summary_report_path)
        result["quality_report_path"] = str(quality_report_paths["latest"])
        result["dated_quality_report_path"] = str(quality_report_paths["dated"])
        result["date_storage"] = date_storage

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
            "end_time": datetime.now(tz=UTC).isoformat(),
            "success": False,
            "error": str(e),
        }


def run_once(
    config: dict[str, Any],
    source_filter: list[str] | None = None,
    generate_report: bool = False,
    notifier: CompositeNotifier | None = None,
    notification_rules: dict[str, Any] | None = None,
    keep_days: int = 90,
    keep_raw_days: int = 180,
    keep_report_days: int = 90,
    snapshot_db: bool = False,
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

    result = run_collection_cycle(
        config,
        source_filter,
        generate_report,
        notifier,
        notification_rules,
        keep_days,
        keep_raw_days,
        keep_report_days,
        snapshot_db,
    )

    if result.get("success"):
        return 0
    else:
        return 1


def run_scheduler(
    config: dict[str, Any],
    interval_hours: int = 24,
    source_filter: list[str] | None = None,
    generate_report: bool = False,
    notifier: CompositeNotifier | None = None,
    notification_rules: dict[str, Any] | None = None,
    keep_days: int = 90,
    keep_raw_days: int = 180,
    keep_report_days: int = 90,
    snapshot_db: bool = False,
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
            _ = run_collection_cycle(
                config,
                source_filter,
                generate_report,
                notifier,
                notification_rules,
                keep_days,
                keep_raw_days,
                keep_report_days,
                snapshot_db,
            )

            # Wait for next cycle
            sleep_seconds = interval_hours * 3600
            next_run = datetime.now(tz=UTC).timestamp() + sleep_seconds

            logger.info(f"Next run scheduled at {datetime.fromtimestamp(next_run, tz=UTC)}")
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
  # Run once with dated report and DB snapshot
  python main.py --mode once --generate-report --snapshot-db

  # Run scheduler (every 24 hours)
  python main.py --daily

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
        "--keep-days",
        type=int,
        default=90,
        help="Retention period in days (default: 90)",
    )
    parser.add_argument(
        "--notifications-config",
        type=str,
        default="config/notifications.yaml",
        help="Path to notifications configuration file",
    )
    parser.add_argument(
        "--generate-report", action="store_true", help="Generate HTML report after collection"
    )
    parser.add_argument(
        "--snapshot-db",
        action="store_true",
        help="Write a date-stamped DuckDB snapshot after each collection cycle",
    )
    parser.add_argument(
        "--keep-raw-days",
        type=int,
        default=180,
        help="Retention period for date-partitioned raw JSONL directories (default: 180)",
    )
    parser.add_argument(
        "--keep-report-days",
        type=int,
        default=90,
        help="Retention period for date-stamped HTML reports (default: 90)",
    )
    parser.add_argument(
        "--daily",
        action="store_true",
        help="Run every 24 hours and write dated report/DB snapshot artifacts",
    )

    args = parser.parse_args()
    if args.daily:
        args.mode = "scheduler"
        args.interval = 24
        args.generate_report = True
        args.snapshot_db = True

    # Setup logging
    setup_logging()

    # Load configuration
    try:
        config = load_sources(args.config)
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        return 1

    notification_config = load_notification_config(Path(args.notifications_config))
    notifier = _build_notifier(notification_config)

    # Parse source filter
    source_filter = None
    if args.sources:
        source_filter = [s.strip() for s in args.sources.split(",")]
        logger.info(f"Source filter: {source_filter}")

    # Run based on mode
    if args.mode == "once":
        exit_code = run_once(
            config,
            source_filter,
            args.generate_report,
            notifier,
            notification_config.rules,
            args.keep_days,
            args.keep_raw_days,
            args.keep_report_days,
            args.snapshot_db,
        )
        return exit_code
    elif args.mode == "scheduler":
        run_scheduler(
            config,
            args.interval,
            source_filter,
            args.generate_report,
            notifier,
            notification_config.rules,
            args.keep_days,
            args.keep_raw_days,
            args.keep_report_days,
            args.snapshot_db,
        )
        return 0


def _build_notifier(config: NotificationConfig) -> CompositeNotifier:
    notifiers: list[object] = []
    if not config.enabled:
        return CompositeNotifier(notifiers)

    channels = {channel.strip().lower() for channel in config.channels}
    email_settings = config.email_settings
    if "email" in channels and email_settings:
        to_addresses = email_settings.get("to_addresses", email_settings.get("to_addrs", []))
        if isinstance(to_addresses, list):
            notifiers.append(
                EmailNotifier(
                    smtp_host=str(email_settings.get("smtp_host", "")),
                    smtp_port=int(email_settings.get("smtp_port", 587)),
                    smtp_user=str(
                        email_settings.get("username", email_settings.get("smtp_user", ""))
                    ),
                    smtp_password=str(
                        email_settings.get("password", email_settings.get("smtp_password", ""))
                    ),
                    from_addr=str(
                        email_settings.get("from_address", email_settings.get("from_addr", ""))
                    ),
                    to_addrs=[str(address) for address in to_addresses],
                )
            )

    if "webhook" in channels and config.webhook_url:
        notifiers.append(WebhookNotifier(url=config.webhook_url))

    return CompositeNotifier(notifiers)


def _get_existing_urls(store: GraphStore) -> set[str]:
    try:
        with duckdb.connect(str(store.db_path)) as conn:
            rows = conn.execute("SELECT url FROM urls").fetchall()
        return {str(row[0]) for row in rows}
    except Exception:
        return set()


def _get_region_price_baseline(store: GraphStore) -> dict[str, float]:
    try:
        with duckdb.connect(str(store.db_path)) as conn:
            rows = conn.execute(
                """
                SELECT region, AVG(price)
                FROM urls
                WHERE region IS NOT NULL AND price IS NOT NULL
                GROUP BY region
                """
            ).fetchall()
        return {str(region): float(avg_price) for region, avg_price in rows}
    except Exception:
        return {}


if __name__ == "__main__":
    sys.exit(main())
