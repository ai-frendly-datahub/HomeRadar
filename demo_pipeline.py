"""
Demo script to test full pipeline: RSS collection → Graph storage → Queries

This demonstrates the complete data flow from RSS feeds to database storage.
"""

import yaml
from datetime import datetime

from analyzers import EntityExtractor
from collectors.rss_collector import RSSCollector
from graph import GraphStore, get_view, get_sources_stats, search_by_keyword, get_trending_entities


def load_sources():
    """Load source configuration from sources.yaml"""
    with open("config/sources.yaml", "r", encoding="utf-8")  as f:
        config = yaml.safe_load(f)
    return config["sources"]


def collect_all_rss():
    """Collect from all enabled RSS sources"""
    sources = load_sources()
    rss_sources = [s for s in sources if s["type"] == "rss" and s.get("enabled", False)]

    print(f"[1/4] Collecting from {len(rss_sources)} RSS sources...")
    print("=" * 80)

    all_items = []

    for source in rss_sources:
        source_id = source["id"]
        source_name = source["name"]

        try:
            print(f"  - Collecting from {source_name}...", end=" ")
            collector = RSSCollector(source_id, source)
            items = collector.collect()
            print(f"[OK] {len(items)} items")
            all_items.extend(items)
        except Exception as e:
            print(f"[ERROR] {str(e)}")

    print(f"\nTotal collected: {len(all_items)} items")
    return all_items


def store_items(items):
    """Store items in graph database with entity extraction"""
    print(f"\n[2/4] Storing {len(items)} items and extracting entities...")
    print("=" * 80)

    store = GraphStore()  # Uses default path: data/homeradar.duckdb
    extractor = EntityExtractor()

    # Store items
    result = store.add_items(items)
    print(f"  - Inserted: {result['inserted']}")
    print(f"  - Updated: {result['updated']}")

    # Extract and store entities
    print(f"\n  Extracting entities...")
    total_entities = 0

    for item in items:
        # Convert RawItem to dict for extract_from_item
        item_dict = {
            "title": item.title,
            "summary": item.summary,
        }

        entities = extractor.extract_from_item(item_dict)

        if entities:
            count = store.add_entities(item.url, entities)
            total_entities += count

    print(f"  - Entities extracted: {total_entities}")

    return store


def query_data(store):
    """Query and display data from database"""
    print("\n[3/4] Querying database...")
    print("=" * 80)

    # Get stats
    stats = store.get_stats()
    print(f"\n  Database Statistics:")
    print(f"  - Total URLs: {stats['total_urls']}")
    print(f"  - Total entities: {stats['total_entities']}")
    print(f"  - Sources: {len(stats['sources'])}")

    # Show entity types
    if stats['entity_types']:
        print(f"\n  Entity Types:")
        for entity_type, unique_count in stats['entity_types'].items():
            print(f"    - {entity_type}: {unique_count} unique values")

    # Show source distribution
    print(f"\n  Source Distribution:")
    for source_id, count in sorted(stats['sources'].items(), key=lambda x: x[1], reverse=True):
        print(f"    - {source_id}: {count} items")

    # Get recent items
    print(f"\n  Recent Items (top 5):")
    recent = get_view(store, "recent", limit=5)
    for i, item in enumerate(recent, 1):
        print(f"    {i}. [{item['source_id']}] {item['title'][:60]}...")
        print(f"       Published: {item['published_at']}")

    # Show trending entities
    if stats['total_entities'] > 0:
        print(f"\n  Trending Districts (top 5):")
        trending_districts = get_trending_entities(store, "district", limit=5)
        for entity, count in trending_districts:
            print(f"    - {entity}: {count} mentions")

        print(f"\n  Trending Keywords (top 5):")
        trending_keywords = get_trending_entities(store, "keyword", limit=5)
        for entity, count in trending_keywords:
            print(f"    - {entity}: {count} mentions")

    # Search by keyword
    print(f"\n  Keyword Search ('부동산'):")
    results = search_by_keyword(store, "부동산", limit=3)
    for i, item in enumerate(results, 1):
        print(f"    {i}. {item['title'][:60]}...")

    # Source stats
    print(f"\n  Source Statistics:")
    source_stats = get_sources_stats(store)
    for stat in source_stats[:3]:
        print(f"    - {stat['source_id']}: {stat['item_count']} items")
        print(f"      Latest: {stat['latest_published']}")


def main():
    """Run full pipeline demo"""
    print("HomeRadar Full Pipeline Demo")
    print("=" * 80)
    print(f"Started at: {datetime.now()}\n")

    try:
        # Step 1: Collect RSS data
        items = collect_all_rss()

        if not items:
            print("\n[!] No items collected. Exiting.")
            return

        # Step 2: Store in database
        store = store_items(items)

        # Step 3: Query and display
        query_data(store)

        # Step 4: Summary
        print("\n[4/4] Summary")
        print("=" * 80)
        print(f"  [OK] Pipeline completed successfully!")
        print(f"  [OK] Data stored in: {store.db_path}")
        print(f"  [OK] {len(items)} items collected and stored")

    except KeyboardInterrupt:
        print("\n\n[!] Interrupted by user")
    except Exception as e:
        print(f"\n\n[ERROR] Fatal error: {e}")
        import traceback
        traceback.print_exc()

    print(f"\nFinished at: {datetime.now()}")
    print("=" * 80)


if __name__ == "__main__":
    main()
