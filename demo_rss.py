"""
Demo script to test RSS collector with live feeds.

This script tests the RSS collector with actual feeds from sources.yaml
"""

from datetime import datetime

import yaml

from collectors.rss_collector import RSSCollector


def load_sources():
    """Load source configuration from sources.yaml"""
    with open("config/sources.yaml", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config["sources"]


def test_rss_sources():
    """Test all enabled RSS sources"""
    sources = load_sources()

    # Filter for enabled RSS sources
    rss_sources = [s for s in sources if s["type"] == "rss" and s.get("enabled", False)]

    print(f"Testing {len(rss_sources)} enabled RSS sources...\n")
    print("=" * 80)

    results = []

    for source in rss_sources:
        source_id = source["id"]
        source_name = source["name"]

        print(f"\n[*] Testing: {source_name} ({source_id})")
        print(f"    URL: {source['url']}")

        try:
            collector = RSSCollector(source_id, source)
            items = collector.collect()

            print(f"    [OK] SUCCESS: Collected {len(items)} items")

            if items:
                # Show first item as sample
                first_item = items[0]
                print("\n    Sample Article:")
                print(f"      Title: {first_item.title[:80]}...")
                print(f"      URL: {first_item.url}")
                print(f"      Published: {first_item.published_at}")
                print(f"      Summary: {first_item.summary[:120]}...")

            results.append(
                {
                    "source_id": source_id,
                    "source_name": source_name,
                    "status": "success",
                    "count": len(items),
                }
            )

        except Exception as e:
            print(f"    [ERROR] {str(e)}")
            results.append(
                {
                    "source_id": source_id,
                    "source_name": source_name,
                    "status": "error",
                    "error": str(e),
                }
            )

        print("-" * 80)

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    success_count = sum(1 for r in results if r["status"] == "success")
    error_count = len(results) - success_count
    total_items = sum(r.get("count", 0) for r in results if r["status"] == "success")

    print(f"\nTotal sources tested: {len(results)}")
    print(f"[OK] Successful: {success_count}")
    print(f"[FAIL] Failed: {error_count}")
    print(f"[NEWS] Total items collected: {total_items}")

    if success_count > 0:
        avg_items = total_items / success_count
        print(f"[STAT] Average items per source: {avg_items:.1f}")

    # Show errors
    if error_count > 0:
        print("\n[!] Failed Sources:")
        for r in results:
            if r["status"] == "error":
                print(f"    - {r['source_name']}: {r['error']}")

    # Show successful sources
    if success_count > 0:
        print("\n[OK] Successful Sources:")
        for r in results:
            if r["status"] == "success":
                print(f"    - {r['source_name']}: {r['count']} items")


if __name__ == "__main__":
    print("HomeRadar RSS Collector Demo")
    print("=" * 80)
    print(f"Started at: {datetime.now()}")
    print()

    try:
        test_rss_sources()
    except KeyboardInterrupt:
        print("\n\n[!] Interrupted by user")
    except Exception as e:
        print(f"\n\n[ERROR] Fatal error: {e}")
        import traceback

        traceback.print_exc()

    print(f"\nFinished at: {datetime.now()}")
    print("=" * 80)
