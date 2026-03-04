"""
Demo script to test MOLIT API collector.

This script demonstrates:
1. Collecting apartment transaction data from MOLIT OpenAPI
2. Storing transactions in the database
3. Querying and analyzing transaction data

Before running:
1. Get API key from https://www.data.go.kr/data/15126469/openapi.do
2. Set environment variable: MOLIT_SERVICE_KEY=your_key_here
"""

import os
from datetime import datetime

from collectors.molit_collector import MOLITCollector
from graph import GraphStore, get_price_statistics, get_transactions


def get_service_key() -> str:
    """Get MOLIT API service key from environment."""
    service_key = os.environ.get('MOLIT_SERVICE_KEY')

    if not service_key:
        print("[!] MOLIT_SERVICE_KEY environment variable not set.")
        print("    Get your key from: https://www.data.go.kr/data/15126469/openapi.do")
        print("    Then run: set MOLIT_SERVICE_KEY=your_key_here")
        raise ValueError("MOLIT_SERVICE_KEY not found")

    return service_key


def collect_transactions():
    """Collect apartment transaction data from MOLIT API."""
    print("[1/3] Collecting transaction data from MOLIT API...")
    print("=" * 80)

    # Get service key
    try:
        service_key = get_service_key()
    except ValueError:
        return []

    # Configure source
    source = {
        'service_key': service_key,
        'url': 'http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev',
        'num_of_rows': 1000,
    }

    collector = MOLITCollector('molit_apt_transaction', source)

    # Test with Gangnam-gu (11680), November 2024
    lawd_cd = '11680'  # Seoul Gangnam-gu
    deal_ymd = '202411'  # November 2024

    print(f"  Region: Gangnam-gu (법정동코드: {lawd_cd})")
    print(f"  Period: {deal_ymd[:4]}년 {deal_ymd[4:6]}월")
    print()

    try:
        print(f"  Fetching data...", end=" ")
        items = collector.collect(lawd_cd, deal_ymd)
        print(f"[OK] {len(items)} transactions collected")

        if items:
            # Show sample
            print(f"\n  Sample Transaction:")
            sample = items[0]
            print(f"    Title: {sample.title}")
            print(f"    Price: {sample.price:,}만원")
            print(f"    Area: {sample.area}㎡")
            print(f"    Region: {sample.region}")
            print(f"    Date: {sample.published_at.date()}")

        return items

    except Exception as e:
        print(f"[ERROR] {str(e)}")
        return []


def store_transactions(items):
    """Store transactions in database."""
    print(f"\n[2/3] Storing {len(items)} transactions...")
    print("=" * 80)

    if not items:
        print("  [!] No items to store.")
        return None

    store = GraphStore()

    # Store items
    result = store.add_items(items)
    print(f"  - Inserted: {result['inserted']}")
    print(f"  - Updated: {result['updated']}")

    return store


def query_transactions(store):
    """Query and analyze transaction data."""
    print("\n[3/3] Querying transaction data...")
    print("=" * 80)

    if store is None:
        print("  [!] No data to query.")
        return

    # Get all transactions
    transactions = get_transactions(store, property_type='아파트', limit=10)

    print(f"\n  Recent Transactions (top 10):")
    for i, tx in enumerate(transactions, 1):
        print(f"    {i}. {tx['title'][:50]}...")
        print(f"       Price: {tx['price']:,}만원 | Area: {tx['area']}㎡ | Date: {tx['published_at']}")

    # Get price statistics
    stats = get_price_statistics(store, property_type='아파트')

    print(f"\n  Price Statistics:")
    print(f"    - Total transactions: {stats['count']}")
    if stats['avg_price']:
        print(f"    - Average price: {stats['avg_price']:,.0f}만원")
        print(f"    - Min price: {stats['min_price']:,.0f}만원")
        print(f"    - Max price: {stats['max_price']:,.0f}만원")

    # Get price range distribution
    print(f"\n  Price Range Distribution:")

    # Under 50,000만원
    low = get_transactions(store, property_type='아파트', max_price=50000, limit=1000)
    print(f"    - Under 5억: {len(low)} transactions")

    # 50,000 - 100,000만원
    mid = get_transactions(store, property_type='아파트', min_price=50000, max_price=100000, limit=1000)
    print(f"    - 5억 ~ 10억: {len(mid)} transactions")

    # Over 100,000만원
    high = get_transactions(store, property_type='아파트', min_price=100000, limit=1000)
    print(f"    - Over 10억: {len(high)} transactions")


def main():
    """Run MOLIT API demo."""
    print("HomeRadar MOLIT API Demo")
    print("=" * 80)
    print(f"Started at: {datetime.now()}\n")

    try:
        # Step 1: Collect transactions
        items = collect_transactions()

        if not items:
            print("\n[!] No transactions collected. Exiting.")
            print("\nPossible issues:")
            print("  - MOLIT_SERVICE_KEY not set or invalid")
            print("  - API rate limit reached")
            print("  - Network connection issue")
            print("  - No transactions for the selected period/region")
            return

        # Step 2: Store in database
        store = store_transactions(items)

        # Step 3: Query and display
        query_transactions(store)

        # Summary
        print("\n[OK] Demo completed successfully!")
        print(f"[OK] {len(items)} transactions collected and stored")

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
