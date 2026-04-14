# OnbidCollector and SubscriptionCollector Implementation Summary

## Overview
Successfully implemented two new collectors for HomeRadar to expand real estate data sources:
- **OnbidCollector**: Public auction property data from Onbid API
- **SubscriptionCollector**: Apartment subscription and pre-sale information

## Files Created

### Collectors
1. **collectors/onbid_collector.py** (341 lines)
   - Collects public auction property listings
   - Parses appraisal prices, minimum bids, and winning bids
   - Supports region-based filtering
   - Implements retry logic with tenacity (3 attempts, exponential backoff)
   - Handles multiple date formats and area units

2. **collectors/subscription_collector.py** (340 lines)
   - Collects apartment subscription notices and pre-sale information
   - Parses competition rates, supply prices, and subscription schedules
   - Supports region-based filtering
   - Implements retry logic with tenacity (3 attempts, exponential backoff)
   - Handles multiple date formats and area units

### Tests
1. **tests/unit/test_onbid_collector.py** (29 test cases)
   - Initialization tests (5 tests)
   - Collection tests (5 tests)
   - Item parsing tests (9 tests)
   - Date parsing tests (5 tests)
   - Area parsing tests (5 tests)

2. **tests/unit/test_subscription_collector.py** (27 test cases)
   - Initialization tests (5 tests)
   - Collection tests (5 tests)
   - Item parsing tests (7 tests)
   - Date parsing tests (5 tests)
   - Area parsing tests (5 tests)

3. **tests/unit/test_collector_registry.py** (10 test cases)
   - Registry integration tests (8 tests)
   - OnbidCollector integration tests (1 test)
   - SubscriptionCollector integration tests (1 test)

### Configuration
- **config/sources.yaml**: Added two new source definitions
  - `onbid_auction`: Onbid public auction properties
  - `subscription_apt`: Apartment subscription information

### Registry
- **collectors/registry.py**: Updated to register new collector types
  - Added `onbid` type → OnbidCollector
  - Added `subscription` type → SubscriptionCollector

## Test Results

### All New Collector Tests: ✅ 66 PASSED
- OnbidCollector: 29 tests passed
- SubscriptionCollector: 27 tests passed
- Registry Integration: 10 tests passed

### Coverage
- OnbidCollector: 80.67% coverage
- SubscriptionCollector: 80.14% coverage
- Registry: 100% coverage

### Type Checking
- ✅ No errors in OnbidCollector
- ✅ No errors in SubscriptionCollector
- ✅ No errors in Registry

## Key Features

### OnbidCollector
- **API**: Onbid OpenAPI (api.go.kr/B010003)
- **Endpoints**:
  - `OnbidCltrBidRsltListSrvc`: Auction results
  - `UnifyUsageCltr`: Property details
- **Data Fields**:
  - Auction number (cltrNo)
  - Auction name (cltrNm)
  - Appraisal price (감정가)
  - Minimum bid price (최저입찰가)
  - Winning bid price (낙찰가)
  - Location, property type, area, bid date
- **Authentication**: ONBID_API_KEY environment variable
- **Retry Logic**: 3 attempts with exponential backoff (2-10 seconds)

### SubscriptionCollector
- **API**: Korea Real Estate Board (api.odcloud.kr/api/15101046/v1)
- **Endpoint**: `getAPTLttotPblancDetail`
- **Data Fields**:
  - Project number (prjNo)
  - Project name (prjNm)
  - Notice date (noticeDate)
  - Subscription dates (subscriptionStartDate, subscriptionEndDate)
  - Competition rate (경쟁률)
  - Supply price (공급가격)
  - Location, region, property type, area
- **Authentication**: SUBSCRIPTION_API_KEY environment variable
- **Retry Logic**: 3 attempts with exponential backoff (2-10 seconds)

## Data Model

Both collectors follow the HomeRadar BaseCollector pattern and return `RawItem` objects with:
- **url**: Unique identifier (onbid://auction/{cltrNo} or subscription://project/{prjNo})
- **title**: Property/project name with location
- **summary**: Formatted summary with prices, competition rates, and property details
- **source_id**: Source identifier from config
- **published_at**: Publication/transaction date (UTC)
- **region**: Region/district
- **property_type**: Property type (아파트, 빌라, 오피스텔, etc.)
- **price**: Transaction/supply price
- **area**: Property area in square meters
- **raw_data**: Original API response data

## Error Handling

Both collectors implement robust error handling:
- Network errors: Caught and re-raised as CollectorError
- JSON parsing errors: Caught and re-raised as CollectorError
- Item parsing errors: Logged as warnings, collection continues
- Invalid data types: Gracefully handled with type checking
- Missing optional fields: Handled with None values
- Invalid prices/areas: Parsed as None, item still created

## Configuration

### Environment Variables
```bash
export ONBID_API_KEY="your_api_key"
export SUBSCRIPTION_API_KEY="your_api_key"
```

### sources.yaml Configuration
```yaml
- id: onbid_auction
  type: onbid
  enabled: true
  api_key: "optional_key_override"  # Falls back to env var

- id: subscription_apt
  type: subscription
  enabled: true
  api_key: "optional_key_override"  # Falls back to env var
```

## Usage

### Via Registry
```python
from collectors.registry import CollectorRegistry

# Create OnbidCollector
onbid_config = {
    "type": "onbid",
    "api_key": "your_key"
}
onbid_collector = CollectorRegistry.create_collector("onbid_test", onbid_config)
items = onbid_collector.collect()

# Create SubscriptionCollector
sub_config = {
    "type": "subscription",
    "api_key": "your_key"
}
sub_collector = CollectorRegistry.create_collector("subscription_test", sub_config)
items = sub_collector.collect()
```

### Direct Instantiation
```python
from collectors.onbid_collector import OnbidCollector
from collectors.subscription_collector import SubscriptionCollector

onbid = OnbidCollector("onbid_test", {"api_key": "key"})
items = onbid.collect()

subscription = SubscriptionCollector("subscription_test", {"api_key": "key"})
items = subscription.collect()
```

## Integration with HomeRadar Pipeline

Both collectors integrate seamlessly with the HomeRadar pipeline:
1. **Collection**: `collect()` returns list of RawItem objects
2. **Raw Logging**: Items logged to `data/raw/{YYYY-MM-DD}/{source}.jsonl`
3. **Entity Extraction**: Keywords matched against entity definitions
4. **Storage**: Articles upserted to DuckDB (link-based deduplication)
5. **Search Index**: Items indexed in SQLite FTS5
6. **Reporting**: HTML reports generated with Jinja2

## Future Enhancements

Potential improvements:
- Pagination support for large result sets
- Caching of API responses
- Webhook notifications for new listings
- Advanced filtering by price range, area, etc.
- Integration with graph database for relationship analysis
- Real-time monitoring of auction results

## Verification Checklist

- ✅ OnbidCollector returns 50+ items per collection (mocked)
- ✅ SubscriptionCollector returns 20+ items per collection (mocked)
- ✅ Both follow HomeRadar BaseCollector pattern (Pydantic)
- ✅ Integration tests pass (66 tests)
- ✅ lsp_diagnostics clean (no errors)
- ✅ Retry logic implemented (tenacity, 3 attempts)
- ✅ Type annotations complete
- ✅ Error handling robust
- ✅ Configuration updated
- ✅ Registry updated

## Notes

- API keys should be stored in environment variables, not committed to git
- Both collectors support region-based filtering via `collect_by_region()` method
- Date parsing supports multiple formats (YYYY-MM-DD, YYYY/MM/DD, YYYYMMDD)
- Area parsing handles units (㎡, m²) and removes them automatically
- All prices are stored as floats (원 currency)
- Collectors follow HomeRadar conventions (Black, Ruff, MyPy strict)
