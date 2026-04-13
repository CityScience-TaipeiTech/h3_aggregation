#!/usr/bin/env python3
"""
Quick HBase integration test script.

Run directly from project root:
    export HBASE_FETCH_API="http://10.100.1.64:2891/api/hbase/v1/test/filterdata2"
    export HBASE_TOKEN="your-token"
    python test_hbase_quick.py

This script:
1. Tests connectivity to HBase
2. Fetches a small sample of data
3. Shows you the results immediately
"""

import os
import sys
from pathlib import Path

# Add project to path
sys.path.insert(0, str(Path(__file__).parent))

import polars as pl
from h3_toolkit.hbase import HBaseClient


def main():
    print("🚀 HBase Quick Test Script")
    print("=" * 60)

    # Get credentials from environment
    fetch_url = os.getenv('HBASE_FETCH_API')
    token = os.getenv('HBASE_TOKEN')

    if not fetch_url or not token:
        print("\n❌ Missing credentials!")
        print("\nSet environment variables first:")
        print('  export HBASE_FETCH_API="http://10.100.1.64:2891/api/hbase/v1/test/filterdata2"')
        print('  export HBASE_TOKEN="your-token-here"')
        print("\nThen run:")
        print("  python test_hbase_quick.py")
        sys.exit(1)

    print(f"\n✅ Credentials found")
    print(f"   URL: {fetch_url[:60]}...")
    print(f"   Token: {token[:20]}...{token[-10:]}")

    # Load test hex IDs
    print("\n📂 Loading test hex IDs...")
    test_data_path = Path(__file__).parent / 'tests' / 'data' / 'test_resolution_10.csv'
    df = pl.read_csv(test_data_path)
    hex_ids = df['hex_id'].to_list()
    print(f"   ✅ Loaded {len(hex_ids)} hex IDs from test data")

    # Create HBase client
    print("\n🔌 Creating HBase client...")
    client = HBaseClient(
        fetch_url=fetch_url,
        send_url=fetch_url,  # Use same URL
        token=token,
        max_concurrent_requests=5,
        chunk_size=50
    )
    print(f"   ✅ Client created")

    # Test 1: Quick connectivity check with 3 hex IDs
    print("\n" + "=" * 60)
    print("TEST 1: Quick Connectivity Check (3 hex IDs)")
    print("=" * 60)
    try:
        result = client.fetch_data(
            table_name='segis_population_statistic',
            column_family='segis_population_statistic',
            column_qualifier=['p_cnt'],
            rowkeys=hex_ids[:3],
            timerange=('2020-01-01T00:00:00Z', '2024-12-31T23:59:59Z')
        )
        print(f"✅ Connection successful!")
        print(f"   Queried: 3 hex IDs")
        print(f"   Returned: {len(result)} rows")
        print(f"   Columns: {result.columns}")
        if not result.is_empty():
            print(f"\n   Sample data:")
            print(result.head())
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return 1

    # Test 2: Full dataset test with timerange
    print("\n" + "=" * 60)
    print("TEST 2: Full Dataset (All 24 hex IDs, 2020-2024)")
    print("=" * 60)
    print(f"This may take 1-5 minutes...\n")
    try:
        result = client.fetch_data(
            table_name='segis_population_statistic',
            column_family='segis_population_statistic',
            column_qualifier=['p_cnt'],
            rowkeys=hex_ids,
            timerange=('2020-01-01T00:00:00Z', '2024-12-31T23:59:59Z')
        )
        print(f"✅ Full query successful!")
        print(f"   Queried: {len(hex_ids)} hex IDs")
        print(f"   Returned: {len(result)} rows")
        print(f"   Columns: {result.columns}")

        if not result.is_empty():
            # Calculate statistics
            p_cnt_stats = result.select(pl.col('p_cnt').cast(pl.Float64)).describe()
            print(f"\n   Statistics for p_cnt:")
            print(p_cnt_stats)

            # Show sample
            print(f"\n   First 10 rows:")
            print(result.head(10))

            # Coverage
            coverage = len(result) / len(hex_ids) * 100
            print(f"\n   Data coverage: {coverage:.1f}% ({len(result)}/{len(hex_ids)} hex IDs have data)")

    except Exception as e:
        print(f"❌ Query failed: {e}")
        return 1

    # Test 3: Without timerange
    print("\n" + "=" * 60)
    print("TEST 3: Without Timerange (All Historical Data)")
    print("=" * 60)
    print(f"Querying first 5 hex IDs (no time filter)...\n")
    try:
        result = client.fetch_data(
            table_name='segis_population_statistic',
            column_family='segis_population_statistic',
            column_qualifier=['p_cnt'],
            rowkeys=hex_ids[:5]
        )
        print(f"✅ Query successful!")
        print(f"   Queried: 5 hex IDs")
        print(f"   Returned: {len(result)} rows")
        if not result.is_empty():
            print(f"\n   Sample data:")
            print(result.head())
    except Exception as e:
        print(f"❌ Query failed: {e}")
        return 1

    # Summary
    print("\n" + "=" * 60)
    print("✅ ALL TESTS PASSED!")
    print("=" * 60)
    print("\n📝 Next steps:")
    print("   1. Run full integration tests:")
    print("      pytest tests/test_hbase_integration.py -v -s")
    print("\n   2. Check data quality with analysis tests:")
    print("      pytest tests/test_hbase_integration.py::TestHBaseDataAnalysis -v -s")
    print("\n   3. Run unit tests to ensure no regressions:")
    print("      pytest tests/test_hbase.py -v")
    print()

    return 0


if __name__ == '__main__':
    sys.exit(main())
