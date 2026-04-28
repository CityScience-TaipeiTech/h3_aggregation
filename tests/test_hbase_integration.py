"""
Integration tests for HBaseClient with real HBase backend.

These tests connect to an actual HBase server and fetch real data.
To run: pytest tests/test_hbase_integration.py -v -s

Environment variables:
    HBASE_FETCH_API: HBase fetch API endpoint (e.g., http://10.100.1.64:2891/api/hbase/v1/test/filterdata2)
    HBASE_TOKEN: Authentication token for HBase
"""

import os
from pathlib import Path

import polars as pl
import pytest

from h3_toolkit.hbase import HBaseClient

# ==================== HBase Query Configuration ====================
# Modify these variables to change what data the tests query
HBASE_TABLE_NAME = "res_10_time_data"
HBASE_COLUMN_FAMILY = "segis_population_statistic"
HBASE_COLUMN_QUALIFIERS = ["p_cnt"]
HBASE_TIMERANGE_START = "2020-01-01"
HBASE_TIMERANGE_END = "2024-12-31"
# ===================================================================


@pytest.fixture
def hbase_config():
    """Get HBase configuration from environment variables."""
    fetch_url = os.getenv("HBASE_FETCH_API")
    token = os.getenv("HBASE_TOKEN")

    if not fetch_url or not token:
        pytest.skip("HBASE_FETCH_API and HBASE_TOKEN environment variables not set")

    return {
        "fetch_url": fetch_url,
        "send_url": None,  # Not used for fetch-only tests
        "token": token,
    }


@pytest.fixture
def hbase_client(hbase_config):
    """Create an HBaseClient instance with real backend."""
    return HBaseClient(
        fetch_url=hbase_config["fetch_url"],
        send_url=hbase_config["send_url"] or hbase_config["fetch_url"],
        token=hbase_config["token"],
        max_concurrent_requests=5,
        chunk_size=100,
    )


@pytest.fixture
def test_hex_ids():
    """Load hex IDs from test data CSV file."""
    test_data_path = Path(__file__).parent / "data" / "test_resolution_10.csv"
    df = pl.read_csv(test_data_path)
    return df["hex_id"].to_list()


class TestHBaseIntegration:
    """Integration tests with real HBase backend."""

    def test_fetch_data_with_timerange(self, hbase_client, test_hex_ids):
        """
        Test fetching population data from HBase with time range filter.

        Queries:
            Table: HBASE_TABLE_NAME
            Column Family: HBASE_COLUMN_FAMILY
            Column Qualifiers: HBASE_COLUMN_QUALIFIERS
            Time Range: HBASE_TIMERANGE_START to HBASE_TIMERANGE_END
            Row Keys: H3 indices from test data
        """
        result = hbase_client.fetch_data(
            table_name=HBASE_TABLE_NAME,
            column_family=HBASE_COLUMN_FAMILY,
            column_qualifier=HBASE_COLUMN_QUALIFIERS,
            rowkeys=test_hex_ids,
            timerange=(HBASE_TIMERANGE_START, HBASE_TIMERANGE_END),
        )

        # Verify result is a valid DataFrame
        assert isinstance(result, pl.DataFrame)
        assert not result.is_empty()

        # Verify expected columns are present
        assert "hex_id" in result.columns
        for col_qual in HBASE_COLUMN_QUALIFIERS:
            assert col_qual in result.columns

        # Print sample results for inspection
        print("\n✅ Fetch successful!")
        print(f"Rows returned: {len(result)}")
        print(f"Columns: {result.columns}")
        print("\nFirst 5 rows:")
        print(result.head())

    def test_fetch_data_without_timerange(self, hbase_client, test_hex_ids):
        """
        Test fetching data without time range (all available data).
        """
        result = hbase_client.fetch_data(
            table_name=HBASE_TABLE_NAME,
            column_family=HBASE_COLUMN_FAMILY,
            column_qualifier=HBASE_COLUMN_QUALIFIERS,
            rowkeys=test_hex_ids[:5],  # Use first 5 hex IDs for quick test
        )

        # Verify result
        assert isinstance(result, pl.DataFrame)
        assert not result.is_empty()
        assert "hex_id" in result.columns

        print("\n✅ Fetch without timerange successful!")
        print(f"Rows returned: {len(result)}")
        print(result.head())

    def test_fetch_data_small_subset(self, hbase_client, test_hex_ids):
        """
        Quick test with small subset to verify connectivity.
        """
        # Use just the first 3 hex IDs
        hex_ids_subset = test_hex_ids[:3]

        result = hbase_client.fetch_data(
            table_name=HBASE_TABLE_NAME,
            column_family=HBASE_COLUMN_FAMILY,
            column_qualifier=HBASE_COLUMN_QUALIFIERS,
            rowkeys=hex_ids_subset,
            timerange=(HBASE_TIMERANGE_START, HBASE_TIMERANGE_END),
        )

        assert isinstance(result, pl.DataFrame)
        print("\n✅ Small subset test passed!")
        print(f"Queried {len(hex_ids_subset)} hex IDs")
        print(f"Returned {len(result)} rows")
        if not result.is_empty():
            print(f"Sample hex_id: {result['hex_id'][0]}")

    def test_fetch_multiple_column_qualifiers(self, hbase_client, test_hex_ids):
        """
        Test fetching multiple column qualifiers if available.

        Note: Adjust column qualifiers based on what's available in your HBase table.
        """
        # Try to fetch multiple columns - adjust based on your actual table structure
        try:
            result = hbase_client.fetch_data(
                table_name=HBASE_TABLE_NAME,
                column_family=HBASE_COLUMN_FAMILY,
                column_qualifier=HBASE_COLUMN_QUALIFIERS,  # Add more qualifiers if available
                rowkeys=test_hex_ids[:10],
                timerange=(HBASE_TIMERANGE_START, HBASE_TIMERANGE_END),
            )

            assert isinstance(result, pl.DataFrame)
            print("\n✅ Multi-column fetch successful!")
            print(f"Columns returned: {result.columns}")
        except Exception as e:
            print(f"\n⚠️ Multi-column fetch failed: {str(e)}")
            pytest.skip(f"Multiple column qualifiers not available: {str(e)}")

    def test_client_repr(self, hbase_client):
        """Test that client repr shows obfuscated token."""
        client_str = repr(hbase_client)

        # Should contain fetch_url
        assert "fetch_url" in client_str
        # Token should be obfuscated
        assert "*" in client_str
        # Should not contain full token
        assert "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" not in client_str

        print("\n✅ Client repr test passed!")
        print(client_str)


class TestHBaseDataAnalysis:
    """Tests for analyzing fetched HBase data."""

    def test_data_statistics(self, hbase_client, test_hex_ids):
        """
        Fetch data and compute basic statistics.
        """
        result = hbase_client.fetch_data(
            table_name=HBASE_TABLE_NAME,
            column_family=HBASE_COLUMN_FAMILY,
            column_qualifier=HBASE_COLUMN_QUALIFIERS,
            rowkeys=test_hex_ids,
            timerange=(HBASE_TIMERANGE_START, HBASE_TIMERANGE_END),
        )

        if not result.is_empty():
            stats = result.select(pl.col(HBASE_COLUMN_QUALIFIERS[0]).cast(pl.Float64)).describe()

            print("\n✅ Data statistics:")
            print(stats)

            # Verify data types
            assert result["hex_id"].dtype == pl.String
            print(f"\nhex_id column type: {result['hex_id'].dtype}")
            print(f"{HBASE_COLUMN_QUALIFIERS[0]} column type: {result[HBASE_COLUMN_QUALIFIERS[0]].dtype}")

    def test_data_coverage(self, hbase_client, test_hex_ids):
        """
        Check what percentage of hex IDs have data in HBase.
        """
        result = hbase_client.fetch_data(
            table_name=HBASE_TABLE_NAME,
            column_family=HBASE_COLUMN_FAMILY,
            column_qualifier=HBASE_COLUMN_QUALIFIERS,
            rowkeys=test_hex_ids,
            timerange=(HBASE_TIMERANGE_START, HBASE_TIMERANGE_END),
        )

        # Calculate coverage: how many unique hex_ids have at least one row
        unique_hex_ids_with_data = result["hex_id"].n_unique()
        coverage = unique_hex_ids_with_data / len(test_hex_ids) * 100

        print("\n✅ Data coverage:")
        print(f"Total hex IDs queried: {len(test_hex_ids)}")
        print(f"Unique hex IDs with data: {unique_hex_ids_with_data}")
        print(f"Total rows (with time dimension): {len(result)}")
        print(f"Coverage: {coverage:.2f}% ({unique_hex_ids_with_data}/{len(test_hex_ids)})")

        # Show average rows per hex_id
        avg_rows_per_hex = len(result) / unique_hex_ids_with_data if unique_hex_ids_with_data > 0 else 0
        print(f"Avg rows per hex_id: {avg_rows_per_hex:.1f}")

        # Show sample of available data
        if not result.is_empty():
            print("\nSample data:")
            print(result.head(10))
