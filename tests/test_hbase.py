"""
Tests for HBaseClient async operations that work in both regular Python
scripts and Jupyter Notebook environments (with or without running event loop).
"""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from h3_toolkit.hbase import HBaseClient


@pytest.fixture
def hbase_client():
    """Create a test HBaseClient instance."""
    return HBaseClient(
        fetch_url="http://localhost:8080/fetch",
        send_url="http://localhost:8080/send",
        token="test-token-12345",
        max_concurrent_requests=2,
        chunk_size=100,
    )


class TestAsyncioRunDetection:
    """Test that fetch_data correctly detects running event loops."""

    @patch("asyncio.run")
    @patch("h3_toolkit.hbase.HBaseClient._fetch_data_main", new_callable=AsyncMock)
    def test_uses_asyncio_run_when_no_loop(self, mock_fetch, mock_run, hbase_client):
        """
        Test that fetch_data uses asyncio.run() when there's no running loop.

        This verifies the normal Python script path works correctly.
        """
        # Setup
        mock_run.side_effect = ValueError("No data fetched from HBase, please check the input parameters")

        # Execute
        with pytest.raises(ValueError, match="No data fetched"):
            hbase_client.fetch_data(
                table_name="test_table",
                column_family="cf",
                column_qualifier=["col1"],
                rowkeys=["row1"],
            )

        # Assert: asyncio.run was called (not ThreadPoolExecutor)
        assert mock_run.called

    def test_detects_running_loop_and_uses_thread_executor(self, hbase_client):
        """
        Test that fetch_data detects a running event loop and uses ThreadPoolExecutor.

        This simulates calling fetch_data from within a Jupyter-like environment
        with an already-running event loop.
        """
        call_info = {"used_thread_executor": False}

        # We'll patch concurrent.futures.ThreadPoolExecutor to verify it's used
        with patch("h3_toolkit.hbase.concurrent.futures.ThreadPoolExecutor") as mock_executor_class:
            with patch.object(hbase_client, "_fetch_data_main", new_callable=AsyncMock) as _:
                # Setup mock executor
                mock_executor = AsyncMock()
                mock_executor.__enter__ = AsyncMock(return_value=mock_executor)
                mock_executor.__exit__ = AsyncMock(return_value=None)

                # Mock submit to mark that we used the thread executor
                def submit_side_effect(*args, **kwargs):
                    call_info["used_thread_executor"] = True
                    # Return a mock future
                    mock_future = AsyncMock()
                    mock_future.result.return_value = ValueError("No data")
                    return mock_future

                mock_executor.submit = submit_side_effect
                mock_executor_class.return_value = mock_executor

                # Create and run within an event loop (simulating Jupyter)
                async def run_fetch_in_loop():
                    try:
                        hbase_client.fetch_data(
                            table_name="test_table",
                            column_family="cf",
                            column_qualifier=["col1"],
                            rowkeys=["row1"],
                        )
                    except (ValueError, Exception):
                        pass  # We expect it to fail, we just want to see if executor was used

                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(run_fetch_in_loop())
                    # Check if ThreadPoolExecutor was used
                    assert mock_executor_class.called, "ThreadPoolExecutor should be used in Jupyter environment"
                finally:
                    loop.close()
                    asyncio.set_event_loop(None)


class TestSendDataAsyncioHandling:
    """Test send_data async/sync handling."""

    @patch("asyncio.run")
    @patch("h3_toolkit.hbase.HBaseClient._send_data_main", new_callable=AsyncMock)
    def test_send_data_uses_asyncio_run_when_no_loop(self, mock_send, mock_run, hbase_client):
        """
        Test that send_data uses asyncio.run() when there's no running loop.
        """
        import polars as pl

        # Setup
        mock_run.return_value = None  # send_data doesn't return anything

        # Create test data
        data = pl.DataFrame(
            {
                "hex_id": ["row1", "row2"],
                "col1": [1, 2],
            }
        )

        # Execute
        hbase_client.send_data(
            data=data,
            table_name="test_table",
            column_family="cf",
            column_qualifier=["col1"],
            rowkey_col="hex_id",
            timestamp=None,
        )

        # Assert: asyncio.run was called
        assert mock_run.called

    def test_send_data_with_running_loop(self, hbase_client):
        """
        Test that send_data detects a running event loop and uses ThreadPoolExecutor.
        """
        import polars as pl

        with patch("h3_toolkit.hbase.concurrent.futures.ThreadPoolExecutor") as mock_executor_class:
            with patch.object(hbase_client, "_send_data_main", new_callable=AsyncMock):
                # Setup mock executor
                mock_executor = AsyncMock()
                mock_executor.__enter__ = AsyncMock(return_value=mock_executor)
                mock_executor.__exit__ = AsyncMock(return_value=None)

                mock_future = AsyncMock()
                mock_future.result.return_value = None
                mock_executor.submit = AsyncMock(return_value=mock_future)

                mock_executor_class.return_value = mock_executor

                # Create test data
                data = pl.DataFrame(
                    {
                        "hex_id": ["row1"],
                        "col1": [1],
                    }
                )

                # Run within event loop
                async def run_send_in_loop():
                    hbase_client.send_data(
                        data=data,
                        table_name="test_table",
                        column_family="cf",
                        column_qualifier=["col1"],
                        rowkey_col="hex_id",
                        timestamp=None,
                    )

                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(run_send_in_loop())
                    # Check if ThreadPoolExecutor was used
                    assert mock_executor_class.called, "ThreadPoolExecutor should be used in Jupyter environment"
                finally:
                    loop.close()
                    asyncio.set_event_loop(None)


class TestEventLoopDetectionBehavior:
    """Test the underlying asyncio event loop detection behavior."""

    def test_get_running_loop_raises_outside_async(self):
        """
        Verify that asyncio.get_running_loop() raises RuntimeError outside async context.
        """
        with pytest.raises(RuntimeError):
            asyncio.get_running_loop()

    def test_get_running_loop_succeeds_inside_async(self):
        """
        Verify that asyncio.get_running_loop() returns a loop inside async context.
        """
        result = []

        async def check_loop():
            loop = asyncio.get_running_loop()
            result.append(loop is not None)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(check_loop())
            assert result[0] is True
        finally:
            loop.close()
            asyncio.set_event_loop(None)
