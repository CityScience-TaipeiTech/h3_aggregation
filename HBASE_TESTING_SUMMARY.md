# HBase Integration Testing - Quick Start

## Overview

You have two ways to test your HBase integration:

1. **Pytest Integration Tests** (Comprehensive) - `tests/test_hbase_integration.py`
2. **Unit Tests** (No external dependencies) - `tests/test_hbase.py`

---

## Method 1: Pytest Integration Tests (✅ Comprehensive)

### Setup

#### Step 1: Get API Token
1. Register account at: http://10.100.1.64:2891/swagger/index.html
2. Get your API token from account settings

#### Step 2: Configure Environment
```bash
cd /Users/syuanbo/Documents/GitHub/H3-ToolKits
source .venv/bin/activate

# Set environment variables
export HBASE_FETCH_API="http://10.100.1.64:2891/api/hbase/v1/test/filterdata2"
export HBASE_TOKEN="your-token-from-swagger-page"
```

### Run All Tests
```bash
pytest tests/test_hbase_integration.py -v -s
```

### Run Specific Tests

**Quick connectivity test (fastest)**
```bash
pytest tests/test_hbase_integration.py::TestHBaseIntegration::test_fetch_data_small_subset -v -s
```

**Main integration test (all 24 hex IDs, 2020-2024)**
```bash
pytest tests/test_hbase_integration.py::TestHBaseIntegration::test_fetch_data_with_timerange -v -s
```

**Data analysis tests**
```bash
pytest tests/test_hbase_integration.py::TestHBaseDataAnalysis -v -s
```

**Data statistics (min, max, mean, etc.)**
```bash
pytest tests/test_hbase_integration.py::TestHBaseDataAnalysis::test_data_statistics -v -s
```

**Data coverage analysis**
```bash
pytest tests/test_hbase_integration.py::TestHBaseDataAnalysis::test_data_coverage -v -s
```

### Available Tests

| Test | Hex IDs | Time Filter | Duration | Purpose |
|------|---------|-------------|----------|---------|
| `test_fetch_data_small_subset` | 3 | 2020-2024 | <30s | Verify connectivity |
| `test_fetch_data_with_timerange` | 24 | 2020-2024 | 2-5m | Main integration test |
| `test_fetch_data_without_timerange` | 5 | None | 1-2m | Test all historical data |
| `test_fetch_multiple_column_qualifiers` | 10 | 2020-2024 | 1-2m | Multiple columns (if available) |
| `test_data_statistics` | 24 | 2020-2024 | 2-5m | Compute min/max/mean/median |
| `test_data_coverage` | 24 | 2020-2024 | 2-5m | % of hex IDs with data |

---

## Method 2: Unit Tests (No HBase Required)

Run without external dependencies:

```bash
# All unit tests (no network needed)
pytest tests/test_hbase.py -v

# Specific test
pytest tests/test_hbase.py::TestAsyncioRunDetection::test_uses_asyncio_run_when_no_loop -v
```

These tests verify:
- ✅ Event loop detection in Jupyter vs scripts
- ✅ ThreadPoolExecutor usage for async isolation
- ✅ Semaphore lazy initialization
- ✅ No event loop conflicts

---

## Test Data

**Source**: `tests/data/test_h3.json`

**24 Hex IDs** at resolution 12:
```
8c4ba0a412a01ff, 8c4ba0a412a05ff, 8c4ba0a412a07ff,
8c4ba0a412a09ff, 8c4ba0a412a0bff, 8c4ba0a412a0dff,
...
```

These cover a geographic region and are used for all HBase tests.

---

## Query Configuration Used in Tests

```
Table:           segis_population_statistic
Column Family:   segis_population_statistic
Column Qualifier: p_cnt (population count)
Time Range:      2020-01-01T00:00:00Z to 2024-12-31T23:59:59Z
Row Keys:        24 H3 indices from test data
```

---

## Troubleshooting

### ❌ "HBASE_FETCH_API and HBASE_TOKEN environment variables not set"
```bash
export HBASE_FETCH_API="http://10.100.1.64:2891/api/hbase/v1/test/filterdata2"
export HBASE_TOKEN="your-token"
```

### ❌ "Connection timeout" or "Connection refused"
- Check if HBase server is running at `10.100.1.64:2891`
- Check network connectivity:
  ```bash
  ping 10.100.1.64
  ```
- Try curl to test API:
  ```bash
  curl -H "Authorization: Bearer $HBASE_TOKEN" \
    "$HBASE_FETCH_API"
  ```

### ❌ "Invalid token" or "Unauthorized (401)"
- Get new token from: http://10.100.2.218:2891/swagger/index.html#/user/post_user_login
- Update `HBASE_TOKEN`

### ❌ "Table not found" or "No data returned"
- Verify table exists: `segis_population_statistic`
- Check column family: `segis_population_statistic`
- Verify data exists for time range: 2020-2024
- Try without timerange filter first

### ❌ Test timeout
- Increase timeout: `pytest --timeout=600 ...`
- Reduce data: Use smaller hex_ids subset
- Increase chunk_size in fixture

---

## Recommended Test Workflow

### First Time Setup (30 seconds - 10 minutes)
```bash
# 1. Quick connectivity check (30 seconds)
pytest tests/test_hbase_integration.py::TestHBaseIntegration::test_fetch_data_small_subset -v -s

# 2. If successful, run unit tests (no HBase needed)
pytest tests/test_hbase.py -v
```

### Ongoing Development (varies)
```bash
# Run quick smoke test (30 seconds)
pytest tests/test_hbase_integration.py::TestHBaseIntegration::test_fetch_data_small_subset -v -s

# Run full integration tests when making changes (5-10 minutes)
pytest tests/test_hbase_integration.py -v -s

# Check data quality and statistics
pytest tests/test_hbase_integration.py::TestHBaseDataAnalysis -v -s
```

### Before Committing
```bash
# Run all tests
pytest tests/test_hbase.py -v          # Unit tests
pytest tests/test_hbase_integration.py -v  # Integration tests (if HBase available)
```

---

## Files Created

- `tests/test_hbase_integration.py` - Comprehensive pytest integration tests
- `tests/INTEGRATION_TEST_GUIDE.md` - Detailed integration testing guide
- `HBASE_TESTING_SUMMARY.md` - This file

---

## Next Steps

1. **Run the quick test**:
   ```bash
   python test_hbase_quick.py
   ```

2. **Review the results** - Check if data is being fetched correctly

3. **Adjust tests** if needed:
   - Different column qualifiers
   - Different time ranges
   - Different hex IDs

4. **Add to CI/CD** (optional):
   - Mark tests as `@pytest.mark.integration`
   - Run unit tests in CI, skip integration tests
   - Run integration tests separately with HBase credentials

---

## Performance Optimization Tips

If queries are slow:

1. **Increase chunk size** (default 100):
   ```python
   HBaseClient(..., chunk_size=200)
   ```

2. **Increase concurrency** (default 5):
   ```python
   HBaseClient(..., max_concurrent_requests=10)
   ```

3. **Use shorter time range** (if possible):
   ```python
   timerange=('2023-01-01T00:00:00Z', '2024-12-31T23:59:59Z')
   ```

4. **Reduce hex IDs** (for testing):
   ```python
   rowkeys=hex_ids[:10]  # First 10 instead of all 24
   ```

---

## Support

For issues or questions:
- Check `INTEGRATION_TEST_GUIDE.md` for detailed troubleshooting
- Review test output with `-s` flag for detailed error messages
- Verify credentials and network connectivity first
