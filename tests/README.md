# HBase Integration Testing Guide

Complete guide for testing HBase integration with h3-toolkit. Choose between integration tests with real HBase backend or unit tests without external dependencies.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Setup](#setup)
3. [Running Tests](#running-tests)
4. [Test Types](#test-types)
5. [Customization](#customization)
6. [Troubleshooting](#troubleshooting)
7. [Performance Tips](#performance-tips)

---

## Quick Start

### First Time (30 seconds to 10 minutes)

```bash
# Step 1: Get API token from http://10.100.1.64:2891/swagger/index.html
# Register account and copy your token

# Step 2: Set environment variables
export HBASE_FETCH_API="http://10.100.1.64:2891/api/hbase/v1/test/filterdata2"
export HBASE_TOKEN="your-token-from-swagger-page"

# Step 3: Activate virtual environment
source .venv/bin/activate

# Step 4: Quick connectivity check (30 seconds)
pytest test_hbase_integration.py::TestHBaseIntegration::test_fetch_data_small_subset -v -s

# Step 5: Run unit tests (no HBase needed)
pytest test_hbase.py -v
```

---

## Setup

### 1. Get HBase API Token

To use HBase integration tests, you need an API token:

1. Go to: **http://10.100.1.64:2891/swagger/index.html**
2. Register a new account
3. Get your API token from account settings
4. Keep it safe (don't commit to version control)

### 2. Set Environment Variables

**Option A: Export directly**
```bash
export HBASE_FETCH_API="http://10.100.1.64:2891/api/hbase/v1/test/filterdata2"
export HBASE_TOKEN="your-token-here"
```

**Option B: Use .env file (recommended)**
```bash
# Create .env file (add to .gitignore)
cat > .env << EOF
export HBASE_FETCH_API="http://10.100.1.64:2891/api/hbase/v1/test/filterdata2"
export HBASE_TOKEN="your-token-here"
EOF

# Load before testing
source .env
```

### 3. Activate Virtual Environment

```bash
source .venv/bin/activate
```

---

## Running Tests

### Test Selection Guide

| Scenario | Command | Duration | Use Case |
|----------|---------|----------|----------|
| **Quick check** | `pytest test_hbase_integration.py::TestHBaseIntegration::test_fetch_data_small_subset -v -s` | <30s | Verify connectivity works |
| **Full integration** | `pytest test_hbase_integration.py -v -s` | 5-10m | Main testing workflow |
| **Data analysis** | `pytest test_hbase_integration.py::TestHBaseDataAnalysis -v -s` | 5-10m | Check data quality |
| **Unit tests only** | `pytest test_hbase.py -v` | <1m | No HBase needed |

### Specific Test Commands

**Quick connectivity test (fastest)**
```bash
pytest test_hbase_integration.py::TestHBaseIntegration::test_fetch_data_small_subset -v -s
```

**Main integration test (all 937 hex IDs, 2020-2024 timerange)**
```bash
pytest test_hbase_integration.py::TestHBaseIntegration::test_fetch_data_with_timerange -v -s
```

**Without timerange filter (all historical data)**
```bash
pytest test_hbase_integration.py::TestHBaseIntegration::test_fetch_data_without_timerange -v -s
```

**Data statistics analysis**
```bash
pytest test_hbase_integration.py::TestHBaseDataAnalysis::test_data_statistics -v -s
```

**Data coverage analysis**
```bash
pytest test_hbase_integration.py::TestHBaseDataAnalysis::test_data_coverage -v -s
```

**All integration tests**
```bash
pytest test_hbase_integration.py -v -s
```

**With coverage report**
```bash
pytest test_hbase_integration.py -v -s --cov=h3_toolkit
```

---

## Test Types

### Integration Tests (with HBase)

Located in: `test_hbase_integration.py`

**Core Tests:**

| Test | Hex IDs | Filters | Duration | Purpose |
|------|---------|---------|----------|---------|
| `test_fetch_data_small_subset` | 3 | 2020-2024 | <30s | Verify connectivity |
| `test_fetch_data_with_timerange` | 937 | 2020-2024 | 5-10m | Main integration test |
| `test_fetch_data_without_timerange` | 5 | None | 1-2m | All historical data |
| `test_fetch_multiple_column_qualifiers` | 10 | 2020-2024 | 1-2m | Multiple columns |
| `test_client_repr` | N/A | N/A | <1s | Token obfuscation |

**Data Analysis Tests:**

| Test | Purpose |
|------|---------|
| `test_data_statistics` | Compute min/max/mean/median for p_cnt column |
| `test_data_coverage` | Show % of hex IDs with data in HBase |

### Unit Tests (without HBase)

Located in: `test_hbase.py`

```bash
pytest test_hbase.py -v
```

These verify:
- ✅ Event loop detection in Jupyter vs scripts
- ✅ ThreadPoolExecutor usage for async isolation
- ✅ Semaphore lazy initialization
- ✅ No event loop conflicts

---

## Customization

### Modify Query Parameters

All HBase query configuration is in `test_hbase_integration.py` at the top:

```python
# ==================== HBase Query Configuration ====================
HBASE_TABLE_NAME = 'res_10_time_data'
HBASE_COLUMN_FAMILY = 'segis_population_statistic'
HBASE_COLUMN_QUALIFIERS = ['p_cnt']
HBASE_TIMERANGE_START = '2020-01-01T00:00:00Z'
HBASE_TIMERANGE_END = '2024-12-31T23:59:59Z'
# ===================================================================
```

**Change any of these and all tests will use the new configuration.**

### Examples

**Query different time range:**
```python
HBASE_TIMERANGE_START = '2023-01-01T00:00:00Z'
HBASE_TIMERANGE_END = '2023-12-31T23:59:59Z'
```

**Query different columns:**
```python
HBASE_COLUMN_QUALIFIERS = ['h_cnt', 'f_cnt']  # if these columns exist
```

**Change row keys source:**
Edit the fixture in `test_hbase_integration.py`:
```python
@pytest.fixture
def test_hex_ids():
    """Load hex IDs from test data CSV file."""
    test_data_path = Path(__file__).parent / 'data' / 'test_resolution_10.csv'
    df = pl.read_csv(test_data_path)
    return df['hex_id'].to_list()
```

**Increase concurrency:**
Edit the fixture:
```python
@pytest.fixture
def hbase_client(hbase_config):
    return HBaseClient(
        fetch_url=hbase_config['fetch_url'],
        send_url=hbase_config['send_url'] or hbase_config['fetch_url'],
        token=hbase_config['token'],
        max_concurrent_requests=10,  # Increase for faster queries
        chunk_size=200  # Increase chunk size
    )
```

---

## Troubleshooting

### ❌ "HBASE_FETCH_API and HBASE_TOKEN environment variables not set"

**Solution**: Export the environment variables before running tests
```bash
export HBASE_FETCH_API="http://10.100.1.64:2891/api/hbase/v1/test/filterdata2"
export HBASE_TOKEN="your-token-here"
pytest test_hbase_integration.py -v
```

Or use `.env` file:
```bash
source .env
pytest test_hbase_integration.py -v
```

### ❌ "Connection timeout" or "Connection refused"

**Possible causes**:
- HBase server is down or unreachable
- Firewall blocking connection to 10.100.1.64:2891
- Network connectivity issues

**Debug steps**:
```bash
# Test connectivity from command line
ping 10.100.1.64

# Test API with curl
curl -H "Authorization: Bearer $HBASE_TOKEN" \
  "$HBASE_FETCH_API"
```

### ❌ "Invalid token" or "Unauthorized (401)"

**Solution**: Token may have expired. Get a new one:

1. Go to: http://10.100.1.64:2891/swagger/index.html
2. Sign in with your account credentials
3. Navigate to account settings to get a new API token
4. Update your `HBASE_TOKEN` environment variable

### ❌ "Table not found" or "No data returned"

**Possible causes**:
- Table name is incorrect (check exact capitalization)
- Column family name is wrong
- Data doesn't exist in time range

**Debug**:
- Verify table: `res_10_time_data`
- Verify column family: `segis_population_statistic`
- Try without timerange filter first
- Check data coverage with: `pytest test_hbase_integration.py::TestHBaseDataAnalysis::test_data_coverage -v -s`

### ❌ Tests timeout (>10 minutes)

**Possible causes**:
- Network is slow
- HBase server is overloaded
- Trying to fetch too much data at once

**Solutions**:
```bash
# Increase pytest timeout
pytest --timeout=600 test_hbase_integration.py -v

# Reduce chunk size in fixture (smaller batches)
# Edit: max_concurrent_requests=3, chunk_size=50

# Use smaller hex_ids subset
# Edit test to use: hex_ids[:100] instead of all 937
```

---

## Recommended Test Workflow

### First Time Setup
```bash
# 1. Register and get token
# Go to http://10.100.1.64:2891/swagger/index.html

# 2. Set credentials
export HBASE_FETCH_API="http://10.100.1.64:2891/api/hbase/v1/test/filterdata2"
export HBASE_TOKEN="your-token"
source .venv/bin/activate

# 3. Quick connectivity check
pytest test_hbase_integration.py::TestHBaseIntegration::test_fetch_data_small_subset -v -s

# 4. If successful, run unit tests
pytest test_hbase.py -v
```

### Ongoing Development
```bash
# Run quick smoke test (30 seconds)
pytest test_hbase_integration.py::TestHBaseIntegration::test_fetch_data_small_subset -v -s

# Run full integration tests when making changes
pytest test_hbase_integration.py -v -s

# Check data quality
pytest test_hbase_integration.py::TestHBaseDataAnalysis -v -s
```

### Before Committing
```bash
# Run all unit tests
pytest test_hbase.py -v

# Run integration tests if HBase available
pytest test_hbase_integration.py -v -s
```

---

## Performance Tips

If queries are slow:

1. **Start with small subset**: Use `test_fetch_data_small_subset` first
2. **Increase chunk_size** (default 100):
   ```python
   HBaseClient(..., chunk_size=200)
   ```
3. **Increase max_concurrent_requests** (default 5):
   ```python
   HBaseClient(..., max_concurrent_requests=10)
   ```
4. **Use shorter time range** (if possible):
   - Default: 2020-2024 (4 years)
   - Faster: 2023-2024 (1 year)
5. **Reduce hex IDs** (for testing):
   ```python
   rowkeys=hex_ids[:100]  # First 100 instead of all 937
   ```

---

## Test Data

**Source**: `data/test_resolution_10.csv`

**937 Hex IDs** at H3 resolution 10:
```
8a4ba0a4e15ffff
8a4ba0a4385ffff
8a4ba0a4339ffff
...
```

These cover a geographic region and are used for all HBase tests.

---

## Query Configuration Reference

Current test configuration:

| Parameter | Value |
|-----------|-------|
| **Table** | `res_10_time_data` |
| **Column Family** | `segis_population_statistic` |
| **Column Qualifier** | `p_cnt` (population count) |
| **Time Range** | 2020-01-01 to 2024-12-31 |
| **Row Keys** | 937 H3 indices (resolution 10) |

---

## CI/CD Integration

To skip integration tests in CI (since they require external HBase):

```bash
# Unit tests only
pytest test_hbase.py -v

# Integration tests only
pytest test_hbase_integration.py -v
```

Or mark tests:

```bash
# Add pytest.ini markers
[pytest]
markers =
    integration: marks tests as integration tests
```

Decorate tests:
```python
@pytest.mark.integration
def test_fetch_data_with_timerange(...):
    ...
```

Run without integration tests:
```bash
pytest -m "not integration"
```

---

## Support & Debugging

For issues or questions:

1. **Check environment**: `echo $HBASE_TOKEN` should not be empty
2. **Verify connectivity**: `ping 10.100.1.64`
3. **Test credentials**: Try curl command from troubleshooting section
4. **Review test output**: Run with `-v -s` flags for detailed messages
5. **Check data coverage**: Run `test_data_coverage` to see if data exists
6. **Review HBase logs**: Check HBase server status at http://10.100.1.64:2891

---

## Files in this Directory

- `test_hbase.py` - Unit tests (no external dependencies)
- `test_hbase_integration.py` - Integration tests (requires HBase)
- `data/` - Test data files
  - `test_resolution_10.csv` - 937 hex IDs for integration tests
  - `test_geom.geojson` - Geometry test data
  - `test_raster.tif` - Raster test data
- `README.md` - This file

---

**Last updated**: 2026-04-13
