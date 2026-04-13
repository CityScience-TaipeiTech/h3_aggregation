# HBase Integration Testing Guide

This guide explains how to run integration tests against a real HBase backend using pytest.

## Setup

### 1. Get HBase API Token

First, you need to register an account and get an API token:

1. Go to: http://10.100.1.64:2891/swagger/index.html
2. Register a new account
3. Get your API token from the account page
4. Save it for the next step

### 2. Set Environment Variables

Export your HBase credentials:

```bash
export HBASE_FETCH_API="http://10.100.1.64:2891/api/hbase/v1/test/filterdata2"
export HBASE_TOKEN="xxx"
```

**Security Note**: Never commit credentials to version control. Use a `.env` file locally:

```bash
# .env (add to .gitignore)
export HBASE_FETCH_API="http://10.100.1.64:2891/api/hbase/v1/test/filterdata2"
export HBASE_TOKEN="your-token-here"
```

Then load it before testing:
```bash
source .env
pytest tests/test_hbase_integration.py -v
```

### 3. Activate Virtual Environment

```bash
source .venv/bin/activate
```

## Running Tests

### Quick Connectivity Check (Fastest)
```bash
# Test with just 3 hex IDs to verify connection works
pytest tests/test_hbase_integration.py::TestHBaseIntegration::test_fetch_data_small_subset -v -s
```

### Single Full Query (2-5 minutes)
```bash
# Fetch data for all 24 hex IDs with 2020-2024 timerange
pytest tests/test_hbase_integration.py::TestHBaseIntegration::test_fetch_data_with_timerange -v -s
```

### All Integration Tests
```bash
# Run all tests
pytest tests/test_hbase_integration.py -v -s

# Or with coverage report
pytest tests/test_hbase_integration.py -v -s --cov=h3_toolkit
```

### Data Analysis Tests
```bash
# Run data analysis tests (statistics, coverage, etc.)
pytest tests/test_hbase_integration.py::TestHBaseDataAnalysis -v -s
```

### Run Without Timerange Filter
```bash
# Fetch all available historical data (may be slower)
pytest tests/test_hbase_integration.py::TestHBaseIntegration::test_fetch_data_without_timerange -v -s
```

## Test Descriptions

### Core Integration Tests

#### `test_fetch_data_with_timerange`
- **Purpose**: Main integration test
- **Queries**: All 24 hex IDs from test data
- **Filters**: Population data (p_cnt) from 2020-01-01 to 2024-12-31
- **Expected Output**: DataFrame with hex_id and p_cnt columns
- **Duration**: 2-5 minutes depending on network

#### `test_fetch_data_without_timerange`
- **Purpose**: Verify functionality without time filters
- **Queries**: First 5 hex IDs
- **Filters**: None (fetches all available data)
- **Expected Output**: Valid DataFrame

#### `test_fetch_data_small_subset`
- **Purpose**: Quick connectivity verification
- **Queries**: First 3 hex IDs only
- **Filters**: 2020-2024 timerange
- **Duration**: <30 seconds
- **Use Case**: Verify HBase is accessible before running full test

#### `test_fetch_multiple_column_qualifiers`
- **Purpose**: Test fetching multiple columns simultaneously
- **Note**: Adjust column qualifiers in test based on your table structure
- **Skips**: If columns don't exist in table

### Data Analysis Tests

#### `test_data_statistics`
- Computes statistics: min, max, mean, median for p_cnt column
- Displays data type information
- Useful for validating data quality

#### `test_data_coverage`
- Shows what percentage of hex IDs have data in HBase
- Reports total queried vs. returned rows
- Displays sample data for inspection

## Expected Output Format

Successful test output looks like:

```
test_fetch_data_with_timerange PASSED
✅ Fetch successful!
Rows returned: 24
Columns: ['hex_id', 'p_cnt']

First 5 rows:
shape: (5, 2)
┌──────────────────┬────────┐
│ hex_id           ┆ p_cnt  │
│ ---              ┆ ---    │
│ str              ┆ f64    │
╞══════════════════╪════════╡
│ 8c4ba0a412a01ff  ┆ 1234.5 │
│ 8c4ba0a412a05ff  ┆ 5678.9 │
│ ...              ┆ ...    │
└──────────────────┴────────┘
```

## Troubleshooting

### 1. "HBASE_FETCH_API and HBASE_TOKEN environment variables not set"
**Solution**: Export the environment variables before running tests
```bash
export HBASE_FETCH_API="http://..."
export HBASE_TOKEN="..."
pytest tests/test_hbase_integration.py -v
```

### 2. "Connection timeout" or "Connection refused"
**Possible causes**:
- HBase server is down or unreachable
- Firewall blocking connection to 10.100.1.64:2891
- Network connectivity issues

**Debug**:
```bash
# Test connectivity from command line
curl -H "Authorization: Bearer $HBASE_TOKEN" \
  "$HBASE_FETCH_API" \
  -d "tablename=segis_population_statistic&rowkey=[\"8c4ba0a412a01ff\"]&column_qualifiers={\"segis_population_statistic\":[\"p_cnt\"]}"
```

### 3. "Invalid token" or "Unauthorized"
**Solution**: Token may have expired. Get a new token from:

1. Go to: http://10.100.1.64:2891/swagger/index.html
2. Sign in with your account credentials
3. Navigate to account settings to get a new API token
4. Update your `HBASE_TOKEN` environment variable

### 4. "Table not found" or "Column family not found"
**Possible causes**:
- Table name is incorrect (check exact capitalization)
- Column family name is wrong
- Data doesn't exist in time range

**Debug**:
- Verify table: `segis_population_statistic`
- Verify cf: `segis_population_statistic`
- Try without timerange filter first

### 5. Tests timeout (>10 minutes)
**Possible causes**:
- Network is slow
- HBase server is overloaded
- Trying to fetch too much data at once

**Solution**:
- Increase pytest timeout: `pytest --timeout=600 ...`
- Reduce chunk size: Modify `chunk_size=50` in fixture
- Use smaller hex_ids subset

## Customization (Update Global Variables)

All query parameters are defined as global constants at the top of `test_hbase_integration.py`:

```python
HBASE_TABLE_NAME = 'res_10_time_data'
HBASE_COLUMN_FAMILY = 'segis_population_statistic'
HBASE_COLUMN_QUALIFIERS = ['p_cnt']
HBASE_TIMERANGE_START = '2020-01-01T00:00:00Z'
HBASE_TIMERANGE_END = '2024-12-31T23:59:59Z'
```

Modify these variables to change test behavior across all tests.

### Adjust Timerange
Edit the global constants at top of test file:
```python
HBASE_TIMERANGE_START = '2021-01-01T00:00:00Z'
HBASE_TIMERANGE_END = '2023-12-31T23:59:59Z'
```

### Query Different Columns
Modify the global constant:
```python
HBASE_COLUMN_QUALIFIERS = ['h_cnt', 'f_cnt']  # if these columns exist
```

### Use Different Hex IDs
Edit the fixture in the test file or modify the CSV file path. The fixture currently loads from:
```python
test_data_path = Path(__file__).parent / 'data' / 'test_resolution_10.csv'
```

### Increase Concurrency
Modify fixture's max_concurrent_requests:
```python
@pytest.fixture
def hbase_client(hbase_config):
    return HBaseClient(
        fetch_url=hbase_config['fetch_url'],
        send_url=hbase_config['send_url'],
        token=hbase_config['token'],
        max_concurrent_requests=10,  # Increase for faster queries
        chunk_size=200  # Increase chunk size
    )
```

## Performance Tips

1. **Start with small subset**: Use `test_fetch_data_small_subset` first
2. **Increase chunk_size**: Larger chunks = fewer requests but more data per request
3. **Increase max_concurrent_requests**: More parallel requests (be careful not to overload)
4. **Use timerange filter**: Limits data scope, faster queries

## CI/CD Integration

To skip integration tests in CI (since they require external HBase):
```bash
pytest tests/test_hbase.py -v  # Unit tests only
pytest tests/test_hbase_integration.py -v  # Integration tests only
```

Or mark them in pytest.ini:
```ini
[pytest]
markers =
    integration: marks tests as integration tests (deselect with '-m "not integration"')
```

Then decorate tests:
```python
@pytest.mark.integration
def test_fetch_data_with_timerange(...):
    ...
```

Run without integration tests:
```bash
pytest -m "not integration"
```
