## Project Overview

H3-ToolKits is a Python library for aggregating and visualizing geospatial data using H3 hexagonal grids. It converts vector geometries and rasters into H3 cells and applies configurable aggregation strategies.

## Architecture

```
h3_toolkit/
├── core.py          # H3Toolkit — main entry point, fluent API
├── aggregation.py   # AggregationStrategy subclasses (EqualSplit, Centroid, Sum, Mean, Count)
├── utils.py         # geom_to_wkb, wkb_to_cells, cell_to_geom helpers
├── exceptions.py    # Custom exceptions (ResolutionRangeError, ColumnNotFoundError, ...)
├── visualization.py # Visualization utilities (pydeck-based)
└── hbase.py         # HBase client (optional, requires HBase connection)
```

**Data flow:**
1. Input: `GeoDataFrame` or `polars.DataFrame` (WKB geometry) or NumPy raster array
2. `H3Toolkit.process_from_vector()` / `process_from_raster()` → converts to H3 cell IDs
3. `set_aggregation_strategy()` → applies column-level aggregation
4. `get_result()` → returns `polars.DataFrame` with `hex_id` column

**Key design decisions:**
- `h3ronpy.raster.raster_to_dataframe` returns `pyarrow.Table` (not polars), convert with `pl.from_arrow()`
- `.h3` polars plugin UDFs are eager — polars cannot optimize them via lazy query planner, so lazy mode gives no benefit here

## Development Setup

```bash
# Install all dependencies (including dev)
poetry install --with dev

# Install pre-commit hooks (required — runs ruff lint + format on every commit)
poetry run pre-commit install

# Run tests
poetry run pytest tests/ -v --ignore=tests/test_hbase.py --ignore=tests/test_hbase_integration.py
```

## Running Tests

Test data lives in `tests/data/`:
- `test_geom.geojson` — polygon GeoDataFrame for vector tests
- `test_raster.tif` — GeoTIFF for raster tests
- `test_resolution_10.csv` — pre-computed H3 cells at resolution 10

HBase tests (`test_hbase.py`, `test_hbase_integration.py`) require a live HBase connection — skip them locally unless you have one.

## Pre-commit Hooks

ruff runs automatically on `git commit`. If it auto-fixes files, the commit is blocked. Re-stage the fixed files and commit again:

```bash
git add <fixed-files>
git commit -m "your message"
```

## Adding a New Aggregation Strategy

1. Subclass `AggregationStrategy` in `aggregation.py`
2. Implement `apply(self, data: pl.LazyFrame, target_cols: list[str]) -> pl.LazyFrame`
3. The `data` LazyFrame always has a `cell` column (uint64 H3 cell IDs) plus the input columns
4. Return a LazyFrame that keeps `cell` and the aggregated `target_cols`
5. Export from `h3_toolkit/__init__.py`
6. Add a test in `tests/test_aggregation.py`

## Custom Exceptions

| Exception | When raised |
|-----------|-------------|
| `ResolutionRangeError` | H3 resolution not in 0–15 |
| `ColumnNotFoundError` | Required column missing from input DataFrame |
| `InputDataTypeError` | Input is not GeoDataFrame or polars DataFrame |
| `HBaseConnectionError` | HBase connection failure |

## graphify (AI assistant context)

This project has a graphify knowledge graph at `graphify-out/`.

- Before answering architecture or codebase questions, read `graphify-out/GRAPH_REPORT.md` for god nodes and community structure
- If `graphify-out/wiki/index.md` exists, navigate it instead of reading raw files
- After modifying code files in this session, run `python3 -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"` to keep the graph current
