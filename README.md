[![pypi](https://img.shields.io/pypi/v/h3_toolkit.svg)](https://pypi.python.org/pypi/h3_toolkit/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

# h3-toolkit

A Polars-native toolkit for aggregating and visualizing geospatial data using [Uber's H3 spatial indexing system](https://h3geo.org/).

**Documentation:** [https://h3-toolkit.readthedocs.io/](https://h3-toolkit.readthedocs.io/)

## Installation

```bash
pip install h3-toolkit

# With visualization support (pydeck, mapclassify)
pip install h3-toolkit[vis]
```

## Quick Example

```python
import geopandas as gpd
from h3_toolkit import H3Toolkit
from h3_toolkit.aggregation import Mean, Sum

# Load your vector data (e.g., census blocks, building footprints)
gdf = gpd.read_file("your_data.geojson")

result = (
    H3Toolkit()
    .process_from_vector(gdf, resolution=9)
    .set_aggregation_strategy({
        "population": Sum(),
        "income":     Mean(),
    })
    .get_result()
)

print(result)
# shape: (n, 3)
# ┌─────────────────┬────────────┬────────┐
# │ cell            ┆ population ┆ income │
# │ ---             ┆ ---        ┆ ---    │
# │ u64             ┆ f64        ┆ f64    │
# ╞═════════════════╪════════════╪════════╡
# │ 613194865823…   ┆ 1204.0     ┆ 52300. │
# └─────────────────┴────────────┴────────┘
```

## Features

- **Polars-native** — all aggregations run on Polars, no pandas overhead
- **Chainable API** — compose `process_from_vector`, `set_aggregation_strategy`, and `get_result` in a single pipeline
- **Multiple input formats** — vector (GeoDataFrame), raster, or existing H3 cells
- **Pluggable aggregation strategies** — `Sum`, `Mean`, `Count`, `EqualSplit`, `Centroid`, and more
- **Optional visualization** — built-in pydeck-based map rendering via the `vis` extra

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request.

## License

MIT © [City Science Lab @ TaipeiTech](https://github.com/CityScience-TaipeiTech), Syuan-Bo Huang
