import geopandas as gpd
import polars as pl
import pytest

from h3_toolkit.aggregation import AggregationStrategy
from h3_toolkit.h3_toolkit import H3Toolkit


@pytest.fixture
def h3_toolkit():
    return H3Toolkit()

# @pytest.fixture
# def mock_hbase_client():
#     class MockHBaseClient:
#         def fetch_data(self, **kwargs):
#             return pl.DataFrame({"hex_id": ["abc123"], "data": [100]})
#         def send_data(self, **kwargs):
#             return True

#     return MockHBaseClient()

def test_set_aggregation_strategy(h3_toolkit):

    # Difine a mocl aggregation strategy
    class MockAggregationStrategy(AggregationStrategy):
        def apply(self, data, target_cols):
            return (
                data
                .with_columns(
                    pl.col(target_cols).sum().alias(target_cols)
                )
            )

    strategies = {'col1': MockAggregationStrategy()}
    h3_toolkit.set_aggregation_strategy(strategies)

    assert "col1" in h3_toolkit.aggregation_strategies
    assert isinstance(h3_toolkit.aggregation_strategies['col1'], AggregationStrategy)

def test_process_from_geometry_with_geodataframe(h3_toolkit):
    # read a mock GeoDataFrame
    gdf = gpd.read_file('data/test_geom.geojson')
    result = h3_toolkit.process_from_geometry(gdf, resolution=12, geometry_col='geometry')
    print("before_parsing", result.get_result())
    assert isinstance(result, H3Toolkit)
    assert result.get_result() is not None

def test_process_from_geometry_with_polars_dataframe(h3_toolkit):
    pass

def test_process_from_h3(h3_toolkit):
    df = pl.read_json('data/test_result.json')
    result = h3_toolkit.process_from_h3(df, resolution=8, h3_col='hex_id')
    print("after_parsing", result.get_result())
    assert isinstance(result, H3Toolkit)
    assert result.get_result() is not None
