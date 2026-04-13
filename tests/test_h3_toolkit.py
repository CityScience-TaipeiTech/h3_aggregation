from pathlib import Path

import geopandas as gpd
import polars as pl
import pytest

from h3_toolkit.aggregation import AggregationStrategy, Centroid, EqualSplit, Sum, SumUp
from h3_toolkit.core import H3Toolkit
from h3_toolkit.utils import geom_to_wkb

DATA_DIR = Path(__file__).parent / 'data'


@pytest.fixture
def h3_toolkit():
    return H3Toolkit()


@pytest.fixture
def sample_gdf():
    return gpd.read_file(DATA_DIR / 'test_geom.geojson')


def test_set_aggregation_strategy(h3_toolkit):
    # Verify that set_aggregation_strategy() stores the provided strategies on the instance
    # and that each value is recognized as an AggregationStrategy subclass.
    class MockAggregationStrategy(AggregationStrategy):
        def apply(self, data, target_cols):
            return data.with_columns(pl.col(target_cols).sum().alias(target_cols))

    strategies = {'col1': MockAggregationStrategy()}
    h3_toolkit.set_aggregation_strategy(strategies)

    assert 'col1' in h3_toolkit.aggregation_strategies
    assert isinstance(h3_toolkit.aggregation_strategies['col1'], AggregationStrategy)


def test_process_from_vector_with_geodataframe(h3_toolkit, sample_gdf):
    # Verify that a GeoDataFrame is correctly converted to H3 cells at resolution 12
    # and that the aggregation strategy (SplitEqually) produces the expected output columns.
    result = (
        h3_toolkit
        .set_aggregation_strategy({'p_cnt': EqualSplit(agg_col='codebase')})
        .process_from_vector(sample_gdf, resolution=12, geometry_col='geometry')
    )
    assert isinstance(result, H3Toolkit)
    output = result.get_result()
    assert not output.is_empty()
    assert 'hex_id' in output.columns
    assert 'p_cnt' in output.columns


def test_process_from_vector_with_polars_dataframe(h3_toolkit, sample_gdf):
    # Verify that process_from_vector() also accepts a Polars DataFrame whose geometry
    # column is already encoded as WKB bytes (the format produced by geom_to_wkb()).
    pdf = geom_to_wkb(sample_gdf, 'geometry')
    result = (
        h3_toolkit
        .set_aggregation_strategy({('p_cnt', 'h_cnt'): Centroid()})
        .process_from_vector(pdf, resolution=12, geometry_col='geometry')
    )
    assert isinstance(result, H3Toolkit)
    output = result.get_result()
    assert not output.is_empty()
    assert 'hex_id' in output.columns
    assert 'p_cnt' in output.columns
    assert 'h_cnt' in output.columns


def test_process_from_h3(h3_toolkit):
    # Verify that process_from_h3() re-aggregates resolution-12 cells up to resolution 10
    # using SumUp, and that the result contains the expected hex_id and value columns.
    df = pl.DataFrame({
        'hex_id': [
            '8c4ba0a412a01ff', '8c4ba0a412a05ff', '8c4ba0a412a07ff',
            '8c4ba0a412a09ff', '8c4ba0a412a0bff',
        ],
        'value': [10.0, 20.0, 30.0, 40.0, 50.0],
    })
    result = (
        h3_toolkit
        .set_aggregation_strategy({'value': Sum()})
        .process_from_h3(df, target_resolution=10, source_resolution=12, h3_col='hex_id')
    )
    assert isinstance(result, H3Toolkit)
    output = result.get_result()
    assert not output.is_empty()
    assert 'hex_id' in output.columns
    assert 'value' in output.columns
