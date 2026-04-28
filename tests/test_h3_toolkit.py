from pathlib import Path

import geopandas as gpd
import polars as pl
import pytest
import rasterio

from h3_toolkit.aggregation import AggregationStrategy, Centroid, EqualSplit, Sum
from h3_toolkit.core import H3Toolkit
from h3_toolkit.utils import geom_to_wkb

DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture
def h3_toolkit():
    return H3Toolkit()


@pytest.fixture
def sample_gdf():
    return gpd.read_file(DATA_DIR / "test_geom.geojson")


def test_set_aggregation_strategy(h3_toolkit):
    # Verify that set_aggregation_strategy() stores the provided strategies on the instance
    # and that each value is recognized as an AggregationStrategy subclass.
    class MockAggregationStrategy(AggregationStrategy):
        def apply(self, data, target_cols):
            return data.with_columns(pl.col(target_cols).sum().alias(target_cols))

    strategies = {"col1": MockAggregationStrategy()}
    h3_toolkit.set_aggregation_strategy(strategies)

    assert "col1" in h3_toolkit.aggregation_strategies
    assert isinstance(h3_toolkit.aggregation_strategies["col1"], AggregationStrategy)


def test_process_from_vector_with_geodataframe(h3_toolkit, sample_gdf):
    # Verify that a GeoDataFrame is correctly converted to H3 cells at resolution 12
    # and that the aggregation strategy (SplitEqually) produces the expected output columns.
    result = h3_toolkit.set_aggregation_strategy({"p_cnt": EqualSplit(agg_col="codebase")}).process_from_vector(
        sample_gdf, resolution=12, geometry_col="geometry"
    )
    assert isinstance(result, H3Toolkit)
    output = result.get_result()
    assert not output.is_empty()
    assert "hex_id" in output.columns
    assert "p_cnt" in output.columns


def test_process_from_vector_with_polars_dataframe(h3_toolkit, sample_gdf):
    # Verify that process_from_vector() also accepts a Polars DataFrame whose geometry
    # column is already encoded as WKB bytes (the format produced by geom_to_wkb()).
    pdf = geom_to_wkb(sample_gdf, "geometry")
    result = h3_toolkit.set_aggregation_strategy({("p_cnt", "h_cnt"): Centroid()}).process_from_vector(
        pdf, resolution=12, geometry_col="geometry"
    )
    assert isinstance(result, H3Toolkit)
    output = result.get_result()
    assert not output.is_empty()
    assert "hex_id" in output.columns
    assert "p_cnt" in output.columns
    assert "h_cnt" in output.columns


def test_process_from_h3(h3_toolkit):
    # Verify that process_from_h3() re-aggregates resolution-12 cells up to resolution 10
    # using SumUp, and that the result contains the expected hex_id and value columns.
    df = pl.DataFrame(
        {
            "hex_id": [
                "8c4ba0a412a01ff",
                "8c4ba0a412a05ff",
                "8c4ba0a412a07ff",
                "8c4ba0a412a09ff",
                "8c4ba0a412a0bff",
            ],
            "value": [10.0, 20.0, 30.0, 40.0, 50.0],
        }
    )
    result = h3_toolkit.set_aggregation_strategy({"value": Sum()}).process_from_h3(
        df, target_resolution=10, source_resolution=12, h3_col="hex_id"
    )
    assert isinstance(result, H3Toolkit)
    output = result.get_result()
    assert not output.is_empty()
    assert "hex_id" in output.columns
    assert "value" in output.columns


@pytest.fixture
def raster_data():
    tif_path = DATA_DIR / "test_raster.tif"
    with rasterio.open(tif_path) as src:
        data = src.read(1)
        transform = src.transform
    return data, transform


def test_process_from_raster_basic(h3_toolkit, raster_data):
    data, transform = raster_data
    result = h3_toolkit.process_from_raster(data=data, transform=transform, resolution=9)
    assert isinstance(result, H3Toolkit)
    output = result.get_result()
    assert not output.is_empty()
    assert "hex_id" in output.columns
    assert "value" in output.columns


def test_process_from_raster_custom_value_name(h3_toolkit, raster_data):
    data, transform = raster_data
    output = h3_toolkit.process_from_raster(
        data=data, transform=transform, resolution=9, return_value_name="elevation"
    ).get_result()
    assert "elevation" in output.columns
    assert "value" not in output.columns


def test_process_from_raster_hex_id_format(h3_toolkit, raster_data):
    data, transform = raster_data
    output = h3_toolkit.process_from_raster(data=data, transform=transform, resolution=9).get_result()
    assert output["hex_id"].dtype == pl.String
    assert all(len(h) == 15 for h in output["hex_id"].to_list())


def test_process_from_raster_nodata_excluded(h3_toolkit, raster_data):
    data, transform = raster_data
    nodata_val = float(data[0, 0])
    output_with = h3_toolkit.process_from_raster(
        data=data, transform=transform, resolution=9, nodata_value=nodata_val
    ).get_result()
    output_without = H3Toolkit().process_from_raster(data=data, transform=transform, resolution=9).get_result()
    assert len(output_with) <= len(output_without)


def test_process_from_raster_invalid_resolution(h3_toolkit, raster_data):
    from h3_toolkit.exceptions import ResolutionRangeError

    data, transform = raster_data
    with pytest.raises(ResolutionRangeError):
        h3_toolkit.process_from_raster(data=data, transform=transform, resolution=99)
