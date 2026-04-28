import logging

import geopandas as gpd
import polars as pl
import pyarrow as pa
from h3ronpy import ContainmentMode as Cont
from h3ronpy.vector import wkb_to_cells as _h3_wkb_to_cells
from shapely import to_wkb


def geom_to_wkb(df: gpd.GeoDataFrame, geometry: str) -> pl.DataFrame:
    """
    convert GeoDataFrame to polars.DataFrame
    (geometry to wkb)
    """
    if df.crs != "epsg:4326":
        raise ValueError("The input GeoDataFrame CRS must be in EPSG:4326")

    if geometry not in df.columns:
        raise ValueError(f"Column '{geometry}' not found in the input GeoDataFrame")

    # 確保input跟output的geometry的column name不會改變，同時從geometry type 轉換成 wkb
    df = (
        df.rename(columns={geometry: "ready_to_convert"})
        .assign(geometry_wkb=lambda df: to_wkb(df["ready_to_convert"]))
        .drop("ready_to_convert", axis=1)  # drop geometry column (convert geodataframe to dataframe)
        .rename(columns={"geometry_wkb": geometry})
    )

    return (
        # pandas to polars
        pl.from_pandas(df)
    )


def wkb_to_cells(
    df: pl.DataFrame,
    resolution: int,
    geom_col: str = None,
    #  selected_cols:list=[],
    mode: Cont = Cont.ContainsCentroid,
) -> pl.DataFrame:
    """
    convert geometry to h3 cells
    df: polars.DataFrame, the input dataframe
    source_r: int, the resolution of the source geometry
    selected_cols: list, the columns to be selected
    """
    # 不需要對geometry進行處裡
    if geom_col is None:
        return df

    if geom_col not in df.collect_schema().names():
        raise ValueError(f"Column '{geom_col}' not found in the input DataFrame, \
                         please use `set_geometry()` to set the geometry column first")

    collected = df.collect() if hasattr(df, "collect") else df
    wkb_pyarrow = collected[geom_col].to_arrow().cast(pa.large_binary())
    cells_arro3 = _h3_wkb_to_cells(
        wkb_pyarrow, resolution=resolution, containment_mode=mode, compact=False, flatten=False
    )
    cells_pa = pa.chunked_array([pa.array(cells_arro3.to_pylist(), type=pa.list_(pa.uint64()))])
    return collected.with_columns(pl.from_arrow(cells_pa).alias("cell")).explode("cell").lazy()


def cell_to_geom(df: pl.DataFrame) -> gpd.GeoDataFrame:
    """
    convert h3 cells to geometry
    """
    import h3ronpy.polars  # noqa: F401
    from h3ronpy.vector import cells_to_wkb_polygons as _cells_to_wkb
    from shapely import from_wkb as _from_wkb

    cells = df["hex_id"].h3.cells_parse()
    wkb_arr = _cells_to_wkb(pa.chunked_array([pa.array(cells.to_list(), type=pa.uint64())]))
    geoms = _from_wkb([bytes(b) if b is not None else None for b in wkb_arr.to_pylist()])
    return gpd.GeoDataFrame(df.to_pandas(), geometry=list(geoms), crs="epsg:4326")


def setup_default_logger(logger_name: str, level=logging.WARNING):
    """
    Sets up a default logger with the given name and log level.

    Args:
        logger_name (str): The name of the logger.
        level (int): The logging level (e.g., logging.INFO, logging.WARNING).
    """
    logger = logging.getLogger(logger_name)
    if not logger.hasHandlers():  # Prevent multiple handlers
        handler = logging.StreamHandler()  # Outputs to console
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(name)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(level)
    return logger
