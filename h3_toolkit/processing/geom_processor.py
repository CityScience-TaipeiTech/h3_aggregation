import polars as pl
import geopandas as gpd
from shapely import to_wkb
from h3ronpy import ContainmentMode as Cont
import h3ronpy.polars

def geom_to_wkb(df:gpd.GeoDataFrame)->pl.DataFrame:
    """
    convert GeoDataFrame to polars.DataFrame
    (geometry to wkb)
    """
    if df.crs != 'epsg:4326':
        raise ValueError("The input GeoDataFrame CRS must be in EPSG:4326")

    df = (
        df
        .assign(geometry_wkb = lambda df: to_wkb(df['geometry']))
        .drop('geometry', axis=1)
    )
    return (
        pl.DataFrame(df)
    )
    

def wkb_to_cells(df:pl.DataFrame, source_r:int, geom_col:str='geometry_wkb', selected_cols:list=[]):
    """
    convert geometry to h3 cells
    df: polars.DataFrame, the input dataframe
    source_r: int, the resolution of the source geometry
    selected_cols: list, the columns to be selected
    """
    if geom_col not in df.collect_schema().names():
        raise ValueError(f"Column {geom_col} not found in the input DataFrame, please use `set_geometry()` to set the geometry column first")

    # TODO: use lazyframe instaed of eagerframe?
    return (
        df
        .select(
            pl.col(geom_col)
            .custom.custom_wkb_to_cells(
                resolution=source_r,
                containment_mode=Cont.ContainsCentroid,
                compact=False,
                flatten=False
            ).alias('cell'),
            pl.col(selected_cols) if selected_cols else pl.exclude(geom_col)
        )
        .explode('cell')
    )

def cell_to_geom(df:pl.DataFrame)->gpd.GeoDataFrame:
    """
    convert h3 cells to geometry
    """
    return (
        gpd.GeoDataFrame(
            df
            .select(
                pl.exclude('cell'),
                pl.col('cell')
                .custom.custom_cells_to_wkb_polygons()
                .custom.custom_from_wkb()
                .alias('geometry')
            ).to_pandas()
            , geometry='geometry'
            , crs='epsg:4326'
        )
    )