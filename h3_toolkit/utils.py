import geopandas as gpd
import polars as pl
from h3ronpy import ContainmentMode as Cont
from shapely import to_wkb


def geom_to_wkb(df:gpd.GeoDataFrame, geometry:str)->pl.DataFrame:
    """
    convert GeoDataFrame to polars.DataFrame
    (geometry to wkb)
    """
    if df.crs != 'epsg:4326':
        raise ValueError("The input GeoDataFrame CRS must be in EPSG:4326")

    if geometry not in df.columns:
        raise ValueError(f"Column '{geometry}' not found in the input GeoDataFrame")

    # 確保input跟output的geometry的column name不會改變，同時從geometry type 轉換成 wkb
    df = (
        df
        .rename(columns={geometry: 'ready_to_convert'})
        .assign(geometry_wkb = lambda df: to_wkb(df['ready_to_convert']))
        .drop('ready_to_convert', axis=1) # drop geometry column (convert geodataframe to dataframe)
        .rename(columns={'geometry_wkb': geometry})
    )



    return (
        # pandas to polars
        pl.from_pandas(df)
    )

def wkb_to_cells(df:pl.DataFrame,
                 resolution:int,
                 geom_col:str=None,
                #  selected_cols:list=[],
                 mode:Cont=Cont.ContainsCentroid
                 )->pl.DataFrame:
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

    # TODO: use lazyframe instaed of eagerframe?
    return (
        df
        .with_columns(
            pl.col(geom_col)
            .custom.custom_wkb_to_cells(
                resolution=resolution,
                containment_mode=mode,
                compact=False,
                flatten=False
            ).alias('cell'),
            # pl.col(selected_cols) if selected_cols else pl.exclude(geom_col)
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
                pl.exclude('hex_id'),
                pl.col('hex_id')
                .h3.cells_parse()
                .custom.custom_cells_to_wkb_polygons()
                .custom.custom_from_wkb()
                .alias('geometry')
            ).to_pandas()
            , geometry='geometry'
            , crs='epsg:4326'
        )
    )
