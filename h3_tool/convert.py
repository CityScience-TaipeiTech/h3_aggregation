import polars as pl
from shapely import to_wkb
import h3ronpy.polars
from h3ronpy import ContainmentMode as Cont
import geopandas as gpd
from h3_tool.hbase_put import put_data
from h3_tool.hbase_get import get_data


def _geom_to_wkb(df:gpd.GeoDataFrame)->pl.DataFrame:
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
    

def _wkb_to_cells(df:pl.DataFrame, source_r:int, selected_cols:list, geom_col:str='geometry_wkb'):
    """
    convert geometry to h3 cells
    df: polars.DataFrame, the input dataframe
    source_r: int, the resolution of the source geometry
    selected_cols: list, the columns to be selected
    """
    # TODO: use lazyframe instaed of eagerframe?
    return (
        df
        .select(
            pl.col('geometry_wkb')
            .custom.custom_wkb_to_cells(
                resolution=source_r,
                containment_mode=Cont.ContainsCentroid,
                compact=False,
                flatten=False
            ).alias('cell'),
            pl.col(selected_cols)
        )
        .explode('cell')
    )

def _cell_to_geom(df:pl.DataFrame)->gpd.GeoDataFrame:
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
            , geometry='geometry', crs='epsg:4326')
    )

def _sum(df, target_cols, agg_cols):
    """
    target_cols: list, the columns to be aggregated
    agg_cols: list, the columns to be aggregated by, usually is a boundary
    """
    return (
        df
        .with_columns(
            # first / count over agg_cols(usually is a boundary)
            ((pl.first(target_cols).over(agg_cols)) /
            (pl.count(target_cols).over(agg_cols)))
            .name.suffix("_sum")
        )
    )

def _avg(df, target_cols):
    # base function
    """
    without doing anything
    """
    return (
        df
        .with_columns(
            pl.col(target_cols).name.suffix("_avg")
        )
    )

def _count(df, target_cols):
    # base function
    """
    target_cols: list, the columns to be counted inside the designated resolution
    # no matter the is nun/null or not
    """
    return (
        df
        .with_columns(
            # Don't care null/nun
            pl.col(target_cols).len().over('cell').name.suffix("_count"),
            # Care null/nun
            # pl.count(target_cols).over('cell').name.suffix("_count")
        ) 
    )

def _major(df, target_cols, target_r):
    # 會影響output cell數量
    # 把change_resolution拉出去
    # scale up function
    """
    target_cols: list, the columns to be counted inside the designated resolution
    target_r must be bigger than the source_r
    """
    return (
        df
        # scale up the resolution to the target resolution
        .with_columns(
            pl.col('cell')
            .h3.change_resolution(target_r)
            .name.suffix(f"_{target_r}")
        )
        # get the most frequent value in the cell, if there are multiple values, return the first one
        .groupby(f"cell_{target_r}")
        .agg(
            pl.col(target_cols)
            .mode() # get the most frequent value
            .first() # the first one
            .name.suffix("_major")
        )
    )

# TODO
def _percentage(df, target_cols, target_r):
    # 把change_resolution拉出去
    # scale up function
    """
    target_cols: list, the columns to be counted inside the designated resolution
    """
    return (
        df
        .with_columns(
            pl.col('cell')
            .h3.change_resolution(target_r)
            .name.suffix(f"_{target_r}")
        )
        .groupby(f"cell_{target_r}")
        .agg(
            pl.col(target_cols)
            .value_counts()
            .unstack()
            .alias('count_')
        )
    )

def _get_cell_data_from_hbase(rows):
    pass

# def _change_resolution(df, source_r, target_r):
#     """
#     change the resolution of the cell
#     """
#     diff_r = 7**(source_r - target_r) # scale up
#     return (
#         df
#         # convert into the designated resolution of the cell
#         .with_columns(
#             pl.col('cell')
#             .h3.change_resolution(target_r).name.suffix(f"_{target_r}"),
#         )
#         .groupby(f'cell_{target_r}')
#         .agg(
#             pl.count(f'cell_{target_r}').alias('count_')
#         )
#         # find the missing h3 cells (uncomplete aggregation, in the boundary)
#         .filter(
#             pl.col('count_').eq(diff_r)
#         )
#         .select(
#             pl.col(f'cell_{target_r}')
#             .h3.change_resolution(source_r)
#             # .h3.cells_to_string()
#             .alias('hex_id'),
#         )
#     )

def vector_to_cell(
    data: pl.DataFrame | gpd.GeoDataFrame,
    agg_func,
    agg_cols: list,
    target_cols: list,
    source_r: int = 12,
    # target_r: int = 9,
)->pl.DataFrame:
    
    selected_cols = agg_cols + target_cols
    # data = data.copy()
    if isinstance(data, gpd.GeoDataFrame):
        data = _geom_to_wkb(data)

    result = (
        data
        .fill_null(0)
        .lazy()
        .pipe(_wkb_to_cells, source_r, selected_cols)
    

        # .pipe(_change_resolution, source_r, target_r)
        .pipe(_sum, target_cols, agg_cols)

        # .select(
        #     pl.col('cell')
        #     .custom.custom_cells_to_wkb_polygons(),
        #     # pl.exclude('cell')
        # )
        # .pipe(_avg, target_cols)
        # .pipe(_count, target_cols)
        # .pipe(_major, target_cols, target_r)
        # .pipe(agg_func, target_cols)
        .select(
            # Convert the cell(unit64) to string
            pl.col('cell')
            .h3.cells_to_string().alias('hex_id'),
            pl.exclude('cell')
        )
        .collect(streaming=True)
    )

    # upload data to hbase
    put_data(
        result, 
        table_name='res12_test_data',
        cf='cf1', 
        cq_list=['h_cnt_sum', 'p_cnt_sum'], 
        rowkey_col='hex_id', 
        timestamp=None
    )

    return result

# def geodf_to_polardf(gdf):
#     gdf['geometry'] = gdf['geometry'].map(to_wkb)
#     return pl.DataFrame(gdf)