import polars as pl
import h3ronpy.polars
import geopandas as gpd
from enum import Enum
from typing import Literal, Callable, Optional
from h3_tool.hbase.tools import put_data, get_data
from h3_tool.aggregator.aggregator import _sum, _avg, _count, _major, _percentage
from h3_tool.aggregator.aggregator_up import _sum_agg, _avg_agg
from h3_tool.geom_processor import geom_to_wkb, wkb_to_cells

class AggFunc(Enum):
    """
    5 ways to aggregate the data
    """
    SUM = 'sum'
    AVG = 'avg'
    COUNT = 'count'
    MAJOR = 'major'
    PERCENTAGE = 'percentage'

def vector_to_cell_hbase(
    data: pl.DataFrame | gpd.GeoDataFrame,
    agg_func: Literal['sum', 'avg', 'count', 'major', 'percentage'],
    target_cols: list[str],
    agg_col: Optional[str] = None,
    geometry_col: str = 'geometry_wkb',
    resolution: int = 12,
    # source_r: int = 12,
    # target_r: int = 9,
)->pl.DataFrame:
    """
    Args:
        data: pl.DataFrame | gpd.GeoDataFrame, the input data
        agg_func: Literal['sum', 'avg', 'count', 'major', 'percentage'], the aggregation function
        target_cols: list[str], the columns to be aggregated
        agg_cols: Optional[list[str]], the columns to be aggregated by, usually is a boundary
        r: int, the h3 resolution
    Returns:
        pl.DataFrame, the aggregated target data in h3 cells 
    """

    selected_cols:list[str] = [agg_col] + target_cols if agg_col else target_cols

    if isinstance(data, gpd.GeoDataFrame):
        """
        convert GeoDataFrame to polars.DataFrame
        """
        data = geom_to_wkb(data)

    aggregation_func: dict[AggFunc, list[Callable[..., pl.DataFrame]]] = {
        AggFunc.SUM.value: [lambda df: _sum(df, target_cols, agg_col), lambda df: _sum_agg(df, target_cols)],
        AggFunc.AVG.value: [lambda df: _avg(df, target_cols), lambda df: _avg_agg(df, target_cols)],
        AggFunc.COUNT.value: [lambda df: _count(df, target_cols, include_nan=True), lambda df: _sum_agg(df, target_cols)],
        AggFunc.MAJOR.value: _major,
        AggFunc.PERCENTAGE.value: _percentage,
    }

    func = aggregation_func.get(agg_func)

    # resolution 12 （基底resolution），aggregate後存入hbase
    if resolution == 12:
        result = (
            data
            .fill_nan(0) 
            .lazy() 
            .pipe(wkb_to_cells, resolution, selected_cols, geometry_col) # convert geometry to h3 cells
            .pipe(func[0]) # aggregate the data
            .select(  # Convert the cell(unit64) to string
                pl.col('cell')
                .h3.cells_to_string().alias('hex_id'),
                pl.exclude('cell')
            )
            .collect(streaming=True)
        )

    # resolution < 12，從hbase取資料後再存入hbase
    elif resolution < 12:
        target_cols = [f"{target_cols}_{agg_func}" for target_cols in target_cols]

        # get the r12 cells (rowkeys)
        rowkeys_df = (
            data
            .fill_nan(0) 
            .lazy() 
            .pipe(wkb_to_cells, resolution, geometry_col) # convert geometry to h3 cells
            .select(
                pl.col('cell')
                .h3.change_resolution(12)
                .h3.cells_to_string()
                .unique()
                .alias('hex_id'), # scale down to resolution 12
            )
            .collect(streaming=True)
        )

        # call hbase api to get the data from the r12 cells
        data = get_data(
            table_name='res12_pre_data',
            cf='demographic',
            cq_list = target_cols,
            rowkeys = rowkeys_df['hex_id'].to_list(),
        )
        
        result = (
            data
            .lazy()
            .with_columns(
                pl.col('hex_id')
                .h3.cells_parse()
                .h3.change_resolution(resolution)
                .alias('cell')
            )
            .pipe(func[1])
            .select(  # Convert the cell(unit64) to string
                pl.col('cell')
                .h3.cells_to_string().alias('hex_id'),
                pl.exclude('cell')
            )
            .collect(streaming=True)
        )

    return result


    # result = (
    #     data
    #     .fill_null(0)
    #     .lazy()
    #     .pipe(_wkb_to_cells, source_r, selected_cols)
    

    #     # .pipe(_change_resolution, source_r, target_r)
    #     .pipe(_sum, target_cols, agg_cols)

    #     # .select(
    #     #     pl.col('cell')
    #     #     .custom.custom_cells_to_wkb_polygons(),
    #     #     # pl.exclude('cell')
    #     # )
    #     # .pipe(_avg, target_cols)
    #     # .pipe(_count, target_cols)
    #     # .pipe(_major, target_cols, target_r)
    #     # .pipe(agg_func, target_cols)
    #     .select(
    #         # Convert the cell(unit64) to string
    #         pl.col('cell')
    #         .h3.cells_to_string().alias('hex_id'),
    #         pl.exclude('cell')
    #     )
    #     .collect(streaming=True)
    # )

    # # upload data to hbase
    # put_data(
    #     result, 
    #     table_name='res12_test_data',
    #     cf='cf1', 
    #     cq_list=['h_cnt_sum', 'p_cnt_sum'], 
    #     rowkey_col='hex_id', 
    #     timestamp=None
    # )

    # return result

def vector_to_cell(
    
):
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