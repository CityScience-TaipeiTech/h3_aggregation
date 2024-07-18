import polars as pl
import h3ronpy.polars

def _sum(df:pl.DataFrame, target_cols:list[str], agg_col:str)->pl.DataFrame:
    """
    target_cols: list, the columns to be aggregated
    agg_cols: list, the columns to be aggregated by, usually is a boundary
    """

    if agg_col is None:
        raise ValueError("agg_cols must be provided when using sum aggregation")
    
    return (
        df
        .with_columns(
            # first / count over agg_cols(usually is a boundary)
            ((pl.first(target_cols).over(agg_col)) /
            (pl.count(target_cols).over(agg_col)))
            .name.suffix("_sum")
        )
    )

def _avg(df:pl.DataFrame, target_cols:list[str])->pl.DataFrame:
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

def _count(df:pl.DataFrame, target_cols:list[str], include_nan:bool=True)->pl.DataFrame:
    """
    target_cols: list, the columns to be counted inside the designated resolution
    no matter the is nun/null or not
    """
    return (
        df
        .with_columns(
            pl.when(include_nan)
            .then( # 不管是不是nan都會算
                pl.col(target_cols).len().over('cell')
            )
            .otherwise( # 只算不是nan的
                pl.count(target_cols).over('cell')
            ).name.suffix("_count")
        )
    )

# TODO
def _major(df:pl.DataFrame, target_cols:list[str], target_r)->pl.DataFrame:
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
def _percentage(df:pl.DataFrame, target_cols, target_r)->pl.DataFrame:
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