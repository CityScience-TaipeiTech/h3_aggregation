import polars as pl

def _sum_agg(df:pl.DataFrame, target_cols:list[str])->pl.DataFrame:
    """
    Scale Up Function
    target_cols: list, the columns to be aggregated
    agg_cols: list, the columns to be aggregated by, usually is a boundary
    """
    return (
        df
        .group_by(
            'cell'
        )
        .agg(
            pl.col(target_cols).cast(pl.Float64).sum()
        )
    )

def _avg_agg(df:pl.DataFrame, target_cols:list[str])->pl.DataFrame:

    """
    target_cols: list, the columns to be counted inside the designated resolution
    """
    return (
        df
        .group_by(
            'cell'
        )
        .agg(
            pl.col(target_cols).cast(pl.Float64).mean()
        )
    )
