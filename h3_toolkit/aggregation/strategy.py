import polars as pl
import h3ronpy.polars
from abc import ABC, abstractmethod

class AggregationStrategy(ABC):
    @abstractmethod
    def apply(self, df:pl.DataFrame, target_cols:list[str], agg_col:str)->pl.DataFrame:
        pass

class SumAggregation(AggregationStrategy):
    """
    同一個resolution的cell內的數值相加
    """
    def apply(self, df: pl.DataFrame, target_cols: list[str], agg_col: str) -> pl.DataFrame:
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
    
class SumAggregationUp(AggregationStrategy):
    """
    用於將小的reslution scale up 到大的resolution
    """
    def apply(self, df: pl.DataFrame, target_cols: list[str], agg_col: str = None) -> pl.DataFrame:
        """
        Scale Up Function
        target_cols: list, the columns to be aggregated
        agg_cols: list, the columns to be aggregated by, usually is a boundary
        """
        target_cols = [target_col for target_col in target_cols if target_col in df.collect_schema().names()]
        return (
            df
            .group_by(
                'cell'
            )
            .agg(
                pl.col(target_cols).cast(pl.Float64).sum()
            )
        )

class AvgAggregation(AggregationStrategy):
    def apply(self, df: pl.DataFrame, target_cols: list[str], agg_col: str) -> pl.DataFrame:
        return (
            df
            .with_columns(
                pl.col(target_cols).name.suffix("_avg")
            )
        )

class AvgAggregationUp(AggregationStrategy):
    def apply(self, df: pl.DataFrame, target_cols: list[str], agg_col: str) -> pl.DataFrame:
        return (
            df
            .group_by(
                'cell'
            )
            .agg(
                pl.col(target_cols).cast(pl.Float64).mean()
            )
        )

class CountAggregation(AggregationStrategy):
    def apply(self, df: pl.DataFrame, target_cols: list[str], agg_col: str) -> pl.DataFrame:
        return (
            df
            .group_by(['cell', *target_cols])
            .agg([
                pl.count().alias(f'{'_'.join(target_cols)}_count'),
            ])
            .fill_null('null')
            .collect()
            # lazyframe -> dataframe, dataframe is needed for pivot
            .pivot(
                values = f'{'_'.join(target_cols)}_count',
                index = 'cell',
                on = target_cols
            )
            .with_columns(
                pl.sum_horizontal(pl.exclude('cell')).alias('total_count')
            )
            # dataframe -> lazyframe
            .lazy()
        )
    
# TODO:
class MajorAggregation(AggregationStrategy):
    def apply(self, df: pl.DataFrame, target_cols: list[str], agg_col: str) -> pl.DataFrame:
        pass

# TODO:
class PercentageAggregation(AggregationStrategy):
    def apply(self, df: pl.DataFrame, target_cols: list[str], agg_col: str) -> pl.DataFrame:
        pass

# def _sum(df:pl.DataFrame, target_cols:list[str], agg_col:str)->pl.DataFrame:
#     """
#     target_cols: list, the columns to be aggregated
#     agg_cols: list, the columns to be aggregated by, usually is a boundary
#     """

#     if agg_col is None:
#         raise ValueError("agg_cols must be provided when using sum aggregation")
    
#     return (
#         df
#         .with_columns(
#             # first / count over agg_cols(usually is a boundary)
#             ((pl.first(target_cols).over(agg_col)) /
#             (pl.count(target_cols).over(agg_col)))
#             .name.suffix("_sum")
#         )
#     )

# def _sum_agg(df:pl.DataFrame, target_cols:list[str])->pl.DataFrame:
#     """
#     Scale Up Function
#     target_cols: list, the columns to be aggregated
#     agg_cols: list, the columns to be aggregated by, usually is a boundary
#     """
#     target_cols = [target_col for target_col in target_cols if target_col in df.columns]
#     # print(target_cols)
#     return (
#         df
#         .group_by(
#             'cell'
#         )
#         .agg(
#             pl.col(target_cols).cast(pl.Float64).sum()
#         )
#     )

# def _avg(df:pl.DataFrame, target_cols:list[str])->pl.DataFrame:
#     # base function
#     """
#     without doing anything
#     """
#     return (
#         df
#         .with_columns(
#             pl.col(target_cols).name.suffix("_avg")
#         )
#     )

# def _avg_agg(df:pl.DataFrame, target_cols:list[str])->pl.DataFrame:

#     """
#     target_cols: list, the columns to be counted inside the designated resolution
#     """
#     return (
#         df
#         .group_by(
#             'cell'
#         )
#         .agg(
#             pl.col(target_cols).cast(pl.Float64).mean()
#         )
#     )

# def _count(df:pl.LazyFrame, target_cols:list[str], include_nan:bool=True)->pl.LazyFrame:
#     """
#     target_cols: list, the columns to be counted inside the designated resolution
#     no matter the is nun/null or not
#     """
#     return (
#         df
#         .group_by(['cell', *target_cols])
#         .agg([
#             pl.count().alias(f'{'_'.join(target_cols)}_count'),
#         ])
#         .fill_null('null')
#         .collect()
#         # lazyframe -> dataframe, dataframe is needed for pivot
#         .pivot(
#             values = f'{'_'.join(target_cols)}_count',
#             index = 'cell',
#             on = target_cols
#         )
#         .with_columns(
#             pl.sum_horizontal(pl.exclude('cell')).alias('total_count')
#         )
#         # dataframe -> lazyframe
#         .lazy()

#         # .with_columns(
#         #     pl.when(include_nan)
#         #     .then( # 不管是不是nan都會算
#         #         pl.col(target_cols)
#         #         # .len()
#         #         .over('cell')
#         #         .agg_groups()
#         #         .len()
#         #     )
#         #     .otherwise( # 只算不是nan的
#         #         pl.count(target_cols)
#         #         .over('cell')
#         #     ).name.suffix("_count")
#         # )
#     )

# def _major(df:pl.DataFrame, target_cols:list[str], target_r)->pl.DataFrame:
#     # 會影響output cell數量
#     # 把change_resolution拉出去
#     # scale up function
#     """
#     target_cols: list, the columns to be counted inside the designated resolution
#     target_r must be bigger than the source_r
#     """
#     return (
#         df
#         # scale up the resolution to the target resolution
#         .with_columns(
#             pl.col('cell')
#             .h3.change_resolution(target_r)
#             .name.suffix(f"_{target_r}")
#         )
#         # get the most frequent value in the cell, if there are multiple values, return the first one
#         .groupby(f"cell_{target_r}")
#         .agg(
#             pl.col(target_cols)
#             .mode() # get the most frequent value
#             .first() # the first one
#             .name.suffix("_major")
#         )
#     )

# def _percentage(df:pl.DataFrame, target_cols, target_r)->pl.DataFrame:
#     # 把change_resolution拉出去
#     # scale up function
#     """
#     target_cols: list, the columns to be counted inside the designated resolution
#     """
#     return (
#         df
#         .with_columns(
#             pl.col('cell')
#             .h3.change_resolution(target_r)
#             .name.suffix(f"_{target_r}")
#         )
#         .groupby(f"cell_{target_r}")
#         .agg(
#             pl.col(target_cols)
#             .value_counts()
#             .unstack()
#             .alias('count_')
#         )
#     )