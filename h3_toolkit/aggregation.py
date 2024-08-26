"""

"""


from abc import ABC, abstractmethod

import polars as pl


class AggregationStrategy(ABC):

    @abstractmethod
    def apply(self, data:pl.LazyFrame, target_cols:list[str]) -> pl.LazyFrame:
        raise NotImplementedError("Subclasses must implement this method")

class SplitEqually(AggregationStrategy):
    def __init__(self, agg_col:str):
        """
        Args:
            agg_col (str): usually is the boundary, ie: city, town, village, etc.
        """
        self.agg_col = agg_col

    def apply(self, data:pl.LazyFrame, target_cols:list[str]) -> pl.LazyFrame:
        """Provide an example

        Args:
            data (pl.LazyFrame): _description_
            target_cols (list[str]): _description_
            agg_col (str): _description_
        """
        return (
            data
            .with_columns([
                # first / count over agg_cols(usually is a boundary)
                ((pl.first(col).over(self.agg_col)) /
                (pl.count(col).over(self.agg_col))).alias(col) # overwrite the original column
                for col in target_cols
            ])
            .select( # only keep the necessary columns
                pl.col('cell'),
                pl.col(target_cols)
            )
        )

class Centroid(AggregationStrategy):
    def apply(self, data:pl.LazyFrame, target_cols:list[str]) -> pl.LazyFrame:
        return (
            data
            .with_columns([
                pl.col(col).alias(col)
                for col in target_cols
            ])
            .select( # only keep the necessary columns
                pl.col('cell'),
                pl.col(target_cols)
            )
        )

class SumUp(AggregationStrategy):
    def apply(self, df: pl.DataFrame, target_cols: list[str]) -> pl.DataFrame:
        """
        Scale Up Function
        target_cols: list, the columns to be aggregated
        """
        # target_cols = [
        # target_col for target_col in target_cols if target_col in df.collect_schema().names()]
        return (
            df
            .group_by(
                'cell'
            )
            .agg(
                pl.col(target_cols).cast(pl.Float64).sum()
            )
        )

class Mean(AggregationStrategy):
    def apply(self, data: pl.LazyFrame, target_cols: list[str]) -> pl.LazyFrame:
        return (
            data
            .group_by(
                'cell'
            )
            .agg(
                pl.col(target_cols).cast(pl.Float64).mean()
            )
        )

class Count(AggregationStrategy):
    def __init__(self, return_percentage: bool = False):
        self.return_percentage = return_percentage

    def apply(self, data:pl.LazyFrame, target_cols:list[str]) -> pl.LazyFrame:
        if target_cols == ['hex_id']:
            # focus on the h3 index
            return (
                data
                .group_by('cell')
                .agg([
                    pl.count().alias('total_count').cast(pl.Int64),
                ])
                .lazy()
            )
        elif target_cols:
            counts_df = (
                data
                .group_by(['cell', *target_cols])
                .agg([
                    pl.count().alias(f'{'_'.join(target_cols)}_count').cast(pl.Int64),
                ])
                .fill_null('null')
            )

            pivoted_df = (
                counts_df
                .collect()
                # lazyframe -> dataframe, dataframe is needed for pivot
                .pivot(
                    values = f'{'_'.join(target_cols)}_count',
                    index = 'cell',
                    on = target_cols
                )
                .with_columns(
                    pl.sum_horizontal(pl.exclude('cell')).alias('total_count').cast(pl.Int64)
                )
            )

            # Check if return_percentage is True
            if self.return_percentage:
                # Calculate percentage for each column

                # percentage 只有建立在total_count都一樣的基礎上才有意義
                percentage_cols = [
                    (pl.col(col) / pl.col('total_count') * 100).round(3)
                    for col in pivoted_df.columns if col != 'cell' and col != 'total_count'
                ]
                return (
                    pivoted_df
                    .with_columns(percentage_cols)
                    # remove total_count if return_percentage is True
                    .select(pl.exclude('total_count'))
                    .lazy()  # dataframe -> lazyframe
                )
            else:
                # Return counts directly
                return pivoted_df.lazy()  # dataframe -> lazyframe
