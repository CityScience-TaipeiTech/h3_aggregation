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
        )

class Centroid(AggregationStrategy):
    def apply(self, data:pl.LazyFrame, target_cols:list[str]) -> pl.LazyFrame:
        return (
            data
            .with_columns([
                pl.col(col).alias(col)
                for col in target_cols
            ])
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
    def apply(self, data:pl.LazyFrame, target_cols:list[str]) -> pl.LazyFrame:
        if target_cols == ['hex_id']:
            # focus on the h3 index
            return (
                data
                .group_by('cell')
                .agg([
                    pl.count().alias('count'),
                ])
                .lazy()
            )
        elif target_cols:
            return (
                data
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
