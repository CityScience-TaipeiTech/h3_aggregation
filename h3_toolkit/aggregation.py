""" """

import warnings
from abc import ABC, abstractmethod

import polars as pl


class AggregationStrategy(ABC):
    @abstractmethod
    def apply(self, data: pl.LazyFrame, target_cols: list[str]) -> pl.LazyFrame:
        raise NotImplementedError("Subclasses must implement this method")


class EqualSplit(AggregationStrategy):
    """Disaggregation strategy that divides a polygon's value equally across all H3 cells
    that fall within it.

    .. image:: ../../images/SplitEqually.svg
    """

    def __init__(self, agg_col: str):
        """
        Args:
            agg_col (str): the boundary column used to group cells, e.g. city, town, village.
        """
        self.agg_col = agg_col

    def apply(self, data: pl.LazyFrame, target_cols: list[str]) -> pl.LazyFrame:
        return data.with_columns(
            [
                (
                    (pl.col(col).cast(pl.Float64).first().over(self.agg_col))
                    / (pl.col(col).count().over(self.agg_col).cast(pl.Float64))
                ).alias(col)
                for col in target_cols
            ]
        ).select(pl.col("cell"), pl.col(target_cols))


class SplitEqually(EqualSplit):
    """Deprecated. Use :class:`EqualSplit` instead."""

    def __init__(self, agg_col: str):
        warnings.warn(
            "SplitEqually is deprecated and will be removed in a future version. " "Use EqualSplit instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(agg_col)


class Centroid(AggregationStrategy):
    """
    .. image:: ../../images/Centroid.svg
    """

    def apply(self, data: pl.LazyFrame, target_cols: list[str]) -> pl.LazyFrame:
        return data.with_columns(
            [pl.col(col).alias(col) for col in target_cols]
        ).select(  # only keep the necessary columns
            pl.col("cell"), pl.col(target_cols)
        )


class Sum(AggregationStrategy):
    """Aggregation strategy that groups H3 cells and sums the target columns.

    .. image:: ../../images/SumUp.svg
    """

    def apply(self, data: pl.LazyFrame, target_cols: list[str]) -> pl.LazyFrame:
        return data.group_by("cell").agg(pl.col(target_cols).cast(pl.Float64).sum())


class SumUp(Sum):
    """Deprecated. Use :class:`Sum` instead."""

    def __init__(self):
        warnings.warn(
            "SumUp is deprecated and will be removed in a future version. " "Use Sum instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__()


class Mean(AggregationStrategy):
    """
    .. image:: ../../images/Mean.svg
    """

    def apply(self, data: pl.LazyFrame, target_cols: list[str]) -> pl.LazyFrame:
        return data.group_by("cell").agg(pl.col(target_cols).cast(pl.Float64).mean())


class Count(AggregationStrategy):
    """
    .. image:: ../../images/Count.svg
    """

    def __init__(self, return_percentage: bool = False):
        self.return_percentage = return_percentage

    def apply(self, data: pl.LazyFrame, target_cols: list[str]) -> pl.LazyFrame:
        if target_cols == ["hex_id"]:
            # focus on the h3 index
            return (
                data.group_by("cell")
                .agg(
                    [
                        pl.count().alias("total_count").cast(pl.Int64),
                    ]
                )
                .lazy()
            )
        elif target_cols:
            counts_df = (
                data.group_by(["cell", *target_cols])
                .agg(
                    [
                        pl.count().alias(f'{"_".join(target_cols)}_count').cast(pl.Int64),
                    ]
                )
                .fill_null("null")
            )

            # Note: pivot() operation requires eager evaluation (cannot be done lazily in Polars).
            # We must call .collect() to materialize the data before pivoting. This breaks the
            # lazy chain temporarily, but is unavoidable for this aggregation strategy.
            pivoted_df = (
                counts_df.collect()
                .pivot(values=f'{"_".join(target_cols)}_count', index="cell", on=target_cols)
                .with_columns(pl.sum_horizontal(pl.exclude("cell")).alias("total_count").cast(pl.Int64))
            )

            # Check if return_percentage is True
            if self.return_percentage:
                # Calculate percentage for each column

                # percentage 只有建立在total_count都一樣的基礎上才有意義
                percentage_cols = [
                    (pl.col(col) / pl.col("total_count") * 100).round(3)
                    for col in pivoted_df.columns
                    if col != "cell" and col != "total_count"
                ]
                return (
                    pivoted_df.with_columns(percentage_cols)
                    # remove total_count if return_percentage is True
                    .select(pl.exclude("total_count"))
                    .lazy()  # dataframe -> lazyframe
                )
            else:
                # Return counts directly
                return pivoted_df.lazy()  # dataframe -> lazyframe
