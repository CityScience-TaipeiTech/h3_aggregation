import warnings

import polars as pl
import pytest

from h3_toolkit.aggregation import Centroid, Count, EqualSplit, Mean, SplitEqually, Sum, SumUp


@pytest.fixture
def sample_lazy_df():
    # Mimics the LazyFrame that _apply_strategy() receives after wkb_to_cells() + explode():
    # each H3 cell row inherits the source polygon's value, so rows within the same
    # boundary share identical values before aggregation.
    return pl.LazyFrame({
        'cell': [1, 2, 3, 4, 5],
        'codebase': ['A', 'A', 'A', 'B', 'B'],
        'p_cnt': [30, 30, 30, 40, 40],
        'h_cnt': [6, 6, 6, 8, 8],
    })


def test_equal_split(sample_lazy_df):
    # EqualSplit divides each boundary's value evenly across all H3 cells that fall
    # within it.  Boundary A has 3 cells with value 30 → each cell gets 10.0.
    # Boundary B has 2 cells with value 40 → each cell gets 20.0.
    strategy = EqualSplit(agg_col='codebase')
    result = strategy.apply(sample_lazy_df, target_cols=['p_cnt']).collect()

    assert 'cell' in result.columns
    assert 'p_cnt' in result.columns
    a_vals = result.filter(pl.col('cell').is_in([1, 2, 3]))['p_cnt'].to_list()
    assert a_vals == [10.0, 10.0, 10.0]
    b_vals = result.filter(pl.col('cell').is_in([4, 5]))['p_cnt'].to_list()
    assert b_vals == [20.0, 20.0]


def test_split_equally_deprecated(sample_lazy_df):
    # SplitEqually is a deprecated alias for EqualSplit.  Instantiating it must emit a
    # DeprecationWarning with a message pointing users to EqualSplit.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        strategy = SplitEqually(agg_col='codebase')

    assert len(caught) == 1
    assert issubclass(caught[0].category, DeprecationWarning)
    assert "EqualSplit" in str(caught[0].message)

    # The deprecated class must still produce correct results so that existing
    # callers continue to work without modification.
    result = strategy.apply(sample_lazy_df, target_cols=['p_cnt']).collect()
    a_vals = result.filter(pl.col('cell').is_in([1, 2, 3]))['p_cnt'].to_list()
    assert a_vals == [10.0, 10.0, 10.0]


def test_sum(sample_lazy_df):
    # Sum groups by cell and sums the target columns.  Since every cell is unique in
    # the fixture, the per-cell sum equals the original value; the grand total must
    # therefore equal the sum of all input rows.
    strategy = Sum()
    result = strategy.apply(sample_lazy_df, target_cols=['p_cnt', 'h_cnt']).collect()

    assert 'cell' in result.columns
    assert 'p_cnt' in result.columns
    assert result['p_cnt'].sum() == pytest.approx(30 + 30 + 30 + 40 + 40)


def test_sum_up_deprecated(sample_lazy_df):
    # SumUp is a deprecated alias for Sum.  Instantiating it must emit a
    # DeprecationWarning with a message pointing users to Sum.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        strategy = SumUp()

    assert len(caught) == 1
    assert issubclass(caught[0].category, DeprecationWarning)
    assert "Sum" in str(caught[0].message)

    # The deprecated class must still produce correct results so that existing
    # callers continue to work without modification.
    result = strategy.apply(sample_lazy_df, target_cols=['p_cnt']).collect()
    assert result['p_cnt'].sum() == pytest.approx(30 + 30 + 30 + 40 + 40)


def test_mean(sample_lazy_df):
    # Mean groups by cell and averages the target column.  With unique cells, the mean
    # equals the original value and the row count must remain unchanged.
    strategy = Mean()
    result = strategy.apply(sample_lazy_df, target_cols=['p_cnt']).collect()

    assert 'cell' in result.columns
    assert 'p_cnt' in result.columns
    assert result.shape[0] == 5


def test_centroid(sample_lazy_df):
    # Centroid passes the source polygon's value directly to each H3 cell that contains
    # the polygon's centroid — no aggregation is performed, so all input rows and both
    # target columns must be present in the output unchanged.
    strategy = Centroid()
    result = strategy.apply(sample_lazy_df, target_cols=['p_cnt', 'h_cnt']).collect()

    assert 'cell' in result.columns
    assert 'p_cnt' in result.columns
    assert 'h_cnt' in result.columns
    assert result.shape[0] == 5
