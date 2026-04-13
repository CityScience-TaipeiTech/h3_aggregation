"""Visualization utilities for H3 hexagon data with pydeck."""

import logging
from typing import Optional

import polars as pl

logger = logging.getLogger(__name__)


def _check_dependencies() -> None:
    """
    Check if visualization dependencies are installed.

    Raises:
        ImportError: If required packages are missing.
    """
    missing = []

    try:
        import pydeck  # noqa: F401
    except ImportError:
        missing.append('pydeck')

    try:
        import mapclassify  # noqa: F401
    except ImportError:
        missing.append('mapclassify')

    if missing:
        raise ImportError(
            f"Missing visualization dependencies: {', '.join(missing)}. "
            f"Install with: pip install h3-toolkit[visualization]"
        )


# Run dependency check at module import time
try:
    _check_dependencies()
except ImportError:
    # If dependencies are missing at import, we'll catch it again at function call
    pass


def _calculate_initial_view_state(hex_ids: list[str]) -> dict:
    """
    Calculate initial pydeck ViewState based on hex_ids extent.

    Args:
        hex_ids: List of H3 hexagon IDs.

    Returns:
        Dictionary with keys: longitude, latitude, zoom, pitch, bearing.
    """
    pass


def _set_color(
    data: pl.DataFrame,
    target_col: str,
    classifier: str = 'NaturalBreaks',
    k: int = 5,
    cmap: str = 'Oranges',
) -> pl.DataFrame:
    """
    Add color column to data based on classification.

    Args:
        data: Input DataFrame.
        target_col: Column to classify.
        classifier: mapclassify classifier name.
        k: Number of classes.
        cmap: matplotlib colormap name.

    Returns:
        DataFrame with 'color' column added (RGBA tuples).
    """
    pass


def show_h3(
    data: pl.DataFrame,
    target_col: str,
    h3_col: str = 'hex_id',
    classifier: str = 'NaturalBreaks',
    k: int = 5,
    cmap: str = 'Oranges',
    save_to: Optional[str] = None,
    **pydeck_kwargs
):
    """
    Visualize H3 hexagon data with pydeck.

    Args:
        data: DataFrame with H3 hexagon IDs and values.
        target_col: Column to visualize.
        h3_col: Name of hexagon ID column.
        classifier: mapclassify classifier name.
        k: Number of classes.
        cmap: matplotlib colormap name.
        save_to: Path to save HTML file. If None, returns Deck object.
        **pydeck_kwargs: Additional arguments for pdk.Deck.

    Returns:
        pdk.Deck object or None if saved to file.
    """
    pass
