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

    Uses h3ronpy to get hexagon boundaries and calculates map center and zoom.

    Args:
        hex_ids: List of H3 hexagon IDs.

    Returns:
        Dictionary with ViewState: longitude, latitude, zoom, pitch, bearing.
    """
    import math
    import h3ronpy

    # Collect all boundary points from all hexagons
    all_lats = []
    all_lons = []

    for hex_id in hex_ids:
        try:
            # Get the boundary of this hexagon as list of (lat, lon) tuples
            boundary = h3ronpy.cells.cell_to_boundary(hex_id)
            for lat, lon in boundary:
                all_lats.append(lat)
                all_lons.append(lon)
        except Exception:
            # Skip invalid hex IDs
            continue

    if not all_lats or not all_lons:
        # Default view if no valid hexagons
        return {
            "longitude": 0,
            "latitude": 0,
            "zoom": 2,
            "pitch": 0,
            "bearing": 0
        }

    # Calculate bounds
    min_lat, max_lat = min(all_lats), max(all_lats)
    min_lon, max_lon = min(all_lons), max(all_lons)

    # Calculate center
    center_lat = (min_lat + max_lat) / 2
    center_lon = (min_lon + max_lon) / 2

    # Calculate zoom based on extent
    lat_range = max_lat - min_lat
    lon_range = max_lon - min_lon
    max_range = max(lat_range, lon_range)

    # Add 10% padding
    max_range = max_range * 1.1

    # Standard zoom formula: zoom = log2(360 * 2^8 / (max_lon - min_lon))
    if max_range > 0:
        zoom = 8 - math.log2(max_range / 360)
    else:
        zoom = 15  # Default high zoom for small areas

    # Clamp zoom to valid range
    zoom = max(0, min(20, zoom))

    return {
        "longitude": center_lon,
        "latitude": center_lat,
        "zoom": zoom,
        "pitch": 0,
        "bearing": 0
    }


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
