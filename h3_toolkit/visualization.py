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

    Uses mapclassify to classify data and matplotlib to map colors.

    Args:
        data: Input DataFrame.
        target_col: Column to classify.
        classifier: mapclassify classifier name (e.g., 'NaturalBreaks', 'Quantiles').
        k: Number of classes.
        cmap: matplotlib colormap name.

    Returns:
        DataFrame with 'color' column added (RGBA tuples [R, G, B, A]).
    """
    import mapclassify as mc
    from matplotlib import colormaps

    # Validate inputs
    if k < 2:
        raise ValueError(f"k must be >= 2, got {k}")

    if target_col not in data.columns:
        raise ValueError(f"Column '{target_col}' not found in data")

    # Convert to pandas for classification (mapclassify works with numpy/pandas)
    df_pandas = data.to_pandas()
    values = df_pandas[target_col].values

    # Create classifier
    try:
        classifier_class = getattr(mc, classifier)
        classification = classifier_class(values, k=k)
    except AttributeError:
        raise ValueError(
            f"Unknown classifier '{classifier}'. "
            f"See https://pysal.org/mapclassify/api.html for available classifiers."
        )

    # Get the bin (class) for each value
    # mapclassify stores the bin assignment in the yb attribute
    bins = classification.yb

    # Get colormap
    try:
        cmap_obj = colormaps[cmap]
    except KeyError:
        raise ValueError(
            f"Unknown colormap '{cmap}'. "
            f"See https://matplotlib.org/stable/users/explain/colors/colormaps.html"
        )

    # Map each bin to a color
    colors = []
    for bin_idx in bins:
        # Normalize bin index to [0, 1] for colormap
        normalized = bin_idx / (k - 1) if k > 1 else 0
        rgba = cmap_obj(normalized)
        # Convert to RGBA (0-255)
        rgb = [int(255 * c) for c in rgba[:3]]
        alpha = int(255 * rgba[3]) if len(rgba) > 3 else 255
        colors.append(rgb + [alpha])

    # Add color column back to original data
    result = data.clone()
    result = result.with_columns(
        pl.Series('color', colors)
    )

    return result


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
    try:
        _check_dependencies()
    except ImportError as e:
        raise ImportError(
            f"Cannot visualize data: {e}\n"
            f"Install visualization dependencies with: pip install h3-toolkit[visualization]"
        ) from e

    import pydeck as pdk

    # Validate inputs
    if target_col not in data.columns:
        raise ValueError(f"Column '{target_col}' not found in data")

    if h3_col not in data.columns:
        raise ValueError(f"Column '{h3_col}' not found in data")

    if k < 2:
        raise ValueError(f"k must be >= 2, got {k}")

    # Add colors to data
    data_with_colors = _set_color(
        data,
        target_col,
        classifier=classifier,
        k=k,
        cmap=cmap
    )

    # Convert to dict format for pydeck
    data_dict = data_with_colors.to_dicts()

    # Calculate view state
    hex_ids = data[h3_col].to_list()
    view_state = _calculate_initial_view_state(hex_ids)

    # Create H3 hexagon layer
    layer = pdk.Layer(
        'H3HexagonLayer',
        data_dict,
        get_fill_color='color',
        get_hexagon=h3_col,
        pickable=True,
        opacity=0.4,
        stroked=False,
        filled=True,
        extruded=False,
    )

    # Create deck
    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=pdk.ViewState(**view_state),
        tooltip={"text": f"{target_col}: {{{target_col}}}"},
        **pydeck_kwargs
    )

    # Save or return
    if save_to:
        deck.to_html(save_to)
        logger.info(f"Map saved to {save_to}")
        return None
    else:
        return deck
