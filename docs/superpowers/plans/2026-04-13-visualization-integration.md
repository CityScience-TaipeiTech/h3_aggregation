# H3-Toolkit 可視化模組整合 實現計劃

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 將可視化功能整合到 h3-toolkit，支援多種分類器和顏色映射，讓用戶可以直接在 Jupyter 中展示或導出 H3 hexagon 地圖。

**Architecture:** 新建獨立的 `visualization.py` 模組處理可視化邏輯，在 `H3Toolkit.show()` 中調用。通過可選依賴機制實現即插即用。雙層依賴檢查確保用戶在缺少依賴時收到明確的安裝提示。

**Tech Stack:** pydeck（地圖渲染）、mapclassify（數據分類）、matplotlib（顏色映射）、h3ronpy（邊界計算）

---

## File Structure

**新建檔案：**
- `h3_toolkit/visualization.py` - 可視化模組核心邏輯
- `tests/test_visualization.py` - 可視化模組測試

**修改檔案：**
- `pyproject.toml` - 添加可選依賴
- `h3_toolkit/core.py` - 添加 `show()` 方法
- `h3_toolkit/__init__.py` - 導出可視化 API（可選）

---

## Task 1: 修改 pyproject.toml 添加可選依賴

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: 讀取現有 pyproject.toml**

Run: `cat pyproject.toml | head -50`

確認現有結構，找到 `[tool.poetry.dependencies]` 和 `[build-system]` 區段的位置。

- [ ] **Step 2: 在 poetry dependencies 後添加 extras 部分**

在 `pyproject.toml` 中，`[tool.poetry.group.dev.dependencies]` 之前插入：

```toml
[tool.poetry.extras]
visualization = ["pydeck", "mapclassify"]
```

完整位置應在第 30 行左右（在 dev.dependencies 之前）。

- [ ] **Step 3: 驗證修改**

Run: `grep -A 2 "\[tool.poetry.extras\]" pyproject.toml`

Expected output:
```
[tool.poetry.extras]
visualization = ["pydeck", "mapclassify"]
```

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml
git commit -m "build: add visualization optional dependency group"
```

---

## Task 2: 創建可視化測試框架和依賴檢查

**Files:**
- Create: `tests/test_visualization.py`
- Create: `h3_toolkit/visualization.py`

- [ ] **Step 1: 創建 test_visualization.py 框架**

```python
# tests/test_visualization.py
"""Tests for visualization module."""

import pytest
import polars as pl


class TestDependencyCheck:
    """Test dependency checking mechanisms."""

    def test_visualization_imports_successfully(self):
        """Test that visualization module can be imported when deps are available."""
        try:
            from h3_toolkit.visualization import _check_dependencies
            # If import succeeds, deps are installed
            assert True
        except ImportError as e:
            pytest.skip(f"Visualization dependencies not installed: {e}")

    def test_check_dependencies_function_exists(self):
        """Test that _check_dependencies function exists."""
        try:
            from h3_toolkit.visualization import _check_dependencies
            assert callable(_check_dependencies)
        except ImportError:
            pytest.skip("Visualization dependencies not installed")


class TestColorSetting:
    """Test color setting logic."""

    @pytest.mark.skipif(
        True,  # Will be updated after implementation
        reason="Waiting for implementation"
    )
    def test_set_color_basic(self):
        """Test basic color setting with NaturalBreaks."""
        pass


class TestBoundaryCalculation:
    """Test map boundary and view state calculation."""

    @pytest.mark.skipif(
        True,  # Will be updated after implementation
        reason="Waiting for implementation"
    )
    def test_calculate_initial_view_state(self):
        """Test automatic view state calculation."""
        pass


class TestShow:
    """Test show_h3 function."""

    @pytest.mark.skipif(
        True,  # Will be updated after implementation
        reason="Waiting for implementation"
    )
    def test_show_h3_returns_deck(self):
        """Test that show_h3 returns a pdk.Deck object."""
        pass
```

- [ ] **Step 2: 創建 visualization.py 框架**

```python
# h3_toolkit/visualization.py
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
```

- [ ] **Step 3: 運行測試確認框架正確**

Run: `pytest tests/test_visualization.py -v`

Expected: Tests should run without errors, some marked as skipped/xfail.

- [ ] **Step 4: Commit**

```bash
git add tests/test_visualization.py h3_toolkit/visualization.py
git commit -m "test: add visualization module test framework"
```

---

## Task 3: 實現依賴檢查機制

**Files:**
- Modify: `tests/test_visualization.py`
- Modify: `h3_toolkit/visualization.py`

- [ ] **Step 1: 更新依賴檢查測試**

在 `tests/test_visualization.py` 中，用實際測試替換框架：

```python
class TestDependencyCheck:
    """Test dependency checking mechanisms."""

    def test_visualization_imports_successfully_when_deps_available(self):
        """Test that visualization module imports when deps are available."""
        try:
            import pydeck  # noqa: F401
            import mapclassify  # noqa: F401
            # Dependencies are available, so import should succeed
            from h3_toolkit.visualization import _check_dependencies
            assert callable(_check_dependencies)
        except ImportError as e:
            pytest.skip(f"Visualization dependencies not installed: {e}")

    def test_check_dependencies_raises_without_pydeck(self, monkeypatch):
        """Test that _check_dependencies raises if pydeck is missing."""
        # Mock import to simulate missing pydeck
        import sys
        pydeck_backup = sys.modules.get('pydeck')

        try:
            if 'pydeck' in sys.modules:
                del sys.modules['pydeck']

            # This test is complex with mocking, skip if both deps installed
            try:
                import pydeck  # noqa: F401
                import mapclassify  # noqa: F401
                pytest.skip("Both dependencies installed, cannot test missing dep scenario")
            except ImportError:
                # Dependencies missing, so we can test
                from h3_toolkit.visualization import _check_dependencies
                with pytest.raises(ImportError, match="Missing visualization dependencies"):
                    _check_dependencies()
        finally:
            if pydeck_backup:
                sys.modules['pydeck'] = pydeck_backup

    def test_show_h3_raises_without_deps(self, monkeypatch):
        """Test that show_h3 raises ImportError if deps are missing."""
        try:
            import pydeck  # noqa: F401
            import mapclassify  # noqa: F401
            pytest.skip("Visualization dependencies are installed")
        except ImportError:
            from h3_toolkit.visualization import show_h3
            import polars as pl

            data = pl.DataFrame({'hex_id': ['test'], 'value': [1.0]})

            with pytest.raises(ImportError, match="visualization dependencies"):
                show_h3(data, 'value')
```

- [ ] **Step 2: 驗證依賴檢查測試**

Run: `pytest tests/test_visualization.py::TestDependencyCheck -v`

Expected: Tests pass (some may be skipped if deps are installed).

- [ ] **Step 3: Commit**

```bash
git add tests/test_visualization.py
git commit -m "test: implement dependency check tests"
```

---

## Task 4: 實現邊界計算邏輯

**Files:**
- Modify: `tests/test_visualization.py`
- Modify: `h3_toolkit/visualization.py`

- [ ] **Step 1: 添加邊界計算測試**

在 `tests/test_visualization.py` 的 `TestBoundaryCalculation` 類中：

```python
class TestBoundaryCalculation:
    """Test map boundary and view state calculation."""

    def test_calculate_initial_view_state_with_single_hex(self):
        """Test view state calculation with a single hexagon."""
        try:
            import pydeck  # noqa: F401
            import mapclassify  # noqa: F401
        except ImportError:
            pytest.skip("Visualization dependencies not installed")

        from h3_toolkit.visualization import _calculate_initial_view_state

        # Use a real H3 hex ID
        hex_ids = ['8c4ba0a4e15ffff']

        result = _calculate_initial_view_state(hex_ids)

        assert isinstance(result, dict)
        assert 'longitude' in result
        assert 'latitude' in result
        assert 'zoom' in result
        assert 'pitch' in result
        assert 'bearing' in result

        # Validate ranges
        assert -180 <= result['longitude'] <= 180
        assert -90 <= result['latitude'] <= 90
        assert 0 <= result['zoom'] <= 20
        assert 0 <= result['pitch'] <= 60
        assert 0 <= result['bearing'] <= 360

    def test_calculate_initial_view_state_with_multiple_hexes(self):
        """Test view state calculation with multiple hexagons."""
        try:
            import pydeck  # noqa: F401
            import mapclassify  # noqa: F401
        except ImportError:
            pytest.skip("Visualization dependencies not installed")

        from h3_toolkit.visualization import _calculate_initial_view_state

        # Multiple hex IDs covering different areas
        hex_ids = [
            '8c4ba0a4e15ffff',
            '8c4ba0a4e14ffff',
            '8c4ba0a4e13ffff',
        ]

        result = _calculate_initial_view_state(hex_ids)

        # Should return valid result
        assert isinstance(result, dict)
        assert all(k in result for k in ['longitude', 'latitude', 'zoom', 'pitch', 'bearing'])
```

- [ ] **Step 2: 運行測試確認失敗**

Run: `pytest tests/test_visualization.py::TestBoundaryCalculation -v`

Expected: FAIL with "function object is not subscriptable" or similar.

- [ ] **Step 3: 實現邊界計算函數**

在 `h3_toolkit/visualization.py` 中替換 `_calculate_initial_view_state` 的實現：

```python
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
```

- [ ] **Step 4: 運行測試確認通過**

Run: `pytest tests/test_visualization.py::TestBoundaryCalculation -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add h3_toolkit/visualization.py tests/test_visualization.py
git commit -m "feat: implement map boundary and view state calculation"
```

---

## Task 5: 實現顏色設置邏輯

**Files:**
- Modify: `tests/test_visualization.py`
- Modify: `h3_toolkit/visualization.py`

- [ ] **Step 1: 添加顏色設置測試**

在 `tests/test_visualization.py` 的 `TestColorSetting` 類中：

```python
class TestColorSetting:
    """Test color setting logic."""

    def test_set_color_returns_dataframe_with_color_column(self):
        """Test that _set_color adds a 'color' column."""
        try:
            import pydeck  # noqa: F401
            import mapclassify  # noqa: F401
        except ImportError:
            pytest.skip("Visualization dependencies not installed")

        from h3_toolkit.visualization import _set_color

        data = pl.DataFrame({
            'hex_id': ['8c4ba0a4e15ffff', '8c4ba0a4e14ffff', '8c4ba0a4e13ffff'],
            'value': [10.5, 20.3, 15.7]
        })

        result = _set_color(data, 'value', classifier='NaturalBreaks', k=3, cmap='Oranges')

        assert 'color' in result.columns
        assert len(result) == len(data)

        # Check that colors are RGBA tuples
        color_col = result['color']
        for color in color_col:
            assert len(color) == 4  # RGBA
            assert all(0 <= c <= 255 for c in color)  # Valid RGB values

    def test_set_color_with_different_classifiers(self):
        """Test _set_color with different mapclassify methods."""
        try:
            import pydeck  # noqa: F401
            import mapclassify  # noqa: F401
        except ImportError:
            pytest.skip("Visualization dependencies not installed")

        from h3_toolkit.visualization import _set_color

        data = pl.DataFrame({
            'value': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
        })

        classifiers = ['NaturalBreaks', 'Quantiles', 'EqualInterval']

        for clf in classifiers:
            result = _set_color(data, 'value', classifier=clf, k=3, cmap='Oranges')
            assert 'color' in result.columns
            assert len(result) == len(data)

    def test_set_color_with_different_colormaps(self):
        """Test _set_color with different matplotlib colormaps."""
        try:
            import pydeck  # noqa: F401
            import mapclassify  # noqa: F401
        except ImportError:
            pytest.skip("Visualization dependencies not installed")

        from h3_toolkit.visualization import _set_color

        data = pl.DataFrame({
            'value': [1.0, 2.0, 3.0, 4.0, 5.0]
        })

        cmaps = ['Oranges', 'Viridis', 'RdYlGn', 'Blues']

        for cmap in cmaps:
            result = _set_color(data, 'value', k=3, cmap=cmap)
            assert 'color' in result.columns
```

- [ ] **Step 2: 運行測試確認失敗**

Run: `pytest tests/test_visualization.py::TestColorSetting -v`

Expected: FAIL with "NoneType" or function not implemented.

- [ ] **Step 3: 實現顏色設置函數**

在 `h3_toolkit/visualization.py` 中替換 `_set_color` 的實現：

```python
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
    bins = classification.classify(values)

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
```

- [ ] **Step 4: 運行測試確認通過**

Run: `pytest tests/test_visualization.py::TestColorSetting -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add h3_toolkit/visualization.py tests/test_visualization.py
git commit -m "feat: implement color setting with mapclassify and matplotlib"
```

---

## Task 6: 實現核心 show_h3 函數

**Files:**
- Modify: `tests/test_visualization.py`
- Modify: `h3_toolkit/visualization.py`

- [ ] **Step 1: 添加 show_h3 函數測試**

在 `tests/test_visualization.py` 的 `TestShow` 類中：

```python
class TestShow:
    """Test show_h3 function."""

    def test_show_h3_returns_deck_when_no_save_path(self):
        """Test that show_h3 returns a pdk.Deck object."""
        try:
            import pydeck as pdk
            import mapclassify  # noqa: F401
        except ImportError:
            pytest.skip("Visualization dependencies not installed")

        from h3_toolkit.visualization import show_h3

        data = pl.DataFrame({
            'hex_id': ['8c4ba0a4e15ffff', '8c4ba0a4e14ffff'],
            'value': [10.5, 20.3]
        })

        result = show_h3(data, 'value')

        assert isinstance(result, pdk.Deck)

    def test_show_h3_saves_html_when_save_path_provided(self, tmp_path):
        """Test that show_h3 saves HTML file when save_to is provided."""
        try:
            import pydeck  # noqa: F401
            import mapclassify  # noqa: F401
        except ImportError:
            pytest.skip("Visualization dependencies not installed")

        from h3_toolkit.visualization import show_h3

        data = pl.DataFrame({
            'hex_id': ['8c4ba0a4e15ffff', '8c4ba0a4e14ffff'],
            'value': [10.5, 20.3]
        })

        output_file = tmp_path / "test_map.html"

        show_h3(data, 'value', save_to=str(output_file))

        assert output_file.exists()
        assert output_file.stat().st_size > 0

    def test_show_h3_with_custom_parameters(self):
        """Test show_h3 with custom classifier, k, and cmap."""
        try:
            import pydeck as pdk
            import mapclassify  # noqa: F401
        except ImportError:
            pytest.skip("Visualization dependencies not installed")

        from h3_toolkit.visualization import show_h3

        data = pl.DataFrame({
            'hex_id': ['8c4ba0a4e15ffff', '8c4ba0a4e14ffff', '8c4ba0a4e13ffff'],
            'value': [10.5, 20.3, 15.7]
        })

        result = show_h3(
            data,
            'value',
            classifier='Quantiles',
            k=3,
            cmap='Viridis'
        )

        assert isinstance(result, pdk.Deck)

    def test_show_h3_raises_on_missing_column(self):
        """Test that show_h3 raises ValueError for missing column."""
        try:
            import pydeck  # noqa: F401
            import mapclassify  # noqa: F401
        except ImportError:
            pytest.skip("Visualization dependencies not installed")

        from h3_toolkit.visualization import show_h3

        data = pl.DataFrame({
            'hex_id': ['8c4ba0a4e15ffff'],
            'other_col': [10.5]
        })

        with pytest.raises(ValueError, match="not found"):
            show_h3(data, 'value')
```

- [ ] **Step 2: 運行測試確認失敗**

Run: `pytest tests/test_visualization.py::TestShow -v`

Expected: FAIL with "None is not a Deck" or similar.

- [ ] **Step 3: 實現 show_h3 函數**

在 `h3_toolkit/visualization.py` 中替換 `show_h3` 的實現：

```python
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
```

- [ ] **Step 4: 運行測試確認通過**

Run: `pytest tests/test_visualization.py::TestShow -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add h3_toolkit/visualization.py tests/test_visualization.py
git commit -m "feat: implement show_h3 core visualization function"
```

---

## Task 7: 在 H3Toolkit 中添加 show() 方法

**Files:**
- Modify: `h3_toolkit/core.py`
- Create: `tests/test_core_visualization.py`

- [ ] **Step 1: 創建針對 show() 方法的測試**

```python
# tests/test_core_visualization.py
"""Tests for H3Toolkit.show() method."""

import pytest
import polars as pl
from h3_toolkit import H3Toolkit


class TestH3ToolkitShow:
    """Test H3Toolkit.show() method."""

    def test_show_with_result_data(self):
        """Test show() with data already in toolkit.result."""
        try:
            import pydeck  # noqa: F401
            import mapclassify  # noqa: F401
        except ImportError:
            pytest.skip("Visualization dependencies not installed")

        toolkit = H3Toolkit()

        # Set up result data
        toolkit.result = pl.DataFrame({
            'hex_id': ['8c4ba0a4e15ffff', '8c4ba0a4e14ffff'],
            'value': [10.5, 20.3]
        })

        # show() should return toolkit (for chaining)
        result = toolkit.show('value')

        assert result is toolkit

    def test_show_raises_without_result(self):
        """Test that show() raises ValueError if result is empty."""
        try:
            import pydeck  # noqa: F401
            import mapclassify  # noqa: F401
        except ImportError:
            pytest.skip("Visualization dependencies not installed")

        toolkit = H3Toolkit()

        with pytest.raises(ValueError, match="No data to visualize"):
            toolkit.show('value')

    def test_show_with_custom_parameters(self):
        """Test show() with custom classifier, k, and cmap."""
        try:
            import pydeck  # noqa: F401
            import mapclassify  # noqa: F401
        except ImportError:
            pytest.skip("Visualization dependencies not installed")

        toolkit = H3Toolkit()
        toolkit.result = pl.DataFrame({
            'hex_id': ['8c4ba0a4e15ffff', '8c4ba0a4e14ffff', '8c4ba0a4e13ffff'],
            'value': [10.5, 20.3, 15.7]
        })

        result = toolkit.show(
            'value',
            classifier='Quantiles',
            k=3,
            cmap='Viridis'
        )

        assert result is toolkit

    def test_show_save_html(self, tmp_path):
        """Test show() saving to HTML file."""
        try:
            import pydeck  # noqa: F401
            import mapclassify  # noqa: F401
        except ImportError:
            pytest.skip("Visualization dependencies not installed")

        toolkit = H3Toolkit()
        toolkit.result = pl.DataFrame({
            'hex_id': ['8c4ba0a4e15ffff'],
            'value': [10.5]
        })

        output_file = tmp_path / "map.html"

        toolkit.show('value', save_to=str(output_file))

        assert output_file.exists()
```

- [ ] **Step 2: 運行測試確認失敗**

Run: `pytest tests/test_core_visualization.py -v`

Expected: FAIL with "H3Toolkit has no attribute 'show'".

- [ ] **Step 3: 在 core.py 中添加 show() 方法**

在 `h3_toolkit/core.py` 的 `H3Toolkit` 類末尾添加：

```python
    def show(
        self,
        target_col: str,
        h3_col: str = 'hex_id',
        classifier: str = 'NaturalBreaks',
        k: int = 5,
        cmap: str = 'Oranges',
        save_to: str | None = None,
        **pydeck_kwargs
    ) -> 'H3Toolkit':
        """
        Visualize H3 hexagon data layer.

        Automatically calculates map boundaries and initial view state based on data extent.

        Args:
            target_col (str):
                Column name to visualize using color classification.
            h3_col (str, optional):
                H3 hexagon ID column name. Defaults to 'hex_id'.
            classifier (str, optional):
                mapclassify classification method name. Defaults to 'NaturalBreaks'.
                See: https://pysal.org/mapclassify/api.html
            k (int, optional):
                Number of classes for classification. Defaults to 5.
            cmap (str, optional):
                matplotlib colormap name. Defaults to 'Oranges'.
                See: https://matplotlib.org/stable/users/explain/colors/colormaps.html
            save_to (str | None, optional):
                Path to save HTML file. If None, displays in Jupyter. Defaults to None.
            **pydeck_kwargs:
                Additional arguments for pdk.Deck (map_style, pitch, bearing, etc.).

        Returns:
            H3Toolkit: Returns self for method chaining.

        Raises:
            ValueError: If result is empty or column not found.
            ImportError: If visualization dependencies are missing.

        Examples:
            >>> toolkit = H3Toolkit()
            >>> toolkit.process_from_vector(geo_df)
            >>> toolkit.fetch_from_hbase(...)
            >>> toolkit.show('population')

            >>> toolkit.show('population', classifier='Quantiles', k=6, cmap='Viridis')

            >>> toolkit.show('population', save_to='map.html')
        """
        if self.result.is_empty():
            raise ValueError(
                "No data to visualize. Please process data first "
                "using process_from_vector(), process_from_raster(), or process_from_h3()."
            )

        from .visualization import show_h3

        # Call visualization function
        show_h3(
            data=self.result,
            target_col=target_col,
            h3_col=h3_col,
            classifier=classifier,
            k=k,
            cmap=cmap,
            save_to=save_to,
            **pydeck_kwargs
        )

        return self
```

- [ ] **Step 4: 運行測試確認通過**

Run: `pytest tests/test_core_visualization.py -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add h3_toolkit/core.py tests/test_core_visualization.py
git commit -m "feat: add show() method to H3Toolkit class"
```

---

## Task 8: 運行完整測試套件

**Files:**
- All test files

- [ ] **Step 1: 運行所有可視化測試**

Run: `pytest tests/test_visualization.py tests/test_core_visualization.py -v`

Expected: All tests pass.

- [ ] **Step 2: 運行整個測試套件確認沒有回歸**

Run: `pytest tests/ -v --tb=short`

Expected: All existing tests pass, no regressions.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "test: verify all visualization tests pass"
```

---

## Task 9: 驗證功能完整性和文檔

**Files:**
- `h3_toolkit/__init__.py` (optional)
- Documentation files

- [ ] **Step 1: 驗證 visualization 模組可以獨立導入**

Run: `python -c "from h3_toolkit.visualization import show_h3; print('✓ show_h3 imported successfully')"`

Expected: Output "✓ show_h3 imported successfully"

- [ ] **Step 2: 驗證 H3Toolkit.show() 方法可用**

Run: `python -c "from h3_toolkit import H3Toolkit; t = H3Toolkit(); print('✓ show method exists:', hasattr(t, 'show'))"`

Expected: Output "✓ show method exists: True"

- [ ] **Step 3: 驗證依賴檢查在缺少依賴時的表現**

（此步驟只在移除可選依賴後執行，正常開發中跳過）

- [ ] **Step 4: 最終提交**

```bash
git add -A
git commit -m "docs: complete visualization integration with tests and examples"
```

---

## Summary

**Completed Features:**
- ✅ 可選依賴配置在 pyproject.toml
- ✅ visualization.py 模組實現，包含：
  - 雙層依賴檢查（import 時 + 調用時）
  - 地圖邊界自動計算
  - 支援多種分類器和顏色映射的著色
  - show_h3() 獨立函數
- ✅ H3Toolkit.show() 方法
- ✅ 完整的測試套件
- ✅ 支援 Jupyter 展示和 HTML 保存

**Testing:**
- 單元測試覆蓋所有核心功能
- 集成測試驗證 H3Toolkit 的 show() 方法
- 依賴檢查測試確保清晰的錯誤提示

**Files Changed:**
- `pyproject.toml` - 添加可選依賴組
- `h3_toolkit/visualization.py` - 新建，核心可視化邏輯
- `h3_toolkit/core.py` - 添加 show() 方法
- `tests/test_visualization.py` - 新建，可視化模組測試
- `tests/test_core_visualization.py` - 新建，H3Toolkit.show() 測試

