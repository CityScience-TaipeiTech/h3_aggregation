"""Tests for visualization module."""

import pytest
import polars as pl


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

    def test_check_dependencies_function_exists(self):
        """Test that _check_dependencies function exists."""
        try:
            from h3_toolkit.visualization import _check_dependencies
            assert callable(_check_dependencies)
        except ImportError:
            pytest.skip("Visualization dependencies not installed")

    def test_check_dependencies_raises_without_pydeck(self, monkeypatch):
        """Test that _check_dependencies raises if pydeck is missing."""
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

    def test_show_h3_raises_without_deps(self):
        """Test that show_h3 raises ImportError if deps are missing."""
        try:
            import pydeck  # noqa: F401
            import mapclassify  # noqa: F401
            pytest.skip("Visualization dependencies are installed")
        except ImportError:
            from h3_toolkit.visualization import show_h3

            data = pl.DataFrame({'hex_id': ['test'], 'value': [1.0]})

            with pytest.raises(ImportError, match="visualization dependencies"):
                show_h3(data, 'value')


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

        cmaps = ['Oranges', 'viridis', 'RdYlGn', 'Blues']

        for cmap in cmaps:
            result = _set_color(data, 'value', k=3, cmap=cmap)
            assert 'color' in result.columns


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
            cmap='viridis'
        )

        assert isinstance(result, pdk.Deck)

class TestCalculateInitialViewState:
    """Test _calculate_initial_view_state returns valid bounds."""

    def test_returns_valid_bounds_for_valid_hex_ids(self):
        """Test that min_lon, min_lat, max_lon, max_lat are finite and in valid ranges."""
        from h3_toolkit.visualization import _calculate_initial_view_state

        # Valid H3 resolution-7 cell IDs in Taipei area (generated via h3ronpy)
        hex_ids = ['874ba0a51ffffff', '874ba0a52ffffff', '874ba0a53ffffff']
        result = _calculate_initial_view_state(hex_ids)

        assert result['longitude'] != 0 or result['latitude'] != 0, \
            "View state should not be the default fallback (0, 0)"
        assert -180 <= result['longitude'] <= 180
        assert -90 <= result['latitude'] <= 90
        assert 0 <= result['zoom'] <= 20
        # Verify the center is roughly in Taipei (lon ~121.5, lat ~25.0)
        assert 120 <= result['longitude'] <= 123
        assert 24 <= result['latitude'] <= 26

    def test_returns_default_for_empty_list(self):
        """Test that empty hex_ids returns default world view."""
        from h3_toolkit.visualization import _calculate_initial_view_state

        result = _calculate_initial_view_state([])

        assert result == {"longitude": 0, "latitude": 0, "zoom": 2, "pitch": 0, "bearing": 0}

    def test_returns_default_for_invalid_hex_ids(self):
        """Test that all-invalid hex_ids returns default view."""
        from h3_toolkit.visualization import _calculate_initial_view_state

        result = _calculate_initial_view_state(["invalid_hex", "not_a_cell"])

        assert result == {"longitude": 0, "latitude": 0, "zoom": 2, "pitch": 0, "bearing": 0}


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
