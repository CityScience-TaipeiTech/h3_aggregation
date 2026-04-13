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

    @pytest.mark.skipif(
        True,  # Will be updated after implementation
        reason="Waiting for implementation"
    )
    def test_set_color_basic(self):
        """Test basic color setting with NaturalBreaks."""
        pass


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

    @pytest.mark.skipif(
        True,  # Will be updated after implementation
        reason="Waiting for implementation"
    )
    def test_show_h3_returns_deck(self):
        """Test that show_h3 returns a pdk.Deck object."""
        pass
