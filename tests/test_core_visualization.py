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
            cmap='viridis'
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
