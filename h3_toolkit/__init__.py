"""


The H3Toolkit is a wrapper around h3ronpy and polars, designed to simplify the aggregation of H3 data across different resolutions.

Inspired by the user-friendly interface of libraries like pandas and polars, H3Toolkit allows you to chain methods together, making your data processing pipeline more readable and maintainable. By leveraging this approach, you can perform complex operations on H3 data with ease.

With H3Toolkit, you only need to create an instance of the H3Toolkit class and chain the various processing methods to it. Additionally, the toolkit provides support for extending polars functionality by allowing you to create custom aggregation functions.

""" # noqa


import polars as pl
from h3ronpy import ContainmentMode as Cont
from h3ronpy.polars.raster import raster_to_dataframe
from h3ronpy.polars.vector import cells_to_wkb_polygons, wkb_to_cells
from shapely import from_wkb

from .core import H3Toolkit

__all__ = [
    'H3Toolkit',
]

@pl.api.register_expr_namespace('custom')
class CustomExpr:
    def __init__(self, expr: pl.Expr):
        self._expr = expr
    def custom_wkb_to_cells(self,
                            resolution:int,
                            containment_mode:Cont=Cont.ContainsCentroid,
                            compact:bool=False,
                            flatten:bool=False
                            )->pl.Expr:
        return (
            self._expr.map_batches(
                lambda s: wkb_to_cells(s, resolution, containment_mode, compact, flatten)
            )
        )

    def custom_cells_to_wkb_polygons(self,
                                     radians:bool=False,
                                     link_cells:bool=False
                                     )->pl.Expr:
        return (
            self._expr.map_batches(
                lambda s: cells_to_wkb_polygons(s, radians, link_cells)
            )
        )

    def custom_from_wkb(self)->pl.Expr:
        return (
            self._expr.map_batches(
                lambda s: from_wkb(s)
            )
        )

    def custom_raster_to_dataframe(self,
                                   in_raster,
                                   transform,
                                   h3_resolution:int,
                                   no_data_value:int | None=None,
                                   axis_order:str='yx',
                                   compact:bool=False,
                                   )->pl.Expr:
        return (
            self._expr.map_batches(
                lambda s: raster_to_dataframe(
                    s, in_raster, transform, h3_resolution,
                    no_data_value, axis_order, compact)
            )
        )
