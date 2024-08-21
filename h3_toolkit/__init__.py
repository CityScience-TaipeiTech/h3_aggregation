import polars as pl
from h3ronpy import ContainmentMode as Cont
from h3ronpy.polars.vector import cells_to_wkb_polygons, wkb_to_cells
from h3ronpy.polars.raster import raster_to_dataframe
from shapely import from_wkb

from .aggregation import Centroid, Count, SplitEqually
from .hbase import HBaseClient

__all__ = [
    'SplitEqually',
    'Centroid',
    'Count',
    'HBaseClient',
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
