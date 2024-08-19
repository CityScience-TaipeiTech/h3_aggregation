from __future__ import annotations

import logging

import geopandas as gpd
import h3ronpy.polars  # noqa: F401
import polars as pl

from .aggregation import AggregationStrategy
from .exceptions import (
    ColumnNotFoundError,
    HBaseConnectionError,
    InputDataTypeError,
    ResolutionRangeError,
)
from .hbase_tools import HBaseClient
from .utils import geom_to_wkb, wkb_to_cells


class H3Toolkit:
    def __init__(self, client=None):
        self.client:HBaseClient = client
        self.aggregation_strategies = {}
        self.logger = logging.getLogger(__name__)  # 這是什麼意思?
        self.source_resolution:int = None
        self.target_resolution:int = None
        self.raw_data = None
        # self.h3_data = None
        self.result = None

    def set_aggregation_strategy(
        self,
        strategies:dict[str | list[str], AggregationStrategy]
    ) -> H3Toolkit:
        """
        Set the aggregation strategies for the designated columns(properties) in the input data

        Args:
            strategies (dict[str | list[str], AggregationStrategy]):
                A dictionary with column names as keys and AggregationStrategy objects as values
        """
        # Check the AggregationStrategy object is valid
        if not all(isinstance(v, AggregationStrategy) for v in strategies.values()):
            raise ValueError("""
                The input strategies must be a dictionary with AggregationStrategy objects,
                please import AggregationStrategy from h3_toolkit.aggregation
            """)
        self.aggregation_strategies = strategies
        return self

    def _apply_strategy(self, df:pl.DataFrame)->pl.DataFrame:
        """_summary_

        Args:
            df (pl.DataFrame): use polar pipe function to apply the aggregation strategy

        Returns:
            pl.DataFrame: _description_
        """

        # it's possible that the aggregation_strategies is empty
        if self.aggregation_strategies:
            for col, strategy in self.aggregation_strategies.items():
                cols = [col] if isinstance(col, str) else col
                # TODO: 可能會有問題，寫測試的時候注意一下
                df = strategy.apply(df, target_cols=cols)
        return df

    # TODO: from raster to h3
    def process_from_raster():
        pass

    # TODO: 命名方式
    def process_from_geometry(
        self,
        data_with_geom: pl.DataFrame | gpd.GeoDataFrame,
        resolution:int = 12,
        geometry_col:str = 'geometry'
    ) -> H3Toolkit:
        """
        Process the input data with geo-spatial information and output the data with H3 cells in the specific resolution.

        Args:
            data_with_geom (pl.DataFrame | gpd.GeoDataFrame): The input data with geo-spatial information
            resolution (int, optional): The resolution of the H3 cells. Defaults to 12.
            geometry_col (str, optional): The name of the geometry column. Defaults to 'geometry'.

        Raises:
            ResolutionRangeError: The resolution must be an integer from 0 to 15
            InputDataTypeError: The input data must be either a GeoDataFrame or a DataFrame with geo-spatial information.
            ColumnNotFoundError: The column name set in the aggregation strategies is not found in the input data
        """  # noqa: E501

        # check resolution is from 0 to 15
        if resolution not in range(0, 16):
            raise ResolutionRangeError("""
                The resolution must be an integer from 0 to 15, please refer to the H3 documentation
            """)
        else:
            self.source_resolution = resolution

        # check the input data type
        if isinstance(data_with_geom, gpd.GeoDataFrame):
            self.raw_data = geom_to_wkb(data_with_geom, geometry_col)
        elif isinstance(data_with_geom, pl.DataFrame):
            self.raw_data = data_with_geom
        else:
            raise InputDataTypeError("""
                The input data must be either a GeoDataFrame or a DataFrame with geo-spatial
                information.
            """)

        selected_cols = set()
        # check the column names set in the aggregation strategies are part of the input data
        if self.aggregation_strategies:
            for cols in self.aggregation_strategies.keys():
                cols = [cols] if isinstance(cols, str) else cols
                selected_cols.update(cols)
                missing_cols = [col for col in cols if col not in self.raw_data.columns]
                if missing_cols:
                    raise ColumnNotFoundError(f"""
                        The column '{', '.join(missing_cols)}' not found in the input data,
                        please use `set_aggregation_strategy()` to reset the valid col name.
                    """)

        logging.info(f"Start converting data to h3 cells in resolution {self.source_resolution}")

        self.result = (
            self.raw_data
            .lazy()
            .fill_nan(0)
            .pipe(wkb_to_cells, self.source_resolution, geometry_col)
            .pipe(self._apply_strategy) # apply the aggregation strategy
            .select(
                # Convert the cell(unit64) to string
                pl.col('cell').h3.cells_to_string().alias('hex_id'),
                # only select the columns set in the aggregation strategies
                # TODO: 可能有問題，寫測試的時候注意一下
                pl.col(selected_cols)
            )
            .collect(streaming=True)
        )

        logging.info(self.result.head(5))
        logging.info(f"Successfully converting data to h3 cells in resolution \
                     {self.source_resolution}")

        return self

    def set_hbase_client(self, client:HBaseClient) -> H3Toolkit:
        """
        Set the HBase client for fetching and sending data to HBase.

        Args:
            client (HBaseClient): The HBase client object
        """
        self.client = client
        return self

    def fetch_from_hbase(
        self,
        table_name:str,
        column_family:str,
        column_qualifier: list[str],
    ) -> H3Toolkit:
        """
        Args:
            table_name (str): The name of the table in HBase, ie: 'res12_pre_data'
            column_family (str): The name of the column family in HBase, ie: 'demographic'
            column_qualifier (list[str]): The list of column qualifiers in HBase, ie: ['p_cnt']
        """

        if not self.result:
            raise ValueError("Please provide the h3 index first \
                             before fetching data from HBase.")
        # TODO: check the HBase client is set 可能要寫set_client
        if self.client:
            self.result = self.client.fetch_data(
                table_name=table_name,
                column_family=column_family,
                column_qualifier=column_qualifier,
                rowkeys=self.result['hex_id'].to_list(),
            )
        else:
            raise HBaseConnectionError("The HBase client didn't set, use `set_hbase_client()` \
                                        to set the HBase client before fetching data from hbase.")

        return self


    def send_to_hbase(
        self,
        table_name:str,
        column_family:str,
        column_qualifier: list[str],
        h3_col:str = 'hex_id',
        timestamp=None,
    ) -> H3Toolkit:
        """
        Args:
            table_name (str): The name of the table in HBase, ie: 'res12_pre_data'
            column_family (str): The name of the column family in HBase, ie: 'demographic'
            column_qualifier (list[str]): The list of column qualifiers in HBase, ie: ['p_cnt']
            h3_col (str): The name of the column that contains the H3 index, default is 'hex_id'
            timestamp (int): The timestamp of the data, default is None, using the current time
        """

        if not self.result:
            raise ValueError("Please process the data first \
                             before sending data to HBase.")

        if self.client:
            self.client.send_data(
                data = self.result,
                table_name = table_name,
                column_family = column_family,
                column_qualifier = column_qualifier,
                rowkey_col = h3_col,
                timestamp = timestamp
            )
        else:
            raise HBaseConnectionError("The HBase client didn't set, use `set_hbase_client()` \
                                        to set the HBase client before sending data to hbase.")

        return self

    def process_from_h3(
        self,
        data_with_h3:pl.DataFrame = None,
        resolution:int = 7,
        h3_col:str = 'hex_id'
    ) -> H3Toolkit:

        # check resolution is from 0 to 15
        if resolution not in range(0, 16):
            raise ResolutionRangeError("""
                The resolution must be an integer from 0 to 15, please refer to the H3 documentation
            """)
        else:
            self.target_resolution = resolution

        # TODO: target resolution 不能大於 source resolution？

        # check the column names set in the aggregation strategies are part of the input data
        if self.aggregation_strategies:
            for cols in self.aggregation_strategies.keys():
                cols = [cols] if isinstance(cols, str) else cols
                # selected_cols.update(cols)
                missing_cols = [col for col in cols if col not in self.raw_data.columns]
                if missing_cols:
                    raise ColumnNotFoundError(f"""
                        The column '{', '.join(missing_cols)}' not found in the input data,
                        please use `set_aggregation_strategy()` to reset the valid col name.
                    """)

        # if data_with_h3 is provided, use the data_with_h3
        if data_with_h3 is not None:
            self.result = data_with_h3

        self.result = (
            self.result
            .lazy()
            .with_columns(
                # 根據h3_col做resolution的轉換
                pl.col(h3_col)
                .h3.cells_parse()
                .h3.change_resolution(self.target_resolution)
                .alias('cell')
            )
            .pipe(self._apply_strategy)
            .drop(h3_col)
            .select(
                pl.col('cell')
                    .h3.cells_to_string()
                    .alias(h3_col),
                pl.exclude('cell')
                )

            # TODO: 評估一下
            .unique(subset=[h3_col])
            .collect(streaming=True)
        )

        return self

    # TODO: 把h3 index 轉換成geometry
    def h3_to_geom():
        pass

    def get_result(self) -> pl.DataFrame:
        """Get the result of the data processing.
        Returns:
            pl.DataFrame: The processed data with H3 cells in the specific resolution.
        """
        return self.result
