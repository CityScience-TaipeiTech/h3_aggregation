from __future__ import annotations

import logging

import geopandas as gpd
import h3ronpy.polars  # noqa: F401
import polars as pl
from h3ronpy.polars.raster import raster_to_dataframe

from .aggregation import AggregationStrategy
from .exceptions import (
    ColumnNotFoundError,
    HBaseConnectionError,
    InputDataTypeError,
    ResolutionRangeError,
)
from .hbase import HBaseClient
from .utils import cell_to_geom, geom_to_wkb, wkb_to_cells


class H3Toolkit:
    def __init__(self):
        self.client:HBaseClient = None
        self.aggregation_strategies = {}
        # self.logger = logging.getLogger(__name__)  # 這是什麼意思?
        self.source_resolution:int = None
        self.target_resolution:int = None
        self.raw_data = None
        self.result:pl.DataFrame = pl.DataFrame()

    def set_aggregation_strategy(
        self,
        strategies:dict[str | tuple[str], AggregationStrategy]
    ) -> H3Toolkit:
        """
        Set the aggregation strategies for the designated columns(properties) in the input data. \

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

        return_df = df.select(pl.col('cell'))
        # it's possible that the aggregation_strategies is empty
        if self.aggregation_strategies:
            for col, strategy in self.aggregation_strategies.items():
                cols = [col] if isinstance(col, str) else col
                # let eveny df is a raw df as the input
                applied_df = strategy.apply(df, target_cols=cols)
                return_df = return_df.join(applied_df, on='cell', how='left')

        return return_df

    def process_from_vector(
        self,
        data: pl.DataFrame | gpd.GeoDataFrame,
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
        if isinstance(data, gpd.GeoDataFrame):
            self.raw_data = geom_to_wkb(data, geometry_col)
        elif isinstance(data, pl.DataFrame):
            self.raw_data = data
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
                # pl.col(selected_cols)
                pl.all().exclude(['cell', geometry_col])
            )
            .collect(streaming=True)
        )

        # Potential hex_id loss: check if there is any null value in the hex_id column

        # resolution選太大就會有null！
        if self.result.select(pl.col('hex_id').is_null().any()).item():
            logging.warning("potential hex_id loss: \
                            please select the higher resolution and with `process_from_h3()`")

        logging.info(self.result.head(5))
        logging.info(f"Successfully converting data to h3 cells in resolution \
                     {self.source_resolution}")

        return self

    def process_from_raster(
        self,
        data: list[list[int | float]],
        transform,
        resolution:int = 12,
        nodata_value:float | int | None = None,
        return_value_name:str = 'value',
    ) -> H3Toolkit:
        """
        Processes raster data and converts it into H3 indexes.

        Args:
            data (list[list[int | float]]): A 2D list representing the raster data, where each element
                can be an integer or float.
            transform: Transformation matrix or function to apply to the raster data.
                It is used for spatial referencing of the raster.
            resolution (int, optional): The H3 resolution level to use for the conversion.
                It must be an integer between 0 and 15. Defaults to 12.
            nodata_value (float | int | None, optional): The value representing "no data" in the raster.
                If provided, this value will be ignored in processing. Defaults to None.
            return_value_name (str, optional): The name to use for the value column in the resulting DataFrame.
                Defaults to 'value'.

        Raises:
            ResolutionRangeError: If the resolution is not an integer between 0 and 15.

        Returns:
            H3Toolkit: The updated instance of the H3Toolkit with the processed data.

        """ # noqa: E501

        # check resolution is from 0 to 15
        if resolution not in range(0, 16):
            raise ResolutionRangeError("""
                The resolution must be an integer from 0 to 15, please refer to the H3 documentation
            """)
        else:
            self.source_resolution = resolution

        self.raw_data = raster_to_dataframe(
            in_raster = data,
            transform = transform,
            h3_resolution = resolution,
            nodata_value = nodata_value,
            compact = False,
            # geo = False,
        )

        self.result = (
            self.raw_data
            .lazy()
            .select(
                pl.col('cell')
                .h3.cells_to_string().alias('hex_id'),
                pl.col('value').alias(return_value_name)
            )
            .collect(streaming=True)
        )

        return self

    def process_from_h3(
        self,
        data:pl.DataFrame | None = None,
        target_resolution:int = 7,
        source_resolution:int = None,
        h3_col:str = 'hex_id'
    ) -> H3Toolkit:
        """
        Processes the input H3 indexed data by converting the resolution of H3 cells and
        applying aggregation strategies to the data. The function validates the source and target
        resolutions, ensures the presence of necessary columns, and transforms H3 indexes to a
        target resolution.

        Args:
            data (pl.DataFrame, optional):
                The input DataFrame containing H3 indexes in the specified `h3_col` column.
                If not provided, the method will use previously processed data from self.result.
                Defaults to None.
            target_resolution (int, optional):
                The desired H3 resolution for the transformation.
                Must be between 0 and 15, with the requirement that it be less than the `source_resolution`.
                Defaults to 7.
            source_resolution (int, optional):
                The source H3 resolution of the input data. If not provided,
                the function will use `self.source_resolution`, if previously set from `process_from_vector()`
                or `process_from_raster()`.
                Defaults to None.
            h3_col (str, optional): The name of the column containing H3 index values in the input data.
                Defaults to 'hex_id'.

        Returns:
            H3Toolkit: Returns the current instance of the H3Toolkit class, allowing for method chaining.

        Raises:
            ResolutionRangeError:
                If the source or target resolution is outside the valid range (0 to 15),
                or if the target resolution is greater than or equal to the source resolution.
            ColumnNotFoundError:
                If the specified H3 column (`h3_col`) is not present in the input DataFrame,
                or if any columns required for the aggregation strategies are missing.

        Example:
            >>> from h3_toolkit import H3Toolkit
            >>> toolkit = H3Toolkit()
            >>> df = pl.DataFrame({"hex_id": [...], "value": [...]})
            >>> toolkit.set_aggregation_strategy({'value': AggregationStrategy()})
            >>> toolkit.process_from_h3(data=df, target_resolution=6, source_resolution=12)

        Note:
            - The source resolution must be higher than the target resolution to allow for downscaling.
            - The aggregation strategies are applied to the transformed H3 data.
            - The function ensures no duplicate H3 cells exist in the final result.

        """ # noqa: E501

        # if data_with_h3 is provided, use the data_with_h3
        if data is not None:
            self.result = data

        # 如果前面執行過process_from_geometry，就會有source_resolution
        if self.source_resolution:
            source_resolution = self.source_resolution

        # check resolution is from 0 to 15
        if source_resolution  not in range(0, 16) or \
            target_resolution not in range(0, 16):
            raise ResolutionRangeError("""
                The resolution must be an integer from 0 to 15, please refer to the H3 documentation
            """)
        elif source_resolution < target_resolution:
            raise ResolutionRangeError("""
                The target resolution must be lower than the source resolution
                ie: source_resolution: 12, target_resolution: 7
            """)
        else:
            self.target_resolution = target_resolution
            self.source_resolution = source_resolution

        # 判斷h3_col是否在data裡面
        if h3_col not in self.result.columns:
            raise ColumnNotFoundError(f"Column '{h3_col}' not found in the input DataFrame")

        # TODO: target resolution 不能大於 source resolution？

        # check the column names set in the aggregation strategies are part of the input data
        if self.aggregation_strategies:
            for cols in self.aggregation_strategies.keys():
                cols = [cols] if isinstance(cols, str) else cols
                # selected_cols.update(cols)
                missing_cols = [col for col in cols if col not in self.result.columns]
                if missing_cols:
                    raise ColumnNotFoundError(f"""
                        The column '{', '.join(missing_cols)}' not found in the input data,
                        please use `set_aggregation_strategy()` to reset the valid col name.
                    """)

        self.result = (
            self.result
            .lazy()
            .drop_nulls(subset=[h3_col])# 沒有h3 index的row就直接刪掉
            .with_columns(
                # 根據h3_col做resolution的轉換
                pl.col(h3_col)
                .h3.cells_parse()
                .h3.change_resolution(self.target_resolution)
                .alias('cell')
            )
            .pipe(self._apply_strategy)
            .select(
                pl.all().exclude(h3_col)
            )
            .select(
                pl.col('cell')
                    .h3.cells_to_string()
                    .alias(h3_col),
                pl.exclude('cell')
                )

            # can't have duplicate hex_id in the result
            .unique(subset=[h3_col])
            .collect(streaming=True)
        )

        return self

    def set_hbase_client(self, client:HBaseClient) -> H3Toolkit:
        """
        Sets the HBase client for interacting with HBase.

        This method sets the HBase client that will be used to fetch and send data to/from HBase.
        The client must be set before any HBase-related operations can be performed, such as
        `fetch_from_hbase()` or `send_to_hbase()` data.

        Args:
            client (HBaseClient):
                The HBase client object used to interact with HBase.
                use `from h3_toolkit.hbase import HBaseClient` to initialize the client.

        Returns:
            H3Toolkit:
                Returns the instance of the `H3Toolkit` class with the HBase client set.

        Examples:
            To set an HBase client for further operations:
            >>> from h3_toolkit.hbase import HBaseClient
            >>> hbase_client = HBaseClient(fetch_url, send_url)
            >>> h3_toolkit.set_hbase_client(hbase_client)
        """
        self.client = client
        return self

    def fetch_from_hbase(
        self,
        table_name:str,
        column_family:str,
        column_qualifier: list[str],
        rowkeys:list[str] = None
    ) -> H3Toolkit:
        """
        Fetches data from an HBase table based on H3 index row keys.

        Note:
            This method retrieves data from an HBase table using the H3 indices generated from methods
            like `process_from_vector()`, `process_from_raster()`, or `process_from_h3()` as row keys.

            It is necessary to call `set_hbase_client()` before using this method to set up the HBase
            client connection.

        Args:
            table_name (str):
                The name of the table in HBase. For example, 'res12_pre_data'.
            column_family (str):
                The name of the column family in HBase. For example, 'demographic'.
            column_qualifier (list[str]):
                A list of column qualifiers to retrieve from the HBase table.
                For example, ['p_cnt', 'h_cnt'].
            rowkeys (list[str], optional):
                A list of H3 indices to fetch data from the HBase table. If not provided,
                the method will use self.result, which contains the processed H3 data.

        Returns:
            H3Toolkit:
                Returns the instance of the `H3Toolkit` class with the fetched data stored
                in the `self.result` attribute.

        Raises:
            ValueError:
                If no H3 index is found in `self.result`, this error is raised, indicating
                that the method requires H3 data to proceed.
            HBaseConnectionError:
                If the HBase client is not set, this error is raised. The user must first
                call `set_hbase_client()` to initialize the connection to HBase.

        Examples:
            To fetch data from an HBase table 'res12_pre_data' under the 'demographic' column family
            with the column qualifiers 'p_cnt' and 'h_cnt':

            >>> from h3_toolkit import H3Toolkit
            >>> toolkit = H3Toolkit()
            >>> toolkit.set_hbase_client(hbase_client)
            >>> toolkit.process_from_vector(vector_data)
            >>> toolkit.fetch_from_hbase('res12_pre_data', 'demographic', ['p_cnt', 'h_cnt'])
            >>> # or
            >>> toolkit = H3Toolkit()
            >>> hex_ids = ['8c4ba1d2914b9ff', '8c4ba1d2914b8ff', '8c4ba1d2914b7ff']
            >>> toolkit.set_hbase_client(hbase_client)
            >>> toolkit.fetch_from_hbase('res12_pre_data', 'demographic', ['p_cnt', 'h_cnt'], rowkeys=hex_ids)
        """ # noqa: E501

        if self.result.is_empty():
            if rowkeys:
                self.result = pl.DataFrame({'hex_id': rowkeys})
            else:
                raise ValueError("Please provide the h3 index first \
                                before fetching data from HBase.")

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
        Sends data to an HBase table based on `self.result`, which retrieved by
        `process_from_vector()`, `process_from_raster()`, or `process_from_h3()`.

        This method sends the processed data stored in `self.result` to an HBase table. You must
        call `set_hbase_client()` before using this method to set up the HBase client connection.
        The data is sent using the H3 index as the row key and the specified column family and
        column qualifiers.

        Args:
            table_name (str):
                The name of the table in HBase. For example, 'res12_pre_data'.
            column_family (str):
                The name of the column family in HBase. For example, 'demographic'.
            column_qualifier (list[str]):
                A list of column qualifiers to send to the HBase table. For example, ['p_cnt'].
            h3_col (str, optional):
                The name of the column that contains the H3 index. Defaults to 'hex_id'.
            timestamp (int, optional):
                The timestamp for the data. Defaults to None,
                in which case the current time is used.

        Returns:
            H3Toolkit:
                Returns the instance of the `H3Toolkit` class after sending data to HBase.

        Raises:
            ValueError:
                If no processed data is found in `self.result`, this error is raised, indicating
                that the method requires data to be processed first.
            HBaseConnectionError:
                If the HBase client is not set, this error is raised. The user must first call
                `set_hbase_client()` to initialize the connection to HBase.

        Example:
            To send data to an HBase table 'res12_pre_data' under the 'demographic' column family
            with the column qualifier 'p_cnt':

            >>> from h3_toolkit import H3Toolkit
            >>> toolkit = H3Toolkit()
            >>> toolkit.set_hbase_client(hbase_client)
            >>> toolkit.process_from_vector(vector_data)
            >>> toolkit.send_to_hbase('res12_pre_data', 'demographic', ['p_cnt'])
        """

        if self.result.is_empty():
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



    def get_result(self, return_geometry:bool=False) -> pl.DataFrame | gpd.GeoDataFrame:
        """Retrieves the result of the data processing, optionally converting H3 cells back to geometries.

        This method returns the processed data, which can either remain in H3 cell format or be converted
        back to geometries if the `return_geometry` option is set to True. If the data has not been
        processed yet, it raises an error.

        Args:
            return_geometry (bool, optional): Whether to convert the H3 cells to geometries.
                Defaults to False. If set to True, the result will be returned as a GeoDataFrame with
                geometries corresponding to the H3 cells.

        Returns:
            pl.DataFrame | gpd.GeoDataFrame: The processed data. If `return_geometry` is False,
            a Polars DataFrame with H3 cell IDs is returned. If `return_geometry` is True,
            a GeoDataFrame with geometries is returned.

        Raises:
            ValueError: If the data has not been processed before calling this method.

        Example:
            >>> from h3_toolkit import H3Toolkit
            >>> toolkit = H3Toolkit()
            >>> toolkit.process_from_vector(geo_df)
            >>> result = toolkit.get_result(return_geometry=True)

        Note:
            - The method checks if the result has been processed before retrieval, raising a `ValueError` if the data is not available.
            - If `return_geometry` is True, the method will convert H3 cells into geometries using the `cell_to_geom` method and return a GeoDataFrame.
            - The resulting data will have any null values filled with 0 before being returned.
            - Future versions might include functionality to merge identical rows and process geometries more efficiently.
        """ #noqa: E501
        if self.result.is_empty():
            raise ValueError("Please process the data first before getting the result.")

        result = self.result.fill_null(0)

        # TODO: 將完全相同的row merge在一起, 配合cells_to_wkb_polygons
        if return_geometry:
            return (
                result
                .pipe(cell_to_geom)
            )
        else:
            return result
