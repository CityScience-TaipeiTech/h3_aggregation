import asyncio
import gc
import json
import logging

import aiohttp
import polars as pl


class SingletontMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]

class HBaseClient(metaclass=SingletontMeta):
    """
    Initializes the HBaseClient instance for fetching and sending data to HBase servers.

    This client supports fetching and sending data to HBase via URLs and controls the
    maximum number of concurrent requests and the chunk size per request to ensure
    efficient data processing and transmission.
    
    Note:
        - The `semaphore` attribute is used to limit the concurrency of requests to the HBase server, ensuring the client adheres to the server's request limits.
        - The `chunk_size` determines the number of row keys processed per request, which helps balance between performance and server load.

    Args:
        fetch_url (str): The URL used for fetching data from the HBase server.
        send_url (str): The URL used for sending data to the HBase server.
        max_concurrent_requests (int, optional): The maximum number of concurrent requests
            allowed. Defaults to 5.
        chunk_size (int, optional): The number of row keys processed per request chunk.
            Defaults to 200,000.

    Attributes:
        fetch_url (str): The URL for fetching data.
        send_url (str): The URL for sending data.
        semaphore (asyncio.Semaphore): Controls the number of concurrent requests
            that can be processed simultaneously to prevent overwhelming the server.
        chunk_size (int): The size of data chunks (in row keys) sent per request.

    Example:
        >>> client = HBaseClient(fetch_url="http://hbase-fetch-url",
        >>>                      send_url="http://hbase-send-url",
        >>>                      max_concurrent_requests=5,
        >>>                      chunk_size=200000)
        >>> # Fetch and send data using the client

    """ # noqa
    def __init__(self,
                 fetch_url:str,
                 send_url:str,
                 max_concurrent_requests:int=5,
                 chunk_size:int=200000
                ):
        self.fetch_url = fetch_url
        self.send_url = send_url
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.chunk_size = chunk_size

    async def _fetch_data(self, session, form_data):
        async with self.semaphore:
            try:
                async with session.post(self.fetch_url, data=form_data) as response:
                    response.raise_for_status() # 有任何不是200的response都會raise exception
                    response_text = await response.json()
                    logging.info("Successfully fetch data")
                    return response_text
            except aiohttp.ClientResponseError as e:
                logging.error(f"Failed to fetch data: {e.status} {e.message}")
                return None
            except Exception as e:
                logging.error(f"Exception occurred: {str(e)}")
                return None

    async def _fetch_data_with_retry(self, session, form_data, retries=3):
        for attempt in range(retries):
            result = await self._fetch_data(session, form_data)
            if result is not None:
                return result
            logging.warning(f"Retry {attempt + 1}/{retries} failed for fetching data")
            await asyncio.sleep(1)
        return None

    async def _fetch_data_main(self, table_name, cf, cq_list, rowkeys):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for start in range(0, len(rowkeys), self.chunk_size):
                form_data = {
                    "tablename": table_name,
                    "rowkey": json.dumps(rowkeys[start:start + self.chunk_size]),
                    "column_qualifiers": json.dumps({cf: cq_list})
                }
                tasks.append(self._fetch_data_with_retry(session, form_data))

            responses = await asyncio.gather(*tasks)

            dfs = []
            for idx, response in enumerate(responses):
                start = idx * self.chunk_size
                end = min(start + self.chunk_size, len(rowkeys))

                if not response:
                    logging.warning(
                        f"Some rowkeys in input range {start}-{end} did not return data"
                    )
                else:
                    for key in response.keys():
                        dfs.append(pl.DataFrame(response[key]))

            if not dfs:
                raise ValueError("No data fetched from HBase, please check the input parameters")

            return pl.concat(dfs, how='vertical')

    # async def _fetch_data_chunks(self, session, table_name, cf, cq_list, rowkeys):
    #     for start in range(0, len(rowkeys), self.chunk_size):
    #         form_data = {
    #             "tablename": table_name,
    #             "rowkey": json.dumps(rowkeys[start:start + self.chunk_size]),
    #             "column_qualifiers": json.dumps({cf: cq_list})
    #         }
    #         yield form_data

    # async def _fetch_data_main(self, table_name, cf, cq_list, rowkeys):
    #     async with aiohttp.ClientSession() as session:
    #         fetch_data_chunks = self._fetch_data_chunks(session, table_name, cf, cq_list, rowkeys)
    #         tasks = [self._fetch_data_with_retry(session, form_data)
    #                   for form_data in fetch_data_chunks]

    #         responses = await asyncio.gather(*tasks)
    #         dfs = []
    #         for response in responses:
    #             if response:
    #                 for key in response.keys():
    #                     dfs.append(pl.DataFrame(response[key]))

    #         result_df = pl.concat(dfs, how='vertical') if dfs else pl.DataFrame()
    #         del dfs
    #         gc.collect()
    #         return result_df


    async def _send_data(self, session, result):
        async with self.semaphore:
            try:
                async with session.post(self.send_url, json=result) as response:
                    response.raise_for_status() # 有任何不是200的response都會raise exception
                    response_text = await response.text()
                    logging.info(f"Successfully sent data: {response_text}")
                    return True
            except aiohttp.ClientResponseError as e:
                logging.error(f"Failed to send data: {e.status} {e.message}")
                return None
            except Exception as e:
                logging.error(f"Exception occurred: {str(e)}")
                return None

    async def _send_data_with_retry(self, session, result, retries=3):
        for attempt in range(retries):
            success = await self._send_data(session, result)
            if success:
                return "Success"
            logging.warning(f"Retry {attempt + 1}/{retries} failed for data chunk")
            await asyncio.sleep(1)  # 等待一段時間後重試
        return "Failed"

    async def _send_data_main(self, data, table_name, cf, cq_list, rowkey_col, timestamp):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for start in range(0, len(data), self.chunk_size):
                chunk = data.slice(start, self.chunk_size)
                result = {
                    "cells": [
                        {
                            "rowkey": row[rowkey_col],
                            "datas": {
                                cf: { cq: str(row[cq]) for cq in cq_list if row[cq] is not None},
                            }
                        } for row in chunk.iter_rows(named=True)
                    ],
                    "tablename": f"{table_name}",
                    "timestamp": timestamp if timestamp else ""
                }
                tasks.append(self._send_data_with_retry(session, result))

            _ = await asyncio.gather(*tasks)
            # for response in responses:
            #     print(response)

    # async def _send_data_chunks(self, session, data, table_name, cf, cq_list, rowkey_col,
    #                               timestamp):
    #     for start in range(0, len(data), self.chunk_size):
    #         chunk = data.slice(start, self.chunk_size)
    #         result = {
    #             "cells": [
    #                 {
    #                     "rowkey": row[rowkey_col],
    #                     "datas": {
    #                         cf: { cq: str(row[cq]) for cq in cq_list if row[cq] is not None },
    #                     }
    #                 } for row in chunk.iter_rows(named=True)
    #             ],
    #             "tablename": f"{table_name}",
    #             "timestamp": timestamp if timestamp else ""
    #         }
    #         yield result

    # async def _send_data_main(self, data, table_name, cf, cq_list, rowkey_col, timestamp):
    #     async with aiohttp.ClientSession() as session:
    #         send_data_chunks = self._send_data_chunks(session, data, table_name, cf, cq_list,
    #                               rowkey_col, timestamp)
    #         tasks = [self._send_data_with_retry(session, result) for result in send_data_chunks]

    #         responses = await asyncio.gather(*tasks)
    #         del tasks
    #         gc.collect()


    def fetch_data(self,
                table_name:str,
                column_family:str,
                column_qualifier:list[str],
                rowkeys:list[str]
        )->pl.DataFrame:
        """

        Args:
            table_name: str, the table name in HBase, ex: "res12_pre_data"
            cf: str, the column family in HBase, ex: "demographic"
            cq_list: list[str], the column qualifier in HBase, ex: ["p_cnt", "h_cnt"]
            rowkeys: list[str], the rowkeys to be fetched, ex: ["8c4ba0a415749ff","8c4ba0a415741ff"]

        Returns:
            pl.DataFrame: the fetched data in polars DataFrame
        """
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(
            self._fetch_data_main(
                table_name, column_family, column_qualifier, rowkeys
            )
        )

        result = (
            result
            .unnest('properties')
            .pivot(index="row", values="value", on="qualifier")
            .select(
                pl.col("row").alias("hex_id"),
                pl.exclude("row")
            )
        )

        return result

    def send_data(self,
                data:pl.DataFrame,
                table_name:str,
                column_family:str,
                column_qualifier:list[str],
                rowkey_col="hex_id",
                timestamp=None
        ) -> None:
        """
        Args:
            rowkey_col: str, the column name of rowkey, default is "hex_id"
            timestamp: str, if timestamp is None, it will use the current time
        """
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._send_data_main(
            data, table_name, column_family, column_qualifier, rowkey_col, timestamp
        ))
        del data
        gc.collect()

if __name__ == '__main__':
    client = HBaseClient()

    # Example for Get data
    table_name = "example_table"
    cf = "cf"
    cq_list = ["cq1", "cq2"]
    rowkeys = ["row1", "row2"]
    data = client.fetch_data(table_name, cf, cq_list, rowkeys)

    # Example for Put data
    data_to_put = pl.DataFrame({
        "rowkey": ["row1", "row2"],
        "cq1": [1, 2],
        "cq2": [3, 4]
    })
    client.send_data(data_to_put, table_name, cf, cq_list, "rowkey")
