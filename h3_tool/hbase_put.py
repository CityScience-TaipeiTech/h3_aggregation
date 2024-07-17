import asyncio
import aiohttp
import logging
import polars as pl
from datetime import datetime

URL = 'http://10.100.2.218:2891/api/hbase/v1/test/putdata'
# URL = 'http://localhost:8080/api/hbase/v1/test/putdata'

def put_data(
        data:pl.DataFrame, 
        table_name:str, 
        cf:str,
        cq_list:list, 
        rowkey_col:str, 
        timestamp:datetime=None,
        url:str = URL
    )->None:
    """
    call hbase api
    """

    async def send_data(session, url, result):
        try:
            async with session.post(url, json=result) as response:
                response_text = await response.text()
                if response.status != 200:
                    logging.error(f"Failed to send data: {response.status} {response_text}")
                else:
                    logging.info(f"Successfully sent data: {response_text}")
                return response_text
        except Exception as e:
            logging.error(f"Exception occurred: {str(e)}") 
            return None

    async def main():
        async with aiohttp.ClientSession() as session:
            tasks = []
            chunk_size = 200000
            for start in range(0, len(data), chunk_size):
                chunk = data.slice(start, chunk_size)
                result = {
                    "cells": [
                        {
                            "rowkey": row[rowkey_col],
                            "datas": {
                                cf: { cq: str(row[cq]) for cq in cq_list},
                            }
                        } for row in chunk.iter_rows(named=True)
                    ],
                    "tablename": f"{table_name}",
                    # if set timestamp, the format must to ve RFC3339
                    # "timestamp": f"{datetime.now(timezone.utc).astimezone().isoformat()}" if timestamp is None else timestamp
                    # let timestamp empty to use the current time
                    "timestamp": timestamp if timestamp else ""
                }
                tasks.append(send_data(session, url, result))
            
            responses = await asyncio.gather(*tasks)
            for response in responses:
                print(response)

    asyncio.run(main())
        