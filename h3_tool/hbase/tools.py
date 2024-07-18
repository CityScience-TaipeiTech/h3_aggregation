import asyncio
import aiohttp
import logging
import polars as pl
import json
from datetime import datetime

GET_URL = 'http://10.100.2.218:2891/api/hbase/v1/test/filterdata2'
PUT_URL = 'http://10.100.2.218:2891/api/hbase/v1/test/putdata'

def get_data(
        table_name:str, 
        cf:str,
        cq_list:list[str], 
        rowkeys:list[str], 
        url:str = GET_URL
    )-> pl.DataFrame:
    """
    get data from hbase table
    """

    async def get_data(session, url, form_data):
        try:
            async with session.post(url, data=form_data) as response:
                response_text = await response.json()
                if response.status != 200:
                    logging.error(f"Failed to get data: {response.status} {response_text}")
                else:
                    logging.info(f"Successfully get data: {response_text}")
                return response_text
        except Exception as e:
            logging.error(f"Exception occurred: {str(e)}") 
            return None

    async def main():
        async with aiohttp.ClientSession() as session:
            tasks = []
            # chunk the rowkeys to avoid the request size too large
            chunk_size = 200000
            for start in range(0, len(rowkeys), chunk_size):
                form_data = {
                    "tablename": table_name,
                    "rowkey": json.dumps(rowkeys[start:start+chunk_size]),
                    # "column_qualifiers": f"{{{cf}: {cq_list}}}"
                    "column_qualifiers": json.dumps({
                        cf: cq_list
                    })
                }
                print("task sent")
                tasks.append(get_data(session, url, form_data))
            
            responses = await asyncio.gather(*tasks)
            print("task received")
            # TODO: Optimize here
            dfs = []
            for response in responses:
                # print("-----------------")
                # return response.keys()
                # dfs_cols = []
                for key in response.keys():
                    df = pl.DataFrame(response[key])
                    dfs.append(df)
                    # elif cq_list[1] in key:
                    #     df = pl.DataFrame(response[key])
                    #     dfs2.append(df)
                # print(response[])
                # dfs.append(pl.DataFrame(response.values()['properties']))
                # dfs2 = []
                # for key in response.keys():
                #     if cq_list[0] in key:
                #         df = pl.DataFrame(response[key])
                #         dfs.append(df)
                #     elif cq_list[1] in key:
                #         df = pl.DataFrame(response[key])
                #         dfs2.append(df)
                
                # print(pl.concat(dfs, how='vertical'))
        
                # for d in response.values():
                #     df = pl.DataFrame(d['properties']).rename({"value": d['qualifier']})
                #     print(df)
                #     df = pl.DataFrame(d['properties']).rename({"value": d['qualifier']})
                #     if len(dfs_cols) != 0:
                #         df = df.drop('row')
                #     dfs_cols.append(df)
                # print(dfs_cols)
                # dfs_rows.append(pl.concat(dfs_cols, how='vertical'))
            # return pl.concat(dfs_rows, how='horizontal')
            return pl.concat(dfs, how='vertical')

    result = asyncio.run(main())
    result = (
        result
        .unnest('properties')
        .pivot(
            index = "row",
            values = "value",
            on = "qualifier"
        )
        .select(
            pl.col("row").alias("hex_id"),
            pl.exclude("row")
        )
    )
    print(result)


    return result
        

def put_data(
        data:pl.DataFrame, 
        table_name:str, 
        cf:str,
        cq_list:list, 
        rowkey_col:str, 
        timestamp:datetime=None,
        url:str = PUT_URL
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