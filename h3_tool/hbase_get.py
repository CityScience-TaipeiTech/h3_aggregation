import asyncio
import aiohttp
import logging
import polars as pl
import json

URL = 'http://10.100.2.218:2891/api/hbase/v1/test/filterdata2'

def get_data(
        table_name:str, 
        cf:str,
        cq_list:list, 
        rowkeys:list, 
        url:str = URL
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
            # "row",
            index = "row",
            values = "value",
            on = "qualifier"
        )
    )



    return result
        