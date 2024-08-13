from h3_toolkit.core import H3AggregatorUp
from constant import TW_COUNTY_DICT, URI, TIMESTAMP
import polars as pl
from sqlalchemy import create_engine
import geopandas as gpd
from h3_toolkit.hbase.client import HBaseClient
import logging
import time

def get_data_from_pg(city_code):
    sql = f""" 
        with boundary as (
            select 代碼, geometry
            from geometry.boundary_smallest
            where 縣市代碼 = '{city_code}'
        )
        select aps.*, ST_AsBinary(boundary.geometry) as geometry
        from geometry.af_ppl_stats as aps
        join boundary
        on aps.codebase = boundary.代碼
    """
    db_data = pl.read_database_uri(
        sql,
        URI, 
        engine='connectorx',
    )
    db_data = db_data.unique()
    
    # write for income data
    # print(db_data)

    return db_data


db_data = get_data_from_pg("10017")
# db_data.write_csv()

aggregator = H3AggregatorUp()
result = (
    aggregator
    .set_client(HBaseClient())
    .set_resolution_source(12)
    .set_resolution_target(9)
    .set_geometry('geometry')
    .sum(target_cols=["p_cnt"])
    .fetch_hbase_data(table_name = 'res12_pre_data', column_family = 'demographic', column_qualifier = ['p_cnt'], data = db_data)
    .process()
)   

result.write_csv("result_9.csv")
# client = HBaseClient()
# client.send_data(result, "test_table", "cf", ["p_cnt"], "codebase", TIMESTAMP)

