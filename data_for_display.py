from constant import TW_COUNTY_DICT, URI, TIMESTAMP
import polars as pl
from sqlalchemy import create_engine
import geopandas as gpd
from h3_tool.main import vector_to_cell_hbase
from h3_tool.hbase.tools import HBaseClient
import logging
import time

# ppl h_cnt, p_cnt SQL
# with boundary as (
#     select 代碼, geometry
#     from geometry.boundary_smallest
#     where 縣市代碼 = '{city_code}'
# )
# select aps.*, ST_AsBinary(boundary.geometry) as geometry_wkb
# from geometry.af_ppl_stats as aps
# join boundary
# on aps.codebase = boundary.代碼

def get_data_from_pg(city_code):
    sql = f"""
    with boundary as (
        select (village_code || '_2021') AS village_code, geometry
        from geometry.boundary_village
        where city_name = '{city_code}'
    )
    select aps.mean AS income_mean, aps.median AS income_median, aps.first_quartile AS income_first_quartile, aps.third_quartile as income_third_quartile, ST_AsBinary(boundary.geometry) as geometry_wkb
    from geometry.af_ppl_income as aps
    join boundary
    on aps.v_id = boundary.village_code
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

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    
    # Initialize
    client = HBaseClient()

    for city_name, city_code in TW_COUNTY_DICT.items():
        logging.info(f"===============start processing {city_name}===============")
        start_time = time.time()
        
        data = get_data_from_pg(city_name)
        resolution = 12

        # convert to h3
        h3_data = vector_to_cell_hbase(
            data,
            agg_func='avg',
            # agg_col= 'codebase',
            target_cols=["income_mean", "income_median", "income_first_quartile", "income_third_quartile"],
            resolution=resolution,
        )   
        
        # save to hbase
        h3_data = (h3_data
                       .drop(["income_mean", "income_median", "income_first_quartile", "income_third_quartile"])
                       .rename({'income_mean_avg': 'income_mean', 'income_median_avg': 'income_median', 'income_first_quartile_avg': 'income_first_quartile', 'income_third_quartile_avg': 'income_third_quartile'}))
        
        logging.info(f"===============loading data into hbase...===============")
        
        client.send_data(
            h3_data,
            table_name=f'res{resolution}_pre_data',
            cf = "economic",
            cq_list=["income_mean", "income_median", "income_first_quartile", "income_third_quartile"],
            rowkey_col='hex_id',
            timestamp=TIMESTAMP,
        )
        
        end_time = time.time()
        duration = end_time - start_time
        logging.info(f"==============={city_name} completed!, using time: {duration:.3}s===============")
        