from constant import TW_COUNTY_DICT, URI, TIMESTAMP
import polars as pl
from sqlalchemy import create_engine
import geopandas as gpd
from h3_toolkit.core import vector_to_cell, vector_to_cell_scale_up
from h3_toolkit.hbase.client import HBaseClient
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

# income SQL
# with boundary as (
#     select (village_code || '_2021') AS village_code, geometry
#     from geometry.boundary_village
#     where city_name = '{city_code}'
# )
# select aps.mean AS income_mean, aps.median AS income_median, aps.first_quartile AS income_first_quartile, aps.third_quartile as income_third_quartile, ST_AsBinary(boundary.geometry) as geometry_wkb
# from geometry.af_ppl_income as aps
# join boundary
# on aps.v_id = boundary.village_code

# boundary SQL
# SELECT ST_AsBinary(geometry) as geometry_wkb
# FROM geometry.boundary_town
# WHERE city_name = '{city_code}'

# POI SQL
# WITH boundary as (
# SELECT geometry as geometry
# FROM geometry.boundary_town
# WHERE city_name = '{city_code}'
# )
# select poi.category, poi.name, ST_AsBinary(poi.geometry) as geometry_wkb
# from geometry.overture_places as poi, boundary
# where poi.geometry && boundary.geometry AND ST_Intersects(poi.geometry, boundary.geometry)


def get_data_from_pg(city_code):
    sql = f""" 
        SELECT ST_AsBinary(geometry) as geometry_wkb
        FROM geometry.boundary_town
        WHERE city_name = '{city_code}'
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

    sql = f"""
        SELECT DISTINCT(category)
        FROM geometry.overture_places
    """
    poi_category = pl.read_database_uri(
        sql,
        URI, 
        engine='connectorx',
    )
    poi_category = poi_category.unique()
    poi_category = poi_category['category'].to_list()
    poi_category.extend(['total_count', 'null'])

    for city_name, city_code in TW_COUNTY_DICT.items():   
        logging.info(f"===============start processing {city_name}===============")
        start_time = time.time()
            
        data = get_data_from_pg(city_name)
        resolution = 7  # 更改

        # convert to h3
        # h3_data = vector_to_cell(
        #     data,
        #     agg_func='count',
        #     # agg_col= 'codebase',
        #     target_cols=["category"],  # 更改
        #     resolution=resolution,
        # )   

        h3_data = vector_to_cell_scale_up(
            client,
            table_name='res12_pre_data',
            column_family='overture_poi',
            column_qualifier=poi_category,
            data=data,
            agg_func='count',
            geometry_col='geometry_wkb',
            resolution_source=12,
            resolution_target=7,
        )

        # save to hbase
        # h3_data = (h3_data
        #                .drop(["income_mean", "income_median", "income_first_quartile", "income_third_quartile"])
        #                .rename({'income_mean_avg': 'income_mean', 'income_median_avg': 'income_median', 'income_first_quartile_avg': 'income_first_quartile', 'income_third_quartile_avg': 'income_third_quartile'}))
        # h3_data = (h3_data
        #                .drop(["m_cnt", "f_cnt"])
        #                .rename({'m_cnt_sum': 'm_cnt', 'f_cnt_sum': 'f_cnt'}))

        # h3_data = (h3_data)
        
        logging.info(f"===============loading data into hbase...===============")
        
        client.send_data(
            h3_data,
            table_name=f'res{resolution}_test_data', # 記得更改table name
            cf = "overture_poi",  # demographic or economic
            cq_list=h3_data.columns[1:],  # 更改
            rowkey_col='hex_id',
            timestamp=TIMESTAMP,
        )
    
        end_time = time.time()
        duration = end_time - start_time
        logging.info(f"==============={city_name} completed!, using time: {duration:.3}s===============")
        