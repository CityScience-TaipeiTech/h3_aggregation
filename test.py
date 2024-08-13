import polars as pl
from sqlalchemy import create_engine
import geopandas as gpd
from h3_toolkit.core import vector_to_cell_hbase
from h3_toolkit.hbase.client import get_data, put_data
import time

URI = 'postgresql://airflow:airflow@10.100.2.218:5432/postgres'
TIMESTAMP = "2024-07-17T16:02:46.110+08:00"

def get_data_from_pg(sql):
    db_data = pl.read_database_uri(
        sql,
        URI, 
        engine='connectorx',
    )
    db_data = db_data.unique()
    return db_data

if __name__ == '__main__':
    sql = """
        select 
            stats.village_id , 
            stats.mean, 
            ST_AsBinary(admin.geometry) as geometry_wkb
        from geometry.boundary_village as admin
        inner join geometry.ppl_income as stats
        on admin.village_code = stats.village_id
        where "city_name" = '臺北市'
    """
    # sql = """
    # SELECT name, category, ST_AsBinary(geometry) as geometry_wkb
    # FROM geometry.overture_places
    # """


    db_data = get_data_from_pg(sql)
    resolution = 12

    # convert to h3
    test = vector_to_cell_hbase(
        data = db_data,
        agg_func = 'sum', # sum / count / mean / percentage / major 
        agg_col = "village_id",
        target_cols = ["mean"],
        resolution = resolution,
    )
    test.write_csv(f'test_{resolution}.csv')
    # save to hbase
    put_data(
        test,
        table_name=f'res{resolution}_pre_data',
        cf = "demographic",
        cq_list=["mean_sum"],
        rowkey_col='hex_id',
        timestamp=TIMESTAMP,
    )

# end_time = time.time()
# execution_time = end_time - start_time
# print(f"程式執行時間： {execution_time} 秒")
# print("-----------------")

# start_time = time.time()
# print(get_data(
#     table_name='res12_test_data',
#     cf='cf1',
#     cq_list=['h_cnt_sum', 'p_cnt_sum'],
#     rowkeys=test['hex_id'].to_list(),
# ))
# end_time = time.time()
# execution_time = end_time - start_time
# print(f"程式執行時間： {execution_time} 秒")