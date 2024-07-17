import polars as pl
from sqlalchemy import create_engine
import geopandas as gpd
from h3_tool.convert import vector_to_cell
from h3_tool.hbase_get import get_data
from h3_tool.hbase_put import put_data
import time

URI = 'postgresql://airflow:airflow@10.100.2.218:5432/postgres'
engine = create_engine(URI)

TIMESTAMP = "2024-07-17T16:02:46.110+08:00"

sql = """
select 
    stats.village_id , 
    stats.h_cnt, 
    stats.p_cnt, 
    ST_AsBinary(admin.geometry) as geometry_wkb
from geometry.admin_village as admin
inner join geometry.ppl_population as stats
on admin."VillageCode" = stats.village_id
where "CityName" = '臺北市'
"""

start_time = time.time()
db_data = pl.read_database_uri(
    sql,
    URI, 
    engine='connectorx',
)
db_data = db_data.unique()

# convert to h3
test = vector_to_cell(
    data = db_data,
    agg_func = 'sum', # sum / count / mean / percentage / major 
    agg_col = "village_id",
    target_cols = ["h_cnt", "p_cnt"],
    resolution = 10,
)

# # save to hbase
# put_data(
#     test,
#     table_name='res12_pre_data',
#     cf = "demographic",
#     cq_list=["h_cnt_sum", "p_cnt_sum"],
#     rowkey_col='hex_id',
#     timestamp=TIMESTAMP,
# )

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