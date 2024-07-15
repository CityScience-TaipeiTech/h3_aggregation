import polars as pl
from sqlalchemy import create_engine
import geopandas as gpd
from h3_tool.convert import vector_to_cell
from h3_tool.hbase_get import get_data
import time

URI = 'postgresql://airflow:airflow@10.100.2.218:5432/postgres'
engine = create_engine(URI)

sql = """
select 
    stats.codebase , 
    stats.h_cnt, 
    stats.p_cnt, 
    ST_AsBinary(admin.geometry) as geometry_wkb
from geometry.admin_smallest as admin
inner join geometry.af_ppl_stats as stats
on admin.代碼 = stats.codebase
where "縣市代碼" = 63000
"""
start_time = time.time()
db_data = pl.read_database_uri(
    sql,
    URI, 
    engine='connectorx',
)
db_data = db_data.unique()


test = vector_to_cell(
    data = db_data,
    agg_func = 'test',
    agg_cols = ["codebase"],
    target_cols = ["h_cnt", "p_cnt"],
)
end_time = time.time()
execution_time = end_time - start_time
print(f"程式執行時間： {execution_time} 秒")
print("-----------------")

start_time = time.time()
print(get_data(
    table_name='res12_test_data',
    cf='cf1',
    cq_list=['h_cnt_sum', 'p_cnt_sum'],
    rowkeys=test['hex_id'].to_list(),
))
end_time = time.time()
execution_time = end_time - start_time
print(f"程式執行時間： {execution_time} 秒")