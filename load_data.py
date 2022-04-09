import os
from dotenv import load_dotenv
from delta import *
from setup_spark import get_spark_session
from get_json_schema import get_table_df
from handle_delta_tables import create_delta_tables,write_to_delta_table 
import pyspark as pyspark


spark = get_spark_session()
table_df = get_table_df(spark)
create_delta_tables(spark, table_df)
write_to_delta_table(spark, table_df)
