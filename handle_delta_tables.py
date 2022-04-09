import os
import pyspark as pyspark
import json
from dotenv import load_dotenv
from delta import *
from IPython.display import display
from pyspark.sql.functions import expr
from pyspark.sql.functions import from_json, col
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import *


def create_delta_tables(spark, table_df):
    load_dotenv()
    table_path = os.environ.get('DELTA_LAKE_TABLE_DIR')
    table = (spark
        .createDataFrame([], table_df.schema)
        .write
        .option("mergeSchema", "true")
        .format("delta")
        .mode("append")
        .save(table_path))


def write_to_delta_table(spark, table_df):
    writestream = table_df.writeStream;
    table_path = os.environ.get('DELTA_LAKE_TABLE_DIR')
    checkpoint_path = os.environ.get('DELTA_LAKE_CHECKOUT_DIR')
    ret = (writestream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .outputMode("append") 
        .start(table_path)) 


def load_table_data(spark):
    load_dotenv()
    table_path = os.environ.get('DELTA_LAKE_TABLE_DIR')
    df_delta = (spark.read
            .format("delta")
            .load(table_path))

    return df_delta
