import os
from setup_spark import get_spark_session
from handle_delta_tables import load_table_data  


spark = get_spark_session()
output_df = load_table_data(spark)
output_df.show()
