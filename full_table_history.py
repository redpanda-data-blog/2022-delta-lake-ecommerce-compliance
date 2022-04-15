import os
from setup_spark import get_spark_session
from handle_delta_tables import get_full_history

spark = get_spark_session()
get_full_history(spark)
