import os
from setup_spark import get_spark_session
from handle_delta_tables import restore_to_version

#spark = get_spark_session()
version = input("Version Number: ")
try:
    version = int(version)
except ValueError as e:
    print("Value passed is not int")
    exit()
restore_to_version(spark, version)
