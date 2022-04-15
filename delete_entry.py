import os
from setup_spark import get_spark_session
from handle_delta_tables import delete_entry, load_table_data

spark = get_spark_session()
file_with_names = open("names_to_be_deleted.txt", "r")
name_list = file_with_names.readlines()
list2 = []
for name in name_list:
    name = name.replace("\n", "")
    delete_entry(spark, name)

output_df = load_table_data(spark)
output_df.show()
