
from pyspark.sql import SparkSession
from cdc_create_db import *

spark = SparkSession.builder.appName("CDC").master("local[2]").enableHiveSupport().getOrCreate()

create_db_tbl_flag = create_db_tbl(spark)
print(create_db_tbl_flag)




