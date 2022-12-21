from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.master('local[*]').appName("del_dup").getOrCreate()

schema = StructType([
    StructField("Employee_ID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("corrput_record", StringType(), True)

    ])

df = spark.read.csv('corrupt*', header=True, schema=schema, mode='PERMISSIVE', columnNameOfCorruptRecord='corrput_record')
df.show()


