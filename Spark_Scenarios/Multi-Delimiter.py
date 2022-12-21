from pyspark import rdd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, regexp_replace

spark = SparkSession.builder.master('local[*]').appName("multi_delimiter").getOrCreate()

df = spark.read.text('Multi-Delimiter.csv')
df.show(truncate=False)

header = df.first()[0]
print(header)

header_schema = header.split('~|')
print(header_schema)

df1 = df.filter(col("value") != header)
df1.show()

# df2 = df.select(split(col("value"), '/~').alias("newcol"))
# df2.show(truncate=0)

df3 = df1.withColumn("newcol", regexp_replace('value','~',' ')).select("newcol")
df3.show()

rd = df3.rdd.map(lambda x : x[0].split('|'))
rd.foreach(print)
cols = ["Name","Age"]
df4 = rd.toDF(cols)
df4.show()





