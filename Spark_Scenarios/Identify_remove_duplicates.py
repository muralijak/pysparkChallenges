from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
spark = SparkSession.builder.master('local[*]').appName("del_dup").getOrCreate()

print(spark)

df = spark.read.csv("duplicate.csv", header='true')
df.show()

#df.dropDuplicates()

#df.subtract(df.dropDuplicates()).show()

# df.groupBy(list(df.columns)).count().where("count > 1").show()
df.groupBy(*df.columns).count().where("count > 1").drop("count").show()
#df.groupBy(*df.columns)

win = Window.partitionBy(*df.columns).orderBy(col("Year").desc())

df.withColumn("rn", row_number().over(win))
df.withColumn("rn", row_number().over(win)).filter("rn > 1").drop("rn").show()
