from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, split, explode, col, collect_list
from pyspark.sql.types import StructType, StructField, IntegerType

spark = SparkSession.builder.master('local[*]').appName("del_dup").getOrCreate()

df = spark.read.csv("lin*")
df.show(1, False)
# df.show(truncate=False)

df1 = df.withColumn("newcol", regexp_replace("_c0", "(.*?\\|){5}", "$0-")).select("newcol")
df1.printSchema()
df1.show(truncate=False)

df2 = df1.select(split(col("newcol"), "\|-").alias("new_col"))

df2.show(truncate=False)

df3 = df2.select(explode(col("new_col")))
df3.printSchema()
df3.show(truncate=False)
#
rdd_df4 = df3.rdd.map(lambda x : x[0].split('|')) # .map(lambda y: (y[0],y[1],y[2],y[3],y[4]))
rdd_df4.foreach(print)
cols = ["name","education","roll_no","tech","mob"]
df4 = rdd_df4.toDF(cols)
df4.show(truncate=False)


#df4.show(truncate=False)

# df4 = df3.select(split(col("col"), "\|").alias("split_col"))
# df4.show(truncate=False)

# df5 = df4.select(collect_list("split_col").alias("lst_col"))
# df5.show(truncate=False)
# res_lst = df5.collect()
# print('res lst is ', res_lst)
#
# schema = StructType([
#     StructField("Name",StructType(), True),
#     StructField("Education",StructType(), True),
#     StructField("Garde",IntegerType(), True),
#     StructField("Course",StructType(), True),
#     StructField("Mob",IntegerType(), True)
# ])
#
#
# df6 = spark.createDataFrame(data=res_lst) #, schema=schema)
# df6.show(truncate=False)
# #

