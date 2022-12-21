import string

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, date_add, expr

spark = SparkSession.builder.master("local").appName("dealingwithdate").getOrCreate()

df = spark.read.csv("dealing*", header=True, inferSchema=True)
df.show()
df.printSchema()

df1 = df.withColumn("date_new",to_date(col("RechargeDate").cast("string"),"yyyyMMdd"))
df1.printSchema()
df1.show()

# df2 = df1.withColumn("Extended_warrenty", date_add("date_new","RemainingDays"))
df1.select("*").withColumn("Expiry_date", expr("date_add(date_new,RemainingDays)")).show()
# df2.show()
