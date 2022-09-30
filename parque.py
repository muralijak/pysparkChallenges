from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType, BooleanType

spark = SparkSession.builder.master('local[1]').appName("Test").getOrCreate()

from pyspark.sql.functions import *

df = spark.read.format('csv').options(header=True, inferSchema= True).load("C:\\Users\\mura\\PycharmProjects\\pythonProject\\src\\adult_data.csv")
df.printSchema()
df.show()
count = df.count()
print("Count of recors " + str(count))

#df.groupBy("education").count().show()
# df.groupBy("education").count().sort("count").show()

df.describe().show()

#df.describe("capital-gain").show()

# df.crosstab('age','income').sort("age_income").show()
#
# df.groupBy("age").count().sort("age").show()

agecount = df.filter(df.age > 40).count()
print("age count " + str(agecount))

df.groupby("marital-status").avg("capital-gain").show()

# pivotDF = df.groupBy("income").pivot("age")
# print(pivotDF)

df = df.withColumn("age_square",col("age")*2)
df.show()
dfcols = df.columns
dfnewcols = ['x', 'age', 'age_square', 'workclass', 'fnlwgt', 'education', 'educational-num', 'marital-status', 'occupation', 'relationship', 'race', 'gender', 'capital-gain', 'capital-loss', 'hours-per-week', 'native-country', 'income']
df_newcols = df.select(dfnewcols)
df_newcols.show()

df.groupby("native-country").agg(count("native-country").alias("cnt_counts")).show()
    # .sort(asc("count(native-country)"))\

# sort(asc("count(native_country)")).show()