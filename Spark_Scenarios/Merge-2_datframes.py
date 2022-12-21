from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StringType, StructField, IntegerType

spark = SparkSession.builder.master("local").appName("merge two dataframes").getOrCreate()

# Option 1- create a new column in df1, that makes df1 and df2 have same col's for union operation.
df1 = spark.read.csv('Merge-2_datframes-1.csv', header=True)
df1.show()
df2 = spark.read.csv('Merge-2_datframes-2.csv', header=True)
df2.show()

new_df1 = df1.withColumn("Year", lit("null"))
new_df1.union(df2).show()

# Option 2- create a schema for df1 with new column(Year), that will auto create a col in df1 with null values.

schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Education", StringType(), True),
    StructField("Year", StringType(), True)
])

df11 = spark.read.csv('Merge-2_datframes-1.csv', header=True, schema=schema)
df11.show()
df12 = spark.read.csv('Merge-2_datframes-2.csv', header=True)
df12.show()
df_union = df11.union(df12)
print("Option2-create schema output")
df_union.show()

# Option3 - Outer Join

df31 = spark.read.csv('Merge-2_datframes-1.csv', header=True)
df31.show()
df32 = spark.read.csv('Merge-2_datframes-2.csv', header=True)
df32.show()


# df_join = df31.join("df32", on=["Name","Age"], how="outer")
# df_join.show()

# Option 4 - Eg - like prod like env if we have multiple columns, the dynamically calculate the missing col's in both DF's
# and create new columns accordingly in both df's

df41 = spark.read.csv('Merge-2_datframes-1.csv', header=True)
df41.show()
df42 = spark.read.csv('Merge-2_datframes-2.csv', header=True)
df42.show()

lst1 = set(df41.columns)
lst2 = set(df42.columns)
print(lst1)
print(lst2)
lst3 = lst2 - lst1
print(lst3)
diff_lst1 = list(set(df41.columns) - set(df42.columns))
diff_lst2 = list(set(df42.columns) - set(df41.columns))
print(diff_lst1)
print(diff_lst2)
# subtracted = list()
# for i, j in zip(lst1, lst2):
#     item = i - j
#     subtracted.append(item)
#
# print(subtracted)
for i in diff_lst1:
    df42 = df42.withColumn(i, lit("null"))
    df42.show()

for i in diff_lst2:
    df41 = df41.withColumn(i, lit("null"))
    df41.show()
df41.union(df42).show()