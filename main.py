from pyspark.sql import SparkSession, Window

# from pyspark.sql.functions import sort
from pyspark.sql.functions import split, col, collect_set, explode, lit

spark = SparkSession.builder.master('local[1]').appName("Test").getOrCreate()

print(spark)

df = spark.sql("""select current_date""")

# name = "hi my name is murali"
#
# word = [sorted(i) for i in name.split(" ")]
# print(word)
# res = " "
# result = (' '.join(word))
#
# print(result)

# filePath="C:\\Users\\mura\\PycharmProjects\\pythonProject\\src\\smallzipcodes.csv"
# df = spark.read.options(header='true', inferSchema='true') \
#           .csv(filePath)
#
# df.printSchema()
# df.show(truncate=False)
#
# df.fillna("").show()
# df.na.fill("{}").show()
# data = [1,2,2,2,1,0,0,0,2,5,2]
#
# data1 = sorted(data)
# print(data1)

# test_list = (("a","aa",1),("b","bb",2),("c","cc",3))
#
# # print("The original list is : " + str(test_list))
# # res = test_list[0]
# # print(res)
# # res1 = res[0], res[-1]*2
# # print(res1)
#
# for res in test_list:
#     res1 = (res[0], res[-1]*2)
#     print(list(res1))

# # list of numbers
# list1 = [10, 20, 4, 45, 99]
#
# # sorting the list
# list1.sort()
#
# # printing the last element
# print("Largest element is:", list1[-1])
#
# data = [("ABC", [20,21,22,23,24,25],"ABC"),
#         ("DEF",[19,20,50,51,52,53],"DEF")]
# schema = ["Name","Marks","Name"]
# df = spark.createDataFrame(data=data, schema = schema)
# df.show(truncate=False)
# col_select = list(set(df.columns))
# print(col_select)
# df_fin = df.select(col_select)
# # df_fin.show()
# # df.select(max("Marks")).show()
# #
# # lst1 = ["ABC",[20,21,22,28,24,25]]
# # lst2 = ["CDE",[19,20,50,51,52,53]]
# #
# # print(lst1,lst2)
#
# # res = lst1[0], max(lst1[1])
# # res1 = lst2[0], max(lst2[1])
# # print(res)
# # print(res1)

data = [("Doha","Qatar"),("Dubai","UAE"),("Doha","Qatar"),("KuwautCity","Kuwat"),("Doha","Qatar"),("Dubai","UAE")]
schema = ["store_city","store_country"]
df = spark.createDataFrame(data,schema)
df.show()
#
# +----------+-------------+
# |store_city|store_country|
# +----------+-------------+
# |      Doha|        Qatar|
# |     Dubai|          UAE|
# |      Doha|        Qatar|
# |KuwautCity|        Kuwat|
# |      Doha|        Qatar|
# |     Dubai|          UAE|
# +----------+-------------+

df_cty = df.select(collect_set("store_city").alias("arr_city")) #[Doha, Dubai, Kuawait]

df_str_cty = df.select(collect_set("store_country").alias("arr_crty")) #[Qatar, UAE, Kuwat]

df_cty.printSchema()
df_cty.show()
df_str_cty.printSchema()
df_str_cty.show()

df_cty = df_cty.select(explode("arr_city")).withColumn("col2",lit("store_city"))
df_str_cty = df_str_cty.select(explode("arr_crty")).withColumn("col2",lit("store_country"))
df_cty.show()
df_str_cty.show()

df_union = df_cty.union(df_str_cty)
df_union.show()
#
# pivotDF = df.groupBy("store_country").pivot("store_city").sort("store_country")
# pivotDF.show(truncate=False)

# data = [(1,"x",10),(2,"A",8),(3,"B",7),(4,"C",6),(5,"D",5),(6,"E",4)]
# schema = ["id","name","value"]


# df1 = spark.createDataFrame(data=data, schema = schema)
# df1.printSchema()
# df1.show(truncate=False)
#
# data = [(3,"B",9),(4,"C",8)]
# schema = ["id","name","value"]
# df2 = spark.createDataFrame(data=data, schema = schema)
# df2.printSchema()
# df2.show(truncate=False)
#
# df1.join(df2,df1.id==df2.id,"leftanti").show()


# data = [(1, "A", "Maths", 65), (1, "A", "Phy", 63), (2, "B", "Maths", 75), (2, "B", "Hindi", 48),
#         (3, "C", "Humanity", 88)]
# schema = ["id", "name", "subject", "marks"]
#
# df = spark.createDataFrame(data, schema)
# df.show()
#
# # df.select("name","marks").groupBy("name").sum("marks").alias("Total_marks").show()
# df.groupBy("name").sum("marks").alias("Totalmarks").show()
#
# data = ["abc-kyrt-45", "Xe4tgrj-ghy", "Stren-gty-rtu-qtt"]
# schema = ["Input"]
#
# df2 = spark.createDataFrame(data,schema)
# df2 = df.select(split(col("name"),",").alias("NameArray")) \
# #     .drop("name")
# df2.show()
