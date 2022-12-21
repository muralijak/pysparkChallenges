from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, expr, round, dayofweek, weekofyear, to_date, countDistinct, year

spark = SparkSession.builder.master('local[*]').appName("Aggregate_Functions").enableHiveSupport().getOrCreate()

print(spark)

df = spark.read.csv('C:\\Users\\mura\\PycharmProjects\\pythonProject\\src\invoices.csv', header='true')
df.show()

df.createOrReplaceTempView("invoice_tbl")

# df_result = spark.sql("select count(*) as cnt, count(StockCode), sum(Quantity), avg(UnitPrice) from invoice_tbl")
# df_result.show()
#
# df.select(count(col("StockCode")).alias("cnt"), sum(col("Quantity"))).show()
#
# df.selectExpr("count(StockCode) as cnt",
#               "sum(Quantity) as Total_Quantity",
#               "avg(Quantity) as avarage",
#               "round(sum(Quantity * UnitPrice),2) as Invoice_Value").show()
#
# spark.sql("""
#             select Country,InvoiceNo, sum(Quantity) as total_qty, avg(UnitPrice) as avarge_price,
#             round(sum(Quantity * UnitPrice),2) as Invoice_Value
#             from invoice_tbl
#             group by Country, InvoiceNo
#              """).show()
#
# df.groupBy("Country", "InvoiceNo") \
#     .agg(sum("Quantity").alias("Total_sum"), round(sum(expr("Quantity * UnitPrice")), 2).alias("Invoice_value")) \
#     .show()
# # any any of the below/above code
# df.groupBy("Country", "InvoiceNo") \
#     .agg(sum("Quantity").alias("Total_sum"), expr("round(sum(Quantity * UnitPrice), 2) as Invoice_value")) \
#     .show()

df_agg = df.withColumn("InvoiceDate", to_date(col("InvoiceDate"), 'dd-MM-yyyy H.mm')) \
    .withColumn("WeekNumber", weekofyear("InvoiceDate")) \
    .groupBy("Country", "WeekNumber") \
    .agg(countDistinct("InvoiceNo").alias("Numinvoices"), sum("Quantity").alias("Total_sum"),
         expr("round(sum(Quantity * UnitPrice), 2) as Invoice_value"))

## or ###
NumInvoices = countDistinct("InvoiceNo").alias("Numinvoices")
Total_quantity = sum("Quantity")
Total_Invoice = expr("round(sum(Quantity * UnitPrice), 2) as Invoice_value")

df_agg1 = df.withColumn("InvoiceDate", to_date(col("InvoiceDate"), 'dd-MM-yyyy H.mm')) \
    .where("year(InvoiceDate) == 2010") \
    .withColumn("WeekNumber", weekofyear("InvoiceDate")) \
    .groupBy("Country", "WeekNumber") \
    .agg(NumInvoices, Total_quantity, Total_Invoice)

df_agg1.sort("Country", "WeekNumber").show()




