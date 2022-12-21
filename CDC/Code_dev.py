import os
from datetime import datetime

from pyspark.sql.functions import col, lit, to_date
from pyspark.sql import SparkSession
from cdc_create_db import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName('Testing').getOrCreate()
print(spark)

folderPath = 'C:\\Users\\mura\\PycharmProjects\\pythonProject\\src\\cdc_res'
date_infile = '20221026'
print(folderPath)
print(date_infile)

######################list comprehension###############################
##if use below list comprehesion, out will be in list,
## we cannot concatinate list .so use normal for loop
# output looks like - ['cdc_file_day_1_20221026.csv']
##    file_dir = folderPath + '\\' + file_input_file
##TypeError: can only concatenate str (not "list") to str

# file_input_file = [filename for filename in os.listdir(folderPath) if date_infile in filename]
# print(file_input_file)
# file_dir = folderPath + '\\' + file_input_file
# print(file_dir)
#######################################################################

landingFileSchema = StructType((
    StructField("empid", IntegerType(), True),
    StructField("epname", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("add", StringType(), True),
    StructField("phonenum", StringType(), True),
))

for file in os.listdir(folderPath):
    print("File is:", file)
    if date_infile in file:
        file_dir = folderPath + '\\' + file
        print(file_dir)

print("####input File Path is " + file_dir)
df = spark.read.options(header='true', inferSchema='true', schema=landingFileSchema).csv(file_dir)
df.show()

date_convert = datetime.strptime(date_infile, '%Y%m%d').date()
print(date_convert)  # printed in default formatting

create_db_tbl_flag = create_db_tbl(spark)
print(create_db_tbl_flag)

# print("####show database and tables########")
# spark.sql("""show databases""").show()
# spark.sql("""show tables""").show()
#
# print("###inserting date into emp_hist table######")
# df2.write.insertInto('default.emp_hist', overwrite=True)
#
# print("####### select emp_hist table after inserting data #########")
# spark.sql("""select * from default.emp_hist""").show()

df_com_master = spark.sql("""select * from emp_master""")

print("lenght of master data" ,len(df_com_master.head(1)))
df_final = df
if len(df_com_master.head(1)) == 0:
    print("proccesing for day 0")
    print("load into hist")
    df_final = df_final.withColumn('flag', lit('N'))
    df_final = df_final.withColumn('curr_date', lit(date_convert))
    #df_final = df_final.select(col("empid"),col("epname"),col("salary"),col("add"),col("phonenum"),col("flag"),col("curr_date"))
    df_final.show()
    spark.sql("""describe default.emp_hist""").show()
    df_final.write.mode('overwrite').format('csv').saveAsTable('default.emp_hist')
    df_final.write.mode('overwrite').format('csv').saveAsTable('default.emp_temp')
    df_temp = spark.sql("""select * from default.emp_temp""")
    df_temp.write.mode('overwrite').format('csv').saveAsTable('default.emp_master')
    print("verfiying")
    print("history")
    spark.sql("""select * from emp_hist where curr_date = '2022-10-26'""").show()
    spark.sql("""select * from emp_master""").show()
else:
    print('running from day 1 onwoards')
    # updated, new , deleted
    #hasing both df's

    df_final = df_final.withColumn("hash_val", md5(*cols))
    df_final.show()









