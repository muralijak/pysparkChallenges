
def create_db_tbl(spark):
    # check if hist and master are exists in DB
    show_db = spark.sql("""show SCHEMAS""")
    show_db.show()

    # create_db = spark.sql("""create database if not exists cdc_db""")
    # show_db2 = spark.sql("""DESCRIBE DATABASE EXTENDED cdc_db""")
    # show_db2.show()

    spark.sql("""use default""")

    spark.sql("""show tables""").show()

    spark.sql("""create table if not exists emp_hist
    (empid	INT,
    epname STRING,
    salary INT,
    add STRING,
    phonenum STRING, curr_date DATE, flag STRING) USING CSV
    PARTITIONED BY (curr_date)
    """)

    spark.sql("""create table if not exists emp_master
    (empid	INT,
    epname STRING,
    salary INT,
    add STRING,
    phonenum STRING, curr_date DATE, flag STRING) USING CSV
     """)

    print("listing tables  ")
    spark.sql("""show tables""").show()
    return 0

