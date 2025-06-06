from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.functions import col, when, count, abs, datediff, row_number
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from dotenv import load_dotenv
import os

def convert_to_double(df):
    df = df.withColumn("revenue", regexp_replace("revenue", r"[\$.]", "")) \
               .withColumn("revenue", regexp_replace("revenue", ",", ".")) \
               .withColumn("revenue", col("revenue").cast("double")) \
               .withColumn("cogs", regexp_replace("cogs", r"[\$.]", "")) \
               .withColumn("cogs", regexp_replace("cogs", ",", ".")) \
               .withColumn("cogs", col("cogs").cast("double"))
    return df

def fill_missing_revenue_and_cogs(df):
    df_null = df.filter(F.col("revenue").isNull())
    df_no_null = df.filter(F.col("revenue").isNotNull())
    joined_df = df_null.alias("a").join(
        df_no_null.alias("b"),
        on=F.col("a.productid") == F.col("b.productid"),
        how="inner"
    )
    joined_df = joined_df.withColumn(
        "diff_date",
        abs(datediff(F.col("a.date"), F.col("b.date")))
    )

    window_spec = Window.partitionBy("a.productid", "a.date").orderBy("diff_date")

    joined_df = joined_df.withColumn(
        "row_num",
        row_number().over(window_spec)
    ).filter(F.col("row_num") == 1)

    joined_final = joined_df.select(
        F.col("a.productid").alias("productid"),
        F.col("a.date").alias("date"),
        F.col("a.zip"),
        F.col("a.units"),
        F.col("b.revenue").alias("revenue"), 
        F.col("b.cogs")
    )

    final_df = df_no_null.union(joined_final)
    return final_df

# Load biến môi trường từ .env
load_dotenv()

# postgres config
database_name = os.getenv("database_name")
port = os.getenv("port")
table_name_1 = os.getenv("table_name_1")
table_name_2 = os.getenv("table_name_2")
table_name_3 = os.getenv("table_name_3")
user = os.getenv("user")
password = os.getenv("password")

#snowflake conector config
sfOptions = {
    "sfURL": os.getenv("SF_URL"),
    "sfUser": os.getenv("SF_USER"),
    "sfPassword": os.getenv("SF_PASSWORD"),
    "sfDatabase": os.getenv("SF_DATABASE"),
    "sfSchema": os.getenv("SF_SCHEMA"),
    "sfWarehouse": os.getenv("SF_WAREHOUSE"),
    "sfRole": os.getenv("SF_ROLE"),
    "insecureMode": "true"
}

def main():
    spark = SparkSession.builder \
    .appName("Postgres and Snowflake") \
    .config("spark.jars", "/home/hadoop/FORECASTING BUSINESS PERFORMANCE/scripts/spark/jars/postgresql-42.6.0.jar") \
    .config("spark.jars.packages",
            "net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3,"
            "net.snowflake:snowflake-jdbc:3.13.22") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.memoryOverhead", "2g") \
    .getOrCreate()

    jdbc_url = f"jdbc:postgresql://localhost:{port}/{database_name}"

    # Cấu hình properties cho Spark JDBC
    properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
    }

    try:
        df_salesfact = spark.read.jdbc(url=jdbc_url, table=table_name_1, properties=properties)
        df_product = spark.read.jdbc(url=jdbc_url, table=table_name_2, properties=properties)
        df_geography = spark.read.jdbc(url=jdbc_url, table=table_name_3, properties=properties)
    except Exception as e:
        print(f"Error: {e}")

    df_salesfact = convert_to_double(df_salesfact)
    df_salesfact = df_salesfact.dropDuplicates()
    df_salesfact = fill_missing_revenue_and_cogs(df_salesfact)

    print("==== TRANFORM SUCCESS ====")

    tables = {
    "salesfact": df_salesfact,
    "product": df_product,
    "geography": df_geography
    }

    for table_name, df in tables.items():
        df.write \
        .format("snowflake") \
        .options(**sfOptions) \
        .option("dbtable", table_name) \
        .mode("overwrite") \
        .save()
        print(f"Đã ghi bảng {table_name} vào Snowflake")

if __name__ == "__main__":
    main()