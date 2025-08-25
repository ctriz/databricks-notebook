# Databricks notebook source
# Read from a file
source_df = spark.read.format("csv").option("header", True).load("/<path>/vol/export.csv")

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("firstName", StringType(), True),
    StructField("middleName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birthDate", DateType(), True),
    StructField("ssn", StringType(), True),
    StructField("salary", IntegerType(), True)
])

from pyspark.sql.functions import lit, col, current_timestamp

# Add default system/control columns per SCD type 2 specs
source_df = source_df.withColumn('id', col('id').cast(IntegerType())) \
    .withColumn('effective_ts', current_timestamp()) \
    .withColumn('expiry_ts', current_timestamp()) \
    .withColumn('is_deleted', lit(False)) \
    .withColumn("source_file", col("_metadata.file_path")) \
    .withColumn("is_processed", lit(False))

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Add auto-incrementing surrogate key
window_spec = Window.orderBy("effective_ts")
source_df = source_df.withColumn("surrogate_key", row_number().over(window_spec))

display(source_df)

spark.sql("CREATE SCHEMA IF NOT EXISTS <catalog_name>.<schema_name>")

source_df.writeTo("<catalog_name>.<schema_name>.dltscd.raw_data_bronze").createOrReplace()