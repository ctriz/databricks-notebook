# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql.functions import lit, current_timestamp

# Load incoming changes (from file, API, etc.)
changes_df = spark.read.csv("/<path>/vol/changes.csv", header=True)\
    .withColumn("effective_ts", current_timestamp()) \
    .withColumn("is_deleted", lit(False))  # or True for deletes

# Reference your bronze Delta table
bronze_table = DeltaTable.forName(spark, "cdcproject.dltscd.raw_data_bronze")

# MERGE logic to apply inserts, updates, and deletes
bronze_table.alias("b") \
    .merge(
        changes_df.alias("c"),
        "b.id = c.id"
    ) \
    .whenMatchedUpdate(set={
        "firstName": "c.firstName",
        "salary": "c.salary",
        "is_deleted": "c.is_deleted",
        "effective_ts": "c.effective_ts"
    }) \
    .whenNotMatchedInsert(values={
        "id": "c.id",
        "firstName": "c.firstName",
        "salary": "c.salary",
        "is_deleted": "c.is_deleted",
        "effective_ts": "c.effective_ts"
    }) \
    .execute()
