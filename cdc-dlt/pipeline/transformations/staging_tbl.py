import dlt
from pyspark.sql.functions import col, current_timestamp, lit, sha2, concat_ws

expectations = {
    "valid_id": "id IS NOT NULL",
    "valid_firstName": "firstName IS NOT NULL",
    "non_negative_salary": "salary >= 0"
}

@dlt.table(
    name="cdcproject.dltscd.staging_processed",
    comment="Staging table with transformations for SCD2 processing"
)
@dlt.expect_all_or_drop(expectations)
def create_staging_table():
    df = spark.readStream.format("delta").option("skipChangeCommits","true").table("<catalog_name>.<schema_name>.raw_data_bronze")
    transformed_df = (
        df.withColumn("effective_ts", current_timestamp())
          .withColumn("expiry_ts", current_timestamp())
          .withColumn("is_deleted", lit(False).cast("boolean"))
          .withColumn("row_num", lit(1).cast("integer"))
          .withColumn(
              "surrogate_key",
              sha2(concat_ws("||", col("id"), col("effective_ts")), 256).cast("int")
          )
          .withColumn("salary", col("salary").cast("string"))
          .select(
              "id", "firstName", "salary", "gender",
              "surrogate_key", "is_deleted", "row_num",
              "effective_ts", "expiry_ts"
          )
    )
    return transformed_df