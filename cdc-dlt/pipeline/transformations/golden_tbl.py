import dlt
from pyspark.sql.functions import col, expr, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType

# 1. Define a view for incoming snapshots
@dlt.view
def cdctablev2():
    return (
        spark.readStream
        .option("skipChangeCommits", "true")
        .table("<catalog_name>.<schema_name>.staging_processed")
    )
# skipChangeCommits set to true when reading the Delta table as a stream source to ignore updates and deletes, which are not supported in streaming reads.
# Alternative: Use streaming staging
# return spark.readStream.table("cdcproject.dltscd.staging_streaming")


# 2. Create the SCD2 history table
dlt.create_streaming_table(
    name="<catalog_name>.<schema_name>.target_scd_v2",
    schema="""
        id INTEGER,
        firstName STRING,
        salary STRING,
        gender STRING,
        surrogate_key INTEGER,
        is_deleted BOOLEAN,
        row_num INTEGER DEFAULT 1,
        effective_ts TIMESTAMP DEFAULT current_timestamp(),
        expiry_ts TIMESTAMP DEFAULT current_timestamp(),
        __START_AT TIMESTAMP,
        __END_AT TIMESTAMP
    """,
    table_properties={
        "delta.feature.allowColumnDefaults": "supported",
        "delta.minReaderVersion": "2", 
        "delta.minWriterVersion": "7"
    }
)

# 3. Apply changes to maintain SCD2, handling deletes with the is_delete flag
dlt.create_auto_cdc_flow(
    target="<catalog_name>.<schema_name>.target_scd_v2",
    source="cdctablev2",
    keys=["id"],
    sequence_by=col("effective_ts"), # Use a timestamp column for ordering
    apply_as_deletes=expr("is_deleted = true"), # If is_delete == True => expire current version
    except_column_list=["effective_ts", "expiry_ts"], # Exclude technical columns from target
    stored_as_scd_type="2"  # SCD2: keeps history via __START_AT and __END_AT
)