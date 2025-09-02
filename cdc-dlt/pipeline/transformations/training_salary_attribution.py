import dlt
from pyspark.sql.functions import col

@dlt.table(name="cdcproject.dltscd.emp_training_salary")
def emp_training_salary():
    # Load current active employees (clean golden)
    emp_df = (
      spark.table("cdcproject.dltscd.target_scd_v2")
      .filter((col("__END_AT").isNull()) & (col("is_deleted") == False))
      .select("id", "salary")
    )
    
    # Load training data
    training_df = spark.table("cdcproject.dltscd.emp_training").select("id", "training_hours", "training_type")
    
    # Join on employee id
    joined_df = emp_df.join(training_df, on="id", how="left")
    
    # Select columns and cast salary as double for analysis
    return joined_df.select(
        "id",
        "training_hours",
        "training_type",
        col("salary").cast("double").alias("salary")
    )
