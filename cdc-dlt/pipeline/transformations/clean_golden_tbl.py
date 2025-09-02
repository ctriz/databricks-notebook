import dlt
from pyspark.sql.functions import col, lit

@dlt.table(name="cdcproject.dltscd.gold_clean")
def gold_clean():
    return (
        spark.table("cdcproject.dltscd.target_scd_v2")
        .filter(
            (col("__END_AT").isNull()) & 
            (col("is_deleted") == False)
        )
        # Optionally, select only the business columns you need
        .select("id", "firstName", "salary", "gender", "surrogate_key")
    )
