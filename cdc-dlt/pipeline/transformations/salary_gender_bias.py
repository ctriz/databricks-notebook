import dlt
from pyspark.sql.functions import avg, col

@dlt.table(name="cdcproject.dltscd.clean_gold_sal_gender")
def clean_gold_sal_gender():
    return (
        spark.table("cdcproject.dltscd.target_scd_v2")
        .filter((col("__END_AT").isNull()) & 
                (col("is_deleted") == False) &
                (col("salary").isNotNull()) &
                (col("salary") != "0") &
                (col("salary").cast("double") != 0)
        )
        .groupBy("gender")
        .agg(avg(col("salary").cast("double")).alias("avg_salary"))
    )
