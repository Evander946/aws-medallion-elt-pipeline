"""Silver: typed, cleaned, de-duplicated, validated. Bad rows go to a quarantine table with a reason."""
from typing import Tuple

from pyspark.sql import DataFrame, Window, functions as F

VALID_STATUSES = ["placed", "shipped", "delivered", "cancelled", "returned"]
TS_FMT = "yyyy-MM-dd HH:mm:ss"


def clean_orders(bronze: DataFrame) -> Tuple[DataFrame, DataFrame]:
    typed = (
        bronze
        .withColumn("customer_id", F.nullif(F.trim("customer_id"), F.lit("")))
        .withColumn("status", F.lower(F.trim("status")))
        .withColumn("amount", F.col("amount").cast("decimal(12,2)"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("order_ts", F.to_timestamp("order_ts", TS_FMT))
        .withColumn("updated_at", F.to_timestamp("updated_at", TS_FMT))
    )

    reason = (
        F.when(F.col("order_id").isNull(), "missing_order_id")
        .when(F.col("customer_id").isNull(), "missing_customer_id")
        .when(F.col("order_ts").isNull(), "invalid_order_ts")
        .when(F.col("amount").isNull() | (F.col("amount") <= 0), "invalid_amount")
        .when(~F.col("status").isin(VALID_STATUSES), "invalid_status")
    )
    flagged = typed.withColumn("_reject_reason", reason)

    quarantine = flagged.filter("_reject_reason IS NOT NULL")
    valid = flagged.filter("_reject_reason IS NULL").drop("_reject_reason")

    # Keep the most recent version of each order (late-arriving updates win).
    w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc(), F.col("_ingested_at").desc())
    deduped = (
        valid.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
        .withColumn("order_date", F.to_date("order_ts"))
    )
    return deduped, quarantine


def clean_customers(bronze: DataFrame) -> DataFrame:
    return (
        bronze
        .withColumn("country", F.upper(F.trim("country")))
        .withColumn("signup_date", F.to_date("signup_date"))
        .dropDuplicates(["customer_id"])
    )
