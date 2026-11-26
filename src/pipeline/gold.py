"""Gold: business-ready aggregates built from Silver."""
from pyspark.sql import DataFrame, functions as F


def daily_sales(orders: DataFrame) -> DataFrame:
    """Revenue excludes cancelled and returned orders."""
    net = orders.filter(~F.col("status").isin("cancelled", "returned"))
    return (
        net.groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.sum(F.col("amount") * F.col("quantity")).alias("revenue"),
            F.avg("amount").alias("avg_order_value"),
        )
        .withColumn("revenue", F.round("revenue", 2))
        .withColumn("avg_order_value", F.round("avg_order_value", 2))
        .orderBy("order_date")
    )


def customer_summary(orders: DataFrame, customers: DataFrame) -> DataFrame:
    net = orders.filter(~F.col("status").isin("cancelled", "returned"))
    agg = net.groupBy("customer_id").agg(
        F.countDistinct("order_id").alias("lifetime_orders"),
        F.round(F.sum(F.col("amount") * F.col("quantity")), 2).alias("lifetime_value"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
    )
    return customers.select("customer_id", "country").join(agg, "customer_id", "left").fillna(
        {"lifetime_orders": 0, "lifetime_value": 0}
    )
