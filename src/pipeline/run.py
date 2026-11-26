"""Run the full Bronze -> Silver -> Gold batch locally (or against S3 paths on AWS).

    python -m src.pipeline.run --raw data/raw --lake data/lake --ingest-date 2026-11-26
"""
import argparse

from src.pipeline import bronze, gold, silver
from src.pipeline.spark import get_spark


def main(raw: str, lake: str, ingest_date: str) -> dict:
    spark = get_spark()
    lake = lake.rstrip("/")

    b_orders = bronze.ingest(spark, f"{raw}/orders.csv", ingest_date)
    b_customers = bronze.ingest(spark, f"{raw}/customers.csv", ingest_date)
    bronze.write(b_orders, f"{lake}/bronze/orders")
    bronze.write(b_customers, f"{lake}/bronze/customers")

    orders, quarantine = silver.clean_orders(spark.read.parquet(f"{lake}/bronze/orders"))
    customers = silver.clean_customers(spark.read.parquet(f"{lake}/bronze/customers"))
    orders.write.mode("overwrite").partitionBy("order_date").parquet(f"{lake}/silver/orders")
    quarantine.write.mode("overwrite").parquet(f"{lake}/silver/orders_quarantine")
    customers.write.mode("overwrite").parquet(f"{lake}/silver/customers")

    s_orders = spark.read.parquet(f"{lake}/silver/orders")
    s_customers = spark.read.parquet(f"{lake}/silver/customers")
    gold.daily_sales(s_orders).write.mode("overwrite").parquet(f"{lake}/gold/daily_sales")
    gold.customer_summary(s_orders, s_customers).write.mode("overwrite").parquet(f"{lake}/gold/customer_summary")

    stats = {
        "bronze_orders": b_orders.count(),
        "silver_orders": s_orders.count(),
        "quarantined": quarantine.count(),
    }
    # Reconciliation: every bronze row is either kept, rejected, or removed as a duplicate version.
    print(stats)
    return stats


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default="data/raw")
    ap.add_argument("--lake", default="data/lake")
    ap.add_argument("--ingest-date", required=True)
    a = ap.parse_args()
    main(a.raw, a.lake, a.ingest_date)
