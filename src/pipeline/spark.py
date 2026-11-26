"""Spark session factory. Local by default; on AWS Glue the session is provided by the job runtime."""
from pyspark.sql import SparkSession


def get_spark(app_name: str = "medallion-elt") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
