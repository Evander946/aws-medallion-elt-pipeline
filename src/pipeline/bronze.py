"""Bronze: land raw files as-is (all strings) plus ingestion metadata. No business logic here."""
from pyspark.sql import DataFrame, SparkSession, functions as F


def ingest(spark: SparkSession, raw_path: str, ingest_date: str) -> DataFrame:
    df = (
        spark.read.option("header", True).option("inferSchema", False).csv(raw_path)
        .withColumn("_ingest_date", F.lit(ingest_date))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )
    return df


def write(df: DataFrame, target: str) -> None:
    df.write.mode("overwrite").partitionBy("_ingest_date").parquet(target)
