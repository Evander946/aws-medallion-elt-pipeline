import tempfile
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from src.pipeline import bronze, gold, silver
from src.pipeline.generate_data import generate
from src.pipeline.run import main
from src.pipeline.spark import get_spark


@pytest.fixture(scope="module")
def spark():
    s = get_spark("tests")
    yield s
    s.stop()


@pytest.fixture(scope="module")
def raw_dir():
    d = Path(tempfile.mkdtemp()) / "raw"
    generate(d, n_customers=50, n_orders=500, seed=7)
    return d


def test_bronze_keeps_everything_as_strings(spark, raw_dir):
    df = bronze.ingest(spark, str(raw_dir / "orders.csv"), "2026-11-26")
    assert all(t == "string" for n, t in df.dtypes if not n.startswith("_"))
    assert {"_ingest_date", "_ingested_at", "_source_file"} <= set(df.columns)


def test_silver_dedupes_and_quarantines(spark, raw_dir):
    b = bronze.ingest(spark, str(raw_dir / "orders.csv"), "2026-11-26")
    orders, quarantine = silver.clean_orders(b)
    assert orders.count() == orders.select("order_id").distinct().count()      # no duplicate keys
    assert orders.filter("amount <= 0 OR customer_id IS NULL OR order_ts IS NULL").count() == 0
    reasons = {r[0] for r in quarantine.select("_reject_reason").distinct().collect()}
    assert {"invalid_amount", "missing_customer_id", "invalid_order_ts"} <= reasons
    assert orders.filter(F.col("status") != F.lower("status")).count() == 0    # casing normalised


def test_silver_keeps_latest_version(spark):
    rows = [
        ("O1", "C1", "placed", "10.00", "1", "2026-01-01 10:00:00", "2026-01-01 10:00:00"),
        ("O1", "C1", "delivered", "10.00", "1", "2026-01-01 10:00:00", "2026-01-03 09:00:00"),
    ]
    cols = ["order_id", "customer_id", "status", "amount", "quantity", "order_ts", "updated_at"]
    df = (spark.createDataFrame(rows, cols)
          .withColumn("_ingested_at", F.current_timestamp()))
    out, _ = silver.clean_orders(df)
    assert out.count() == 1 and out.first()["status"] == "delivered"


def test_gold_excludes_cancelled_and_returned(spark):
    rows = [
        ("O1", "C1", "delivered", "100.00", "1", "2026-01-01 10:00:00", "2026-01-01 10:00:00"),
        ("O2", "C1", "cancelled", "50.00", "1", "2026-01-01 11:00:00", "2026-01-01 11:00:00"),
    ]
    cols = ["order_id", "customer_id", "status", "amount", "quantity", "order_ts", "updated_at"]
    b = spark.createDataFrame(rows, cols).withColumn("_ingested_at", F.current_timestamp())
    orders, _ = silver.clean_orders(b)
    day = gold.daily_sales(orders).first()
    assert day["orders"] == 1 and float(day["revenue"]) == 100.0


def test_end_to_end_reconciles(raw_dir):
    lake = str(Path(tempfile.mkdtemp()) / "lake")
    stats = main(str(raw_dir), lake, "2026-11-26")
    # bronze rows = kept + quarantined + superseded duplicate versions
    assert stats["silver_orders"] + stats["quarantined"] <= stats["bronze_orders"]
    assert stats["silver_orders"] > 0 and stats["quarantined"] > 0
