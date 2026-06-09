import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, to_timestamp, hour, dayofweek, month,
    when, lit, round as spark_round, sum as spark_sum,
    count, countDistinct, avg, max as spark_max, min as spark_min,
    row_number, datediff, current_date
)
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType, IntegerType, BooleanType, DateType

from config.settings import SPARK_CONFIG, HDFS_BASE_PATH


def create_spark_session():
    spark = SparkSession.builder \
        .appName(SPARK_CONFIG["app_name"]) \
        .master(SPARK_CONFIG["master"]) \
        .config("spark.executor.memory", SPARK_CONFIG["spark.executor.memory"]) \
        .config("spark.driver.memory", SPARK_CONFIG["spark.driver.memory"]) \
        .config("spark.sql.shuffle.partitions", SPARK_CONFIG["spark.sql.shuffle.partitions"]) \
        .config("spark.sql.adaptive.enabled", SPARK_CONFIG["spark.sql.adaptive.enabled"]) \
        .config("spark.sql.adaptive.coalescePartitions.enabled",
                SPARK_CONFIG["spark.sql.adaptive.coalescePartitions.enabled"]) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_raw_data(spark, data_dir):
    data_path = Path(data_dir) / "raw"
    dfs = {}
    file_map = {
        "order": "fact_order.csv",
        "inventory": "fact_inventory.csv",
        "behavior": "fact_user_behavior.csv",
        "product": "dim_product.csv",
        "store": "dim_store.csv",
        "user": "dim_user.csv"
    }
    for name, filename in file_map.items():
        fp = data_path / filename
        if fp.exists():
            dfs[name] = spark.read.option("header", True).option("inferSchema", True).csv(str(fp))
            print(f"[LOAD] {filename} → {dfs[name].count():,} rows")
        else:
            print(f"[WARN] {filename} not found at {fp}")
    return dfs


def clean_order_data(df):
    df = df.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
           .withColumn("order_dayofweek", dayofweek(col("order_date"))) \
           .withColumn("order_month", month(col("order_date"))) \
           .withColumn("discount_rate",
                        when(col("total_amount") > 0,
                             spark_round(col("discount_amount") / col("total_amount"), 3))
                        .otherwise(lit(0))) \
           .dropna(subset=["order_id", "user_id", "product_id", "order_date"])
    df = df.withColumn("order_dayofweek", col("order_dayofweek").cast(IntegerType())) \
           .withColumn("order_month", col("order_month").cast(IntegerType())) \
           .withColumn("discount_rate", col("discount_rate").cast(DecimalType(5, 3)))
    return df


def clean_inventory_data(df):
    df = df.withColumn("snapshot_date", to_date(col("snapshot_date"), "yyyy-MM-dd")) \
           .withColumn("is_understock",
                        when(col("stock_qty") < col("safety_stock"), lit(True))
                        .otherwise(lit(False))) \
           .withColumn("stock_turnover_ratio",
                        when(col("reorder_point") > 0,
                             spark_round(col("stock_qty") / col("reorder_point"), 2))
                        .otherwise(lit(0))) \
           .dropna(subset=["snapshot_date", "store_id", "product_id"])
    df = df.withColumn("is_understock", col("is_understock").cast(BooleanType())) \
           .withColumn("stock_turnover_ratio", col("stock_turnover_ratio").cast(DecimalType(8, 2)))
    return df


def clean_behavior_data(df):
    df = df.withColumn("event_datetime", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss")) \
           .withColumn("event_date", to_date(col("event_datetime"))) \
           .withColumn("event_hour", hour(col("event_datetime"))) \
           .dropna(subset=["event_id", "user_id", "event_datetime"])
    df = df.withColumn("is_converted", lit(False).cast(BooleanType()))
    df = df.withColumn("event_hour", col("event_hour").cast(IntegerType()))
    return df


def run_etl(spark, dfs, output_dir):
    processed_path = Path(output_dir) / "processed"
    processed_path.mkdir(parents=True, exist_ok=True)

    results = {}

    if "order" in dfs:
        print("\n[DWD] 清洗订单明细...")
        dwd_order = clean_order_data(dfs["order"])
        dwd_order.write.mode("overwrite").parquet(str(processed_path / "dwd_order_detail"))
        results["dwd_order_detail"] = dwd_order

    if "inventory" in dfs:
        print("\n[DWD] 清洗库存快照...")
        dwd_inv = clean_inventory_data(dfs["inventory"])
        dwd_inv.write.mode("overwrite").parquet(str(processed_path / "dwd_inventory_detail"))
        results["dwd_inventory_detail"] = dwd_inv

    if "behavior" in dfs:
        print("\n[DWD] 清洗用户行为...")
        dwd_behavior = clean_behavior_data(dfs["behavior"])
        dwd_behavior.write.mode("overwrite").parquet(str(processed_path / "dwd_user_behavior"))
        results["dwd_user_behavior"] = dwd_behavior

    print("\n=== ETL 清洗完成 ===")
    return results


if __name__ == "__main__":
    spark = create_spark_session()
    data_dir = str(Path(__file__).resolve().parents[1] / "data")
    output_dir = str(Path(__file__).resolve().parents[1] / "data")
    print(f"数据目录: {data_dir}")
    dfs = load_raw_data(spark, data_dir)
    results = run_etl(spark, dfs, output_dir)
    spark.stop()
