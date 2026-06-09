import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, lag, lead, datediff, current_date,
    when, lit, round as spark_round, sum as spark_sum,
    count, countDistinct, avg, stddev, max as spark_max, min as spark_min,
    weekofyear, month, dayofweek, dayofyear, year,
    monotonically_increasing_id, row_number
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline

from config.settings import SPARK_CONFIG


def create_spark_session():
    spark = SparkSession.builder \
        .appName(f"{SPARK_CONFIG['app_name']}_FeatureEngineering") \
        .master(SPARK_CONFIG["master"]) \
        .config("spark.executor.memory", SPARK_CONFIG["spark.executor.memory"]) \
        .config("spark.driver.memory", SPARK_CONFIG["spark.driver.memory"]) \
        .config("spark.sql.shuffle.partitions", SPARK_CONFIG["spark.sql.shuffle.partitions"]) \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_processed_data(spark, data_dir):
    base = Path(data_dir) / "processed"
    dfs = {}
    path_map = {
        "order": base / "dwd_order_detail",
        "inventory": base / "dwd_inventory_detail",
        "behavior": base / "dwd_user_behavior"
    }
    for name, path in path_map.items():
        if path.exists():
            dfs[name] = spark.read.parquet(str(path))
            print(f"[LOAD] {name} → {dfs[name].count():,} rows, {len(dfs[name].columns)} cols")
        else:
            print(f"[WARN] {path} not found")
    return dfs


def build_sales_features(df_order):
    print("\n[FEATURE] 构建销量预测特征...")

    window_sales = Window.partitionBy("product_id").orderBy("order_date")

    daily_sales = df_order.groupBy("product_id", "order_date").agg(
        spark_sum("quantity").alias("sales_qty"),
        spark_sum("pay_amount").alias("daily_gmv"),
        count("order_id").alias("order_count"),
        countDistinct("user_id").alias("user_count"),
        avg("discount_rate").alias("avg_discount_rate")
    )

    daily_sales = daily_sales.withColumn("product_id", col("product_id")) \
        .withColumn("order_date", col("order_date"))

    daily_sales = daily_sales.withColumn("sales_lag_1",
                                          lag("sales_qty", 1).over(window_sales)) \
                             .withColumn("sales_lag_7",
                                          lag("sales_qty", 7).over(window_sales)) \
                             .withColumn("sales_lag_14",
                                          lag("sales_qty", 14).over(window_sales)) \
                             .withColumn("sales_lag_30",
                                          lag("sales_qty", 30).over(window_sales)) \
                             .withColumn("sales_rolling_7d_avg", avg("sales_qty")
                                          .over(window_sales.rowsBetween(-6, 0))) \
                             .withColumn("sales_rolling_14d_avg", avg("sales_qty")
                                          .over(window_sales.rowsBetween(-13, 0))) \
                             .withColumn("sales_rolling_30d_avg", avg("sales_qty")
                                          .over(window_sales.rowsBetween(-29, 0))) \
                             .withColumn("dayofweek", dayofweek("order_date")) \
                             .withColumn("month", month("order_date")) \
                             .withColumn("weekofyear", weekofyear("order_date"))

    daily_sales = daily_sales.withColumn("dayofweek", col("dayofweek").cast(IntegerType())) \
                             .withColumn("month", col("month").cast(IntegerType())) \
                             .withColumn("weekofyear", col("weekofyear").cast(IntegerType()))

    daily_sales = daily_sales.withColumn("row_id", monotonically_increasing_id())
    print(f"  销量特征样本: {daily_sales.count():,} rows")
    return daily_sales


def build_inventory_features(df_inventory):
    print("\n[FEATURE] 构建库存特征...")

    window_inv = Window.partitionBy("product_id", "store_id").orderBy("snapshot_date")

    df_inv = df_inventory.withColumn("stock_lag_1", lag("stock_qty", 1).over(window_inv)) \
                         .withColumn("waste_lag_1", lag("waste_qty", 1).over(window_inv)) \
                         .withColumn("stock_change_1d",
                                      col("stock_qty") - lag("stock_qty", 1).over(window_inv)) \
                         .withColumn("waste_rate",
                                      when(col("stock_qty") > 0,
                                           spark_round(col("waste_qty") / col("stock_qty"), 4))
                                      .otherwise(lit(0))) \
                         .withColumn("stock_to_safety_ratio",
                                      when(col("safety_stock") > 0,
                                           spark_round(col("stock_qty") / col("safety_stock"), 2))
                                      .otherwise(lit(0))) \
                         .withColumn("overstock_indicator",
                                      when(col("stock_qty") > col("safety_stock") * 3, 1)
                                      .otherwise(0))

    df_inv = df_inv.withColumn("waste_rate", col("waste_rate").cast(DoubleType())) \
                   .withColumn("stock_to_safety_ratio", col("stock_to_safety_ratio").cast(DoubleType())) \
                   .withColumn("overstock_indicator", col("overstock_indicator").cast(IntegerType()))

    print(f"  库存特征样本: {df_inv.count():,} rows")
    return df_inv


def build_user_features(df_order):
    print("\n[FEATURE] 构建用户行为特征...")

    window_user = Window.partitionBy("user_id").orderBy("order_date")
    window_user_all = Window.partitionBy("user_id")

    user_agg = df_order.groupBy("user_id").agg(
        count("order_id").alias("total_orders"),
        spark_sum("pay_amount").alias("total_spend"),
        avg("pay_amount").alias("avg_order_value"),
        countDistinct("order_date").alias("active_days"),
        spark_max("order_date").alias("last_order_date"),
        spark_min("order_date").alias("first_order_date"),
        avg("discount_rate").alias("avg_discount_rate"),
        spark_sum("quantity").alias("total_quantity")
    )

    today_date = df_order.agg(spark_max("order_date")).collect()[0][0]
    user_agg = user_agg.withColumn("recency_days",
                                     datediff(lit(today_date), col("last_order_date"))) \
                       .withColumn("customer_lifetime_days",
                                     datediff(col("last_order_date"), col("first_order_date"))) \
                       .withColumn("purchase_frequency",
                                     when(col("customer_lifetime_days") > 0,
                                          spark_round(col("total_orders") / col("customer_lifetime_days"), 4))
                                     .otherwise(lit(0))) \
                       .withColumn("avg_basket_qty",
                                     spark_round(col("total_quantity") / col("total_orders"), 2))

    user_agg = user_agg.withColumn("recency_days", col("recency_days").cast(IntegerType())) \
                       .withColumn("customer_lifetime_days", col("customer_lifetime_days").cast(IntegerType())) \
                       .withColumn("purchase_frequency", col("purchase_frequency").cast(DoubleType())) \
                       .withColumn("avg_basket_qty", col("avg_basket_qty").cast(DoubleType()))

    print(f"  用户特征样本: {user_agg.count():,} rows")
    return user_agg


def save_features(features, output_dir):
    feature_path = Path(output_dir) / "features"
    feature_path.mkdir(parents=True, exist_ok=True)

    for name, df in features.items():
        if df is not None:
            df.write.mode("overwrite").parquet(str(feature_path / name))
            print(f"[SAVE] {name} → {feature_path / name}")


if __name__ == "__main__":
    spark = create_spark_session()
    data_dir = str(Path(__file__).resolve().parents[1] / "data")
    dfs = load_processed_data(spark, data_dir)

    features = {}

    if "order" in dfs:
        sales_features = build_sales_features(dfs["order"])
        user_features = build_user_features(dfs["order"])
        features["sales_features"] = sales_features
        features["user_features"] = user_features

    if "inventory" in dfs:
        inventory_features = build_inventory_features(dfs["inventory"])
        features["inventory_features"] = inventory_features

    save_features(features, data_dir)
    spark.stop()
