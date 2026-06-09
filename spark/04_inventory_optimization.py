import os
import sys
import math
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, round as spark_round, sum as spark_sum,
    count, avg, max as spark_max, min as spark_min, stddev,
    expr, row_number, lag, lead, collect_list
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, FloatType, DecimalType

from config.settings import SPARK_CONFIG


def create_spark_session():
    spark = SparkSession.builder \
        .appName(f"{SPARK_CONFIG['app_name']}_InventoryOptimization") \
        .master(SPARK_CONFIG["master"]) \
        .config("spark.executor.memory", SPARK_CONFIG["spark.executor.memory"]) \
        .config("spark.driver.memory", SPARK_CONFIG["spark.driver.memory"]) \
        .config("spark.sql.shuffle.partitions", "100") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_data(spark, data_dir):
    base = Path(data_dir) / "processed"
    inv_path = base / "dwd_inventory_detail"
    order_path = base / "dwd_order_detail"
    product_path = Path(data_dir) / "raw" / "dim_product.csv"

    dfs = {}
    if inv_path.exists():
        dfs["inventory"] = spark.read.parquet(str(inv_path))
        print(f"[LOAD] inventory → {dfs['inventory'].count():,} rows")
    if order_path.exists():
        dfs["order"] = spark.read.parquet(str(order_path))
        print(f"[LOAD] order → {dfs['order'].count():,} rows")
    if product_path.exists():
        dfs["product"] = spark.read.option("header", True).option("inferSchema", True).csv(str(product_path))
        print(f"[LOAD] product → {dfs['product'].count():,} rows")
    return dfs


def calc_optimal_inventory(dfs):
    print("\n[ANALYSIS] 计算最优库存策略...")

    order_daily = dfs["order"].groupBy("product_id", "order_date").agg(
        spark_sum("quantity").alias("daily_demand"),
        spark_sum("pay_amount").alias("daily_revenue")
    )

    product_demand = order_daily.groupBy("product_id").agg(
        avg("daily_demand").alias("avg_daily_demand"),
        stddev("daily_demand").alias("std_daily_demand"),
        count("order_date").alias("active_days")
    ).fillna(0)

    if "inventory" in dfs:
        inventory_stats = dfs["inventory"].groupBy("product_id").agg(
            avg("stock_qty").alias("avg_stock"),
            avg("waste_qty").alias("avg_waste"),
            avg("stock_to_safety_ratio").alias("avg_stock_ratio"),
            spark_max("stock_qty").alias("max_stock"),
            spark_min("stock_qty").alias("min_stock")
        )
        product_demand = product_demand.join(inventory_stats, "product_id", "left")

    lead_time_days = 2
    service_level = 1.65
    holding_cost_rate = 0.15
    shortage_cost_rate = 0.40
    order_cost = 200

    result = product_demand.withColumn(
        "optimal_stock",
        spark_round(
            col("avg_daily_demand") * lead_time_days
            + col("std_daily_demand") * service_level * math.sqrt(lead_time_days),
            1
        )
    ).withColumn(
        "reorder_point",
        spark_round(
            col("avg_daily_demand") * lead_time_days
            + col("std_daily_demand") * service_level,
            1
        )
    ).withColumn(
        "safety_stock_opt",
        spark_round(col("std_daily_demand") * service_level * math.sqrt(lead_time_days), 1)
    ).withColumn(
        "eoq",
        spark_round(
            ((2 * order_cost * col("avg_daily_demand") * 365)
             / (col("avg_daily_demand") * holding_cost_rate * 30)).pow(0.5),
            1
        )
    )

    if "product" in dfs:
        result = result.join(dfs["product"].select("product_id", "product_name", "category", "base_price"),
                            "product_id", "left")

    result = result.withColumn("holding_cost",
                                spark_round(col("optimal_stock") * lit(holding_cost_rate) * 30, 2)) \
                   .withColumn("shortage_cost",
                                spark_round(col("avg_waste") * lit(shortage_cost_rate) * 30, 2)) \
                   .withColumn("total_cost",
                                spark_round(col("holding_cost") + col("shortage_cost"), 2))

    result = result.withColumn("optimal_stock", col("optimal_stock").cast(DoubleType())) \
                   .withColumn("reorder_point", col("reorder_point").cast(DoubleType())) \
                   .withColumn("safety_stock_opt", col("safety_stock_opt").cast(DoubleType())) \
                   .withColumn("eoq", col("eoq").cast(DoubleType())) \
                   .withColumn("holding_cost", col("holding_cost").cast(DoubleType())) \
                   .withColumn("shortage_cost", col("shortage_cost").cast(DoubleType())) \
                   .withColumn("total_cost", col("total_cost").cast(DoubleType()))

    return result


def generate_alerts(dfs, optimal_inventory):
    print("\n[ALERT] 生成库存预警...")

    if "inventory" not in dfs:
        print("[WARN] 无库存数据，跳过预警生成")
        return None

    latest_inventory = dfs["inventory"].groupBy("product_id", "store_id").agg(
        spark_max("snapshot_date").alias("snapshot_date"),
        expr("last(stock_qty) as current_stock"),
        expr("last(safety_stock) as safety_stock"),
        expr("last(waste_qty) as waste_qty"),
        expr("last(promotion_flag) as promotion_flag")
    )

    alerts = latest_inventory.join(optimal_inventory, "product_id", "left")

    alerts = alerts.withColumn(
        "alert_level",
        when(col("current_stock") <= col("safety_stock") * 0.5, "严重缺货")
        .when(col("current_stock") <= col("safety_stock"), "预警")
        .when(col("current_stock") > col("optimal_stock") * 1.5, "超量库存")
        .otherwise("正常")
    ).withColumn(
        "suggested_order",
        when(col("current_stock") < col("reorder_point"),
             spark_round(col("optimal_stock") - col("current_stock"), 1))
        .otherwise(lit(0))
    ).withColumn(
        "waste_risk",
        when(col("waste_qty") > 0, "高损耗")
        .when(col("current_stock") > col("optimal_stock") * 2, "临期风险")
        .otherwise("低风险")
    )

    high_alert = alerts.filter(col("alert_level").isin("严重缺货", "预警"))
    high_alert_count = high_alert.count()
    print(f"  高风险预警商品数: {high_alert_count}")
    high_alert.select("product_id", "store_id", "alert_level",
                      "current_stock", "suggested_order").show(10, False)

    return alerts


def category_inventory_analysis(dfs):
    print("\n[ANALYSIS] 品类库存效率分析...")

    if "inventory" not in dfs:
        return None

    recent_inv = dfs["inventory"].filter(
        col("snapshot_date") >= expr("(select max(snapshot_date) from dwd_inventory_detail) - 30")
    )

    if "product" in dfs:
        recent_inv = recent_inv.join(
            dfs["product"].select("product_id", "category"), "product_id", "left"
        )
    else:
        recent_inv = recent_inv.withColumn("category", lit("未知"))

    category_analysis = recent_inv.groupBy("category").agg(
        avg("waste_qty").alias("avg_waste"),
        spark_sum("waste_qty").alias("total_waste"),
        avg("stock_turnover_ratio").alias("avg_turnover"),
        avg("stock_qty").alias("avg_stock")
    ).orderBy(col("total_waste").desc())

    category_analysis.show(15, False)
    return category_analysis


if __name__ == "__main__":
    spark = create_spark_session()
    data_dir = str(Path(__file__).resolve().parents[1] / "data")

    dfs = load_data(spark, data_dir)
    if "order" not in dfs:
        print("[ERROR] 缺少数���，请先运行 01_data_cleaning.py 和 02_feature_engineering.py")
        spark.stop()
        sys.exit(1)

    optimal_inventory = calc_optimal_inventory(dfs)
    optimal_inventory.show(10, False)

    alerts = generate_alerts(dfs, optimal_inventory)

    category_analysis = category_inventory_analysis(dfs)

    output_path = Path(data_dir) / "features"
    optimal_inventory.write.mode("overwrite").parquet(str(output_path / "inventory_optimization"))

    print("\n=== 库存优化分析完成 ===")
    spark.stop()
