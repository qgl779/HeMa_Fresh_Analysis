import os
import sys
import time
import math
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, round as spark_round, sum as spark_sum,
    count, avg, max as spark_max, min as spark_min, stddev,
    expr, row_number, lag, lead, collect_list
)
from pyspark.sql.types import DoubleType, FloatType, DecimalType
from pyspark.sql.window import Window

from config import settings


def create_spark_session():
    print("[INIT] 正在构建 SparkSession ...")
    spark = SparkSession.builder \
        .appName("HemaFresh_InventoryOptimization") \
        .master(settings.SPARK_CONFIG.get("master", "yarn")) \
        .config("spark.submit.deployMode", "client") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.10.128:9000") \
        .config("spark.executor.instances", "3") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.driver.host", "192.168.10.128") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("[INIT] SparkSession 构建完成：appName=HemaFresh_InventoryOptimization, master={}".format(settings.SPARK_CONFIG.get("master", "yarn")))
    return spark


def _hdfs_exists(spark, path):
    try:
        jvm = spark._jvm
        hadoop_conf = spark._jsc.hadoopConfiguration()
        p = jvm.org.apache.hadoop.fs.Path(path)
        fs = p.getFileSystem(hadoop_conf)
        return fs.exists(p)
    except Exception:
        return False


def load_data(spark, cluster_mode):
    dfs = {}
    if cluster_mode:
        inv_path = settings.HDFS_FEATURES_DIR + "/inventory_features"
        order_path = settings.HDFS_FEATURES_DIR + "/order_clean"
        print("[LOAD] 集群模式，读取 HDFS 库存路径: {}".format(inv_path))
        try:
            if _hdfs_exists(spark, inv_path):
                dfs["inventory"] = spark.read.parquet(inv_path)
                print("[LOAD] inventory_features -> {} 行".format(dfs["inventory"].count()))
            else:
                print("[WARN] HDFS 库存特征路径不存在: {}".format(inv_path))
        except Exception as e:
            print("[WARN] 读取 HDFS 库存特征失败: {}".format(str(e)))

        print("[LOAD] 集群模式，读取 HDFS 订单路径: {}".format(order_path))
        try:
            if _hdfs_exists(spark, order_path):
                dfs["order"] = spark.read.parquet(order_path)
                print("[LOAD] order_clean -> {} 行".format(dfs["order"].count()))
            else:
                print("[WARN] HDFS 订单路径不存在: {}".format(order_path))
        except Exception as e:
            print("[WARN] 读取 HDFS 订单失败: {}".format(str(e)))
    else:
        base = Path(__file__).resolve().parents[1] / "data" / "processed"
        inv_path = base / "dwd_inventory_detail"
        order_path = base / "dwd_order_detail"
        print("[LOAD] 本地模式，库存路径: {}".format(str(inv_path)))
        print("[LOAD] 本地模式，订单路径: {}".format(str(order_path)))
        if inv_path.exists():
            dfs["inventory"] = spark.read.parquet(str(inv_path))
            print("[LOAD] inventory -> {} 行".format(dfs["inventory"].count()))
        if order_path.exists():
            dfs["order"] = spark.read.parquet(str(order_path))
            print("[LOAD] order -> {} 行".format(dfs["order"].count()))

    return dfs


def calc_optimal_inventory(dfs):
    print("\n[ANALYSIS] 计算最优库存策略 ...")

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
    annual_demand = 365.0
    unit_holding = holding_cost_rate * 30.0  # 单位持有成本

    # EOQ = sqrt(2DS/H) — 纯 SQL expr，避免 PySpark 方法链兼容问题
    eoq_sql = (
        "round(sqrt((2.0 * {S} * avg_daily_demand * {D}) "
        "/ (avg_daily_demand * {H})), 1)"
    ).format(S=order_cost, D=annual_demand, H=unit_holding)

    result = product_demand.withColumn(
        "optimal_stock",
        spark_round(
            col("avg_daily_demand") * lit(lead_time_days)
            + col("std_daily_demand") * lit(service_level * math.sqrt(lead_time_days)),
            1
        )
    ).withColumn(
        "reorder_point",
        spark_round(
            col("avg_daily_demand") * lit(lead_time_days)
            + col("std_daily_demand") * lit(service_level),
            1
        )
    ).withColumn(
        "safety_stock_opt",
        spark_round(col("std_daily_demand") * lit(service_level * math.sqrt(lead_time_days)), 1)
    ).withColumn(
        "eoq",
        expr(eoq_sql)
    )

    result = result.withColumn(
        "holding_cost",
        spark_round(col("optimal_stock") * lit(holding_cost_rate) * 30, 2)
    ).withColumn(
        "shortage_cost",
        spark_round(col("avg_waste") * lit(shortage_cost_rate) * 30, 2)
    ).withColumn(
        "total_cost",
        spark_round(col("holding_cost") + col("shortage_cost"), 2)
    )

    result = result.withColumn("optimal_stock", col("optimal_stock").cast(DoubleType())) \
                   .withColumn("reorder_point", col("reorder_point").cast(DoubleType())) \
                   .withColumn("safety_stock_opt", col("safety_stock_opt").cast(DoubleType())) \
                   .withColumn("eoq", col("eoq").cast(DoubleType()))

    print("[ANALYSIS] 最优库存策略计算完成，共 {} 个商品".format(result.count()))
    print("  === 库存策略预览 (前10行) ===")
    result.show(10, False)
    return result


def generate_alerts(dfs, optimal_inventory):
    print("\n[ALERT] 生成库存预警 ...")

    if "inventory" not in dfs:
        print("[WARN] 无库存数据，跳过预警生成")
        return None

    # 取每个 product_id + store_id 最新快照（用窗口排序保证确定性）
    w_latest = Window.partitionBy("product_id", "store_id").orderBy(col("snapshot_date").desc())
    latest_inventory = (
        dfs["inventory"]
        .withColumn("_rn", row_number().over(w_latest))
        .filter(col("_rn") == 1)
        .drop("_rn")
        .select(
            "product_id", "store_id",
            col("snapshot_date"),
            col("stock_qty").alias("current_stock"),
            col("safety_stock"),
            col("waste_qty"),
            col("promotion_flag"),
        )
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
    print("  高风险预警商品数: {}".format(high_alert_count))

    alert_summary = alerts.groupBy("alert_level").agg(count("*").alias("cnt"))
    print("  === 预警等级分布 ===")
    alert_summary.show(10, False)

    high_alert.select("product_id", "store_id", "alert_level",
                      "current_stock", "suggested_order").show(10, False)

    return alerts


def category_inventory_analysis(dfs):
    print("\n[ANALYSIS] 品类库存效率分析 ...")

    if "inventory" not in dfs:
        print("[WARN] 无库存数据，跳过品类分析")
        return None

    recent_inv = dfs["inventory"]
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

    print("  === 品类库存排行 (按损耗降序) ===")
    category_analysis.show(15, False)
    return category_analysis


def write_optimal_to_mysql(optimal_df):
    print("\n[MYSQL] 写入库存优化结果到 MySQL hema_fresh_ads.ads_inventory_optimization ...")
    try:
        optimal_df.select(
            "product_id", "optimal_stock", "reorder_point",
            "safety_stock_opt", "eoq", "holding_cost", "shortage_cost", "total_cost"
        ).withColumnRenamed("safety_stock_opt", "safety_stock").write.jdbc(
            url=settings.MYSQL_JDBC_URL,
            table="ads_inventory_optimization",
            mode="overwrite",
            properties=settings.MYSQL_JDBC_PROPERTIES
        )
        print("[MYSQL] 写入完成，共 {} 行".format(optimal_df.count()))
    except Exception as e:
        print("[WARN] MySQL 写入失败: {}".format(str(e)))


if __name__ == "__main__":
    start_time = time.time()
    print("=" * 70)
    print("[START] 盒马鲜生 库存优化 Spark 任务启动 @ {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    print("[INFO]  CLUSTER_MODE = {}, master = {}".format(settings.CLUSTER_MODE, settings.SPARK_CONFIG.get("master", "yarn")))
    print("=" * 70)

    spark = create_spark_session()

    try:
        dfs = load_data(spark, settings.CLUSTER_MODE)
        if "order" not in dfs:
            print("[ERROR] 缺少订单数据，请先运行上游清洗任务")
            spark.stop()
            sys.exit(1)

        optimal_inventory = calc_optimal_inventory(dfs)

        alerts = generate_alerts(dfs, optimal_inventory)

        category_analysis = category_inventory_analysis(dfs)

        if settings.CLUSTER_MODE:
            hdfs_out = settings.HDFS_FEATURES_DIR + "/inventory_optimization"
            print("\n[SAVE] 库存优化结果写入 HDFS: {}".format(hdfs_out))
            optimal_inventory.write.mode("overwrite").parquet(hdfs_out)

            if alerts is not None:
                alerts_hdfs = settings.HDFS_FEATURES_DIR + "/inventory_alerts"
                print("[SAVE] 库存预警写入 HDFS: {}".format(alerts_hdfs))
                alerts.write.mode("overwrite").parquet(alerts_hdfs)

            if category_analysis is not None:
                cat_hdfs = settings.HDFS_FEATURES_DIR + "/category_inventory_analysis"
                print("[SAVE] 品类库存分析写入 HDFS: {}".format(cat_hdfs))
                category_analysis.write.mode("overwrite").parquet(cat_hdfs)
        else:
            local_base = Path(__file__).resolve().parents[1] / "data" / "features"
            local_base.mkdir(parents=True, exist_ok=True)
            out_path = str(local_base / "inventory_optimization")
            print("\n[SAVE] 库存优化结果写入本地: {}".format(out_path))
            optimal_inventory.write.mode("overwrite").parquet(out_path)

        write_optimal_to_mysql(optimal_inventory)

        elapsed = time.time() - start_time
        print("\n" + "=" * 70)
        print("[DONE] 库存优化分析任务完成，耗时 {:.1f} 秒".format(elapsed))
        print("  商品数: {}".format(optimal_inventory.count()))
        print("=" * 70)
    except Exception as e:
        print("[FATAL] 任务执行异常: {}".format(str(e)))
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        print("[END] SparkSession 已停止")
