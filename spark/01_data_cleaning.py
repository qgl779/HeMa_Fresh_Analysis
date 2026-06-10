# -*- coding: utf-8 -*-
"""
01_data_cleaning.py
====================================
数据清洗层: 从 PostgreSQL ODS 层读取原始业务表 -> 清洗加工 ->
写入 Hive hema_fresh.dwd_order_detail / dwd_inventory_detail / dwd_user_behavior
架构: PostgreSQL ODS -> HDFS -> Hive DWD
"""

import os
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, to_timestamp, hour, dayofweek, month,
    when, lit, round as spark_round, concat_ws,
    row_number, datediff, current_date, count as spark_count
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    DecimalType, IntegerType, BooleanType, DateType,
    DoubleType, LongType
)

# ============================================================
# 配置常量
# ============================================================
PG_JDBC_URL = "jdbc:postgresql://192.168.10.144:5432/hema_fresh_dw"
PG_JDBC_PROPERTIES = {
    "user": "hema_admin",
    "password": "hema2024",
    "driver": "org.postgresql.Driver",
}
HDFS_DWD_DIR = "hdfs://192.168.10.128:9000/hema_fresh/dwd"
HIVE_DATABASE = "hema_fresh"


def create_spark_session():
    """
    统一构建 SparkSession
    """
    print("[INIT] 正在构建 SparkSession ...")
    t0 = time.time()
    builder = (
        SparkSession.builder
        .appName("HemaFresh_DataCleaning")
        .master("yarn")
        .config("spark.submit.deployMode", "client")
        .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.10.128:9000")
        .config("spark.driver.host", "192.168.10.128")
        .config("spark.executor.instances", "3")
        .config("spark.executor.cores", "2")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.adaptive.enabled", "true")
        .enableHiveSupport()
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    elapsed = time.time() - t0
    print("[INIT] SparkSession 构建完成: appName=HemaFresh_DataCleaning, "
          "master=yarn, 耗时 {:.2f}s".format(elapsed))
    return spark


# ============================================================
# PG ODS 源表定义
# ============================================================
PG_ODS_TABLES = {
    "order": {
        "table": "ods.ods_order_info",
        "cols": [
            "order_id", "user_id", "product_id", "store_id",
            "order_date", "quantity", "unit_price",
            "total_amount", "discount_amount", "pay_amount",
            "pay_method", "order_status", "create_time",
            "update_time", "is_member", "user_level",
            "category", "product_name"
        ]
    },
    "inventory": {
        "table": "ods.ods_inventory_snapshot",
        "cols": [
            "snapshot_id", "product_id", "store_id",
            "snapshot_date", "stock_qty", "safety_stock",
            "reorder_point", "waste_qty", "promotion_flag",
            "in_transit_qty", "on_order_qty"
        ]
    },
    "user": {
        "table": "ods.dim_user",
        "cols": [
            "user_id", "user_name", "gender", "age",
            "city", "membership_level", "register_date",
            "is_active", "user_tag", "phone"
        ]
    },
    "behavior": {
        "table": "ods.ods_user_behavior",
        "cols": [
            "event_id", "user_id", "event_type", "action",
            "event_time", "event_date", "event_hour",
            "product_id", "page", "stay_seconds",
            "device_type", "session_id"
        ]
    },
}


def read_pg_ods(spark, table_key):
    """
    通过 JDBC 从 PostgreSQL ODS 层读取表数据
    """
    table_meta = PG_ODS_TABLES.get(table_key)
    if table_meta is None:
        print("[ERROR] 未知的 PG ODS 表 key: {}".format(table_key))
        return None
    table_name = table_meta["table"]
    t0 = time.time()
    print("[LOAD] 读取 PG ODS 表: {} ...".format(table_name))
    try:
        df = spark.read.jdbc(url=PG_JDBC_URL, table=table_name, properties=PG_JDBC_PROPERTIES)
        cnt = df.count()
        elapsed = time.time() - t0
        print("[LOAD] ✓ {} -> {:,} 行, {} 列, 耗时 {:.2f}s".format(
            table_name, cnt, len(df.columns), elapsed))
        if cnt > 0:
            print("[LOAD]   前 5 行预览:")
            df.show(5, truncate=False)
        return df
    except Exception as e:
        print("[LOAD] ✗ 读取 PG ODS 表 {} 失败: {}".format(table_name, e))
        return None


# ============================================================
# 清洗函数
# ============================================================
def clean_order_data(df):
    """清洗订单明细 -> dwd_order_detail"""
    if df is None:
        return None
    print("\n[ANALYSIS] 清洗订单明细 ...")
    t0 = time.time()
    original_cnt = df.count()

    if "order_date" in df.columns:
        df = df.withColumn("order_date", to_date(col("order_date")))
    else:
        print("[WARN] 缺少 order_date 列")
        return None

    df = df.withColumn(
        "order_dayofweek", dayofweek(col("order_date")).cast(IntegerType())
    ).withColumn(
        "order_month", month(col("order_date")).cast(IntegerType())
    )

    total_amount_col = "total_amount"
    discount_amount_col = "discount_amount"
    if total_amount_col in df.columns and discount_amount_col in df.columns:
        df = df.withColumn(
            "discount_rate",
            when(col(total_amount_col) > 0,
                 spark_round(col(discount_amount_col) / col(total_amount_col), 3))
            .otherwise(lit(0))
        ).withColumn("discount_rate", col("discount_rate").cast(DecimalType(5, 3)))
    else:
        df = df.withColumn("discount_rate", lit(0).cast(DecimalType(5, 3)))

    required_cols = ["order_id", "user_id", "product_id", "order_date"]
    existing_required = [c for c in required_cols if c in df.columns]
    df = df.dropna(subset=existing_required)

    if "quantity" in df.columns:
        df = df.filter(col("quantity") > 0)

    df = df.withColumn("etl_date", to_date(current_date()).cast(DateType()))

    cleaned_cnt = df.count()
    elapsed = time.time() - t0
    print("[ANALYSIS] ✓ 订单清洗完成: 原始 {:,} 行 -> 清洗后 {:,} 行, "
          "耗时 {:.2f}s, 过滤 {:,} 行".format(
              original_cnt, cleaned_cnt, elapsed, original_cnt - cleaned_cnt))
    return df


def clean_inventory_data(df):
    """清洗库存快照 -> dwd_inventory_detail"""
    if df is None:
        return None
    print("\n[ANALYSIS] 清洗库存快照 ...")
    t0 = time.time()
    original_cnt = df.count()

    if "snapshot_date" in df.columns:
        df = df.withColumn("snapshot_date", to_date(col("snapshot_date")))
    else:
        print("[WARN] 缺少 snapshot_date 列")
        return None

    if "stock_qty" in df.columns and "safety_stock" in df.columns:
        df = df.withColumn(
            "is_understock",
            when(col("stock_qty") < col("safety_stock"), lit(True))
            .otherwise(lit(False))
        ).withColumn("is_understock", col("is_understock").cast(BooleanType()))
    else:
        df = df.withColumn("is_understock", lit(False).cast(BooleanType()))

    if "stock_qty" in df.columns and "reorder_point" in df.columns:
        df = df.withColumn(
            "stock_turnover_ratio",
            when(col("reorder_point") > 0,
                 spark_round(col("stock_qty") / col("reorder_point"), 2))
            .otherwise(lit(0))
        ).withColumn("stock_turnover_ratio", col("stock_turnover_ratio").cast(DecimalType(8, 2)))
    else:
        df = df.withColumn("stock_turnover_ratio", lit(0).cast(DecimalType(8, 2)))

    required_cols = ["snapshot_date", "store_id", "product_id"]
    existing_required = [c for c in required_cols if c in df.columns]
    df = df.dropna(subset=existing_required)

    if "waste_qty" not in df.columns:
        df = df.withColumn("waste_qty", lit(0).cast(IntegerType()))

    df = df.withColumn("etl_date", to_date(current_date()).cast(DateType()))

    cleaned_cnt = df.count()
    elapsed = time.time() - t0
    print("[ANALYSIS] ✓ 库存清洗完成: 原始 {:,} 行 -> 清洗后 {:,} 行, "
          "耗时 {:.2f}s".format(original_cnt, cleaned_cnt, elapsed))
    return df


def clean_user_behavior_data(df):
    """清洗用户行为 -> dwd_user_behavior"""
    if df is None:
        return None
    print("\n[ANALYSIS] 清洗用户行为数据 ...")
    t0 = time.time()
    original_cnt = df.count()

    if "event_time" in df.columns:
        df = df.withColumn("event_datetime", to_timestamp(col("event_time")))
    else:
        print("[WARN] 缺少 event_time 列")
        if "event_date" in df.columns:
            df = df.withColumn(
                "event_datetime",
                to_timestamp(concat_ws(" ", col("event_date"), lit("00:00:00")))
            )
        else:
            return None

    if "event_date" not in df.columns:
        df = df.withColumn("event_date", to_date(col("event_datetime")))
    else:
        df = df.withColumn("event_date", to_date(col("event_date")))

    if "event_hour" not in df.columns and "event_datetime" in df.columns:
        df = df.withColumn("event_hour", hour(col("event_datetime")).cast(IntegerType()))

    df = df.withColumn("is_converted", lit(False).cast(BooleanType()))

    required_cols = ["event_id", "user_id", "event_datetime"]
    existing_required = [c for c in required_cols if c in df.columns]
    df = df.dropna(subset=existing_required)

    df = df.withColumn("etl_date", to_date(current_date()).cast(DateType()))

    cleaned_cnt = df.count()
    elapsed = time.time() - t0
    print("[ANALYSIS] ✓ 用户行为清洗完成: 原始 {:,} 行 -> 清洗后 {:,} 行, 耗时 {:.2f}s".format(
        original_cnt, cleaned_cnt, elapsed))
    return df


# ============================================================
# 写出函数
# ============================================================
def write_to_hdfs_and_hive(spark, df, dwd_name, hive_table_name):
    """
    写出 DWD 数据到 HDFS + Hive
    """
    if df is None:
        return None

    hdfs_path = "{}/{}".format(HDFS_DWD_DIR, dwd_name)
    full_hive_table = "{}.{}".format(HIVE_DATABASE, hive_table_name)

    t0 = time.time()
    cnt = df.count()
    print("\n[SAVE] 写出 {} -> HDFS: {} (Parquet) ...".format(dwd_name, hdfs_path))

    try:
        df.write.mode("overwrite").format("parquet").option("compression", "snappy").save(hdfs_path)
        elapsed_hdfs = time.time() - t0
        print("[SAVE] ✓ HDFS 写出完成: {} 行, 路径: {}, 耗时 {:.2f}s".format(
            cnt, hdfs_path, elapsed_hdfs))
    except Exception as e:
        print("[SAVE] ✗ HDFS 写出失败: {}".format(e))
        return None

    t1 = time.time()
    print("[SAVE] 写出 Hive 表: {} ...".format(full_hive_table))
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(HIVE_DATABASE))
        spark.sql("USE {}".format(HIVE_DATABASE))
        df.write.mode("overwrite").format("parquet").option("path", hdfs_path).saveAsTable(full_hive_table)
        elapsed_hive = time.time() - t1
        print("[SAVE] ✓ Hive 表写出完成: {}, 耗时 {:.2f}s".format(full_hive_table, elapsed_hive))

        verify_df = spark.table(full_hive_table)
        verify_cnt = verify_df.count()
        print("[SAVE]   Hive 表验证: 行数 = {:,} 行 ✓".format(verify_cnt))
        return cnt
    except Exception as e:
        print("[SAVE] ✗ Hive 表写出失败: {}".format(e))
        return None


def main():
    total_start = time.time()
    print("=" * 70)
    print("[START] 盒马鲜生 数据清洗 Spark 任务启动 @ {}".format(
        datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    print("[INFO]  架构: PG ODS -> HDFS -> Hive DWD")
    print("[INFO]  PG_JDBC_URL = {}".format(PG_JDBC_URL))
    print("[INFO]  HDFS_DWD_DIR = {}".format(HDFS_DWD_DIR))
    print("[INFO]  Hive DB = {}".format(HIVE_DATABASE))
    print("=" * 70)

    spark = None
    try:
        spark = create_spark_session()

        print("\n" + "-" * 50)
        print("[LOAD] ===== 第 1 步: 读取 PostgreSQL ODS 层原始数据")
        print("-" * 50)

        raw_order = read_pg_ods(spark, "order")
        raw_inventory = read_pg_ods(spark, "inventory")
        raw_behavior = read_pg_ods(spark, "behavior")

        if raw_order is None and raw_inventory is None and raw_behavior is None:
            print("[FATAL] 没有读取到任何核心 ODS 数据，终止任务")
            return

        print("\n" + "-" * 50)
        print("[ANALYSIS] ===== 第 2 步: 数据清洗加工")
        print("-" * 50)

        dwd_order = clean_order_data(raw_order)
        dwd_inventory = clean_inventory_data(raw_inventory)
        dwd_behavior = clean_user_behavior_data(raw_behavior)

        print("\n" + "-" * 50)
        print("[SAVE] ===== 第 3 步: 写出 DWD 到 HDFS + Hive")
        print("-" * 50)

        results = {}

        if dwd_order is not None:
            results["dwd_order_detail"] = write_to_hdfs_and_hive(
                spark, dwd_order, "dwd_order_detail", "dwd_order_detail")

        if dwd_inventory is not None:
            results["dwd_inventory_detail"] = write_to_hdfs_and_hive(
                spark, dwd_inventory, "dwd_inventory_detail", "dwd_inventory_detail")

        if dwd_behavior is not None:
            results["dwd_user_behavior"] = write_to_hdfs_and_hive(
                spark, dwd_behavior, "dwd_user_behavior", "dwd_user_behavior")

        total_elapsed = time.time() - total_start
        print("\n" + "=" * 70)
        print("[DONE] 数据清洗任务完成 @ {}".format(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        print("[DONE] 总耗时: {:.2f} 秒".format(total_elapsed))
        print("[DONE] 各 DWD 表行数:")
        for k, v in results.items():
            if v is not None:
                print("[DONE]   - {}: {:,} 行".format(k, v))
        print("=" * 70)

    except Exception as e:
        print("\n[FATAL] 任务执行异常: {}".format(e))
        import traceback
        traceback.print_exc()
    finally:
        if spark is not None:
            spark.stop()
            print("[END] SparkSession 已停止")


if __name__ == "__main__":
    main()