# -*- coding: utf-8 -*-
"""
02_feature_engineering.py
====================================
特征工程层: 从 Hive hema_fresh.dwd_* 读取明细数据 ->
构建聚合特征 -> 写入 Hive hema_fresh.dws_* + HDFS 中间数据集
架构: Hive DWD -> Spark 特征聚合 -> Hive DWS + HDFS features/
下游: 03_sales_prediction.py / 04_inventory_optimization.py / 05_user_behavior_analysis.py
"""

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
    count, countDistinct, avg, max as spark_max, min as spark_min,
    stddev, datediff, to_date, expr, row_number, rank, dense_rank,
    lag, lead, current_date, broadcast, month, weekofyear, dayofweek
)
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, DecimalType
from pyspark.sql.window import Window

# ============================================================
# 配置常量
# ============================================================
HDFS_BASE_PATH = "hdfs://192.168.10.128:9000/hema_fresh"
HDFS_DWD_DIR = HDFS_BASE_PATH + "/dwd"
HDFS_DWS_DIR = HDFS_BASE_PATH + "/dws"
HDFS_FEATURES_DIR = HDFS_BASE_PATH + "/features"   # 供 04/05 脚本读取的中间 Parquet
HIVE_DATABASE = "hema_fresh"


def create_spark_session():
    """
    统一构建 SparkSession
    """
    print("[INIT] 正在构建 SparkSession ...")
    t0 = time.time()
    builder = (
        SparkSession.builder
        .appName("HemaFresh_FeatureEngineering")
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
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .enableHiveSupport()
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    elapsed = time.time() - t0
    print("[INIT] SparkSession 构建完成: appName=HemaFresh_FeatureEngineering, "
          "master=yarn, 耗时 {:.2f}s".format(elapsed))
    return spark


# ============================================================
# 1. 读取 Hive DWD 层数据 + 维度表
# ============================================================

def load_all_data(spark):
    """
    从 Hive 读取 DWD 明细表和维度表，并做 JOIN 补全维度字段。
    返回字典: {"order", "inventory", "behavior", "product", "store", "user"}
    """
    spark.sql("USE {}".format(HIVE_DATABASE))
    print("\n[LOAD] 从 Hive 读取源数据 ...")

    # --- 维度表 (broadcast 小表) ---
    t_dim = time.time()
    dim_product = spark.table("dim_product")
    dim_store = spark.table("dim_store")
    dim_user = spark.table("dim_user")
    # PG 源表列名为 membership，重命名为 membership_level 以统一后续使用
    if "membership" in dim_user.columns and "membership_level" not in dim_user.columns:
        dim_user = dim_user.withColumnRenamed("membership", "membership_level")

    for name, df in [("dim_product", dim_product), ("dim_store", dim_store), ("dim_user", dim_user)]:
        cnt = df.count()
        print("[LOAD]   {} = {:,} 行".format(name, cnt))
    print("[LOAD]   维度表读取耗时 {:.1f}s".format(time.time() - t_dim))

    # --- DWD 订单明细 + JOIN 维度 ---
    t_order = time.time()
    print("[LOAD]   读取 dwd_order_detail ...")
    order_raw = spark.table("dwd_order_detail")
    order_enriched = (
        order_raw
        .join(broadcast(dim_product.select("product_id", "category", "sub_category", "brand", "base_price", "shelf_life_days")),
              "product_id", "left")
        .join(broadcast(dim_store.select("store_id", "city")),
              "store_id", "left")
        .join(broadcast(dim_user.select("user_id", "membership_level", "user_tag")),
              "user_id", "left")
        .withColumn("order_date", to_date(col("order_date")))
    )
    order_cnt = order_enriched.count()
    print("[LOAD]   dwd_order_detail + dims = {:,} 行, 耗时 {:.1f}s".format(order_cnt, time.time() - t_order))

    # --- DWD 库存快照 + JOIN 维度 ---
    t_inv = time.time()
    print("[LOAD]   读取 dwd_inventory_detail ...")
    inv_raw = spark.table("dwd_inventory_detail")
    inv_enriched = (
        inv_raw
        .join(broadcast(dim_product.select("product_id", "category")),
              "product_id", "left")
        .join(broadcast(dim_store.select("store_id", "city")),
              "store_id", "left")
        .withColumn("snapshot_date", to_date(col("snapshot_date")))
    )
    inv_cnt = inv_enriched.count()
    print("[LOAD]   dwd_inventory_detail + dims = {:,} 行, 耗时 {:.1f}s".format(inv_cnt, time.time() - t_inv))

    # --- DWD 用户行为 ---
    t_beh = time.time()
    print("[LOAD]   读取 dwd_user_behavior ...")
    beh_raw = spark.table("dwd_user_behavior")
    beh_enriched = (
        beh_raw
        .join(broadcast(dim_user.select("user_id", "membership_level")),
              "user_id", "left")
        .withColumn("event_date", to_date(col("event_time")))
    )
    beh_cnt = beh_enriched.count()
    print("[LOAD]   dwd_user_behavior + dims = {:,} 行, 耗时 {:.1f}s".format(beh_cnt, time.time() - t_beh))

    data = {
        "order": order_enriched,
        "inventory": inv_enriched,
        "behavior": beh_enriched,
        "product": dim_product,
        "store": dim_store,
        "user": dim_user,
    }
    print("[LOAD] 全部数据加载完成 ✓\n")
    return data


# ============================================================
# 2. 构建 dws_sales_daily — 每日销售汇总 + 时序特征
# ============================================================

def build_sales_daily(spark, order_df):
    """
    从订单宽表聚合每日销售指标，并计算 lag / rolling 时序特征
    输出: dws_sales_daily (Hive) — 含特征列供 03 模型消费
    """
    print("=" * 60)
    print("[DWS] 构建 dws_sales_daily — 每日销售汇总宽表 ...")
    t0 = time.time()

    # 2.1 每日粒度聚合：product_id + order_date + category + city
    daily_sales = (
        order_df
        .withColumn("discount_amount",
                    when(col("discount_amount").isNull(), lit(0))
                    .otherwise(col("discount_amount")))
        .groupBy("product_id", "category", "city", "order_date")
        .agg(
            spark_sum("quantity").alias("sales_qty"),
            spark_sum("total_amount").alias("sales_amount"),
            spark_sum("pay_amount").alias("pay_amount"),
            spark_sum("discount_amount").alias("discount_amount"),
            count("order_id").alias("order_count"),
            countDistinct("user_id").alias("user_count"),
        )
    )

    # 2.2 添加日期衍生字段
    daily_sales = (
        daily_sales
        .withColumn("month", month(col("order_date")))
        .withColumn("weekofyear", weekofyear(col("order_date")))
        .withColumn("dayofweek", dayofweek(col("order_date")))
    )

    # 2.3 窗口函数: 按 product_id 排序，计算 lag / rolling 特征
    w_product = Window.partitionBy("product_id").orderBy("order_date")
    w_rolling_7d = Window.partitionBy("product_id").orderBy("order_date")\
        .rowsBetween(-6, 0)   # 包含当日共7天
    w_rolling_14d = Window.partitionBy("product_id").orderBy("order_date")\
        .rowsBetween(-13, 0)
    w_rolling_30d = Window.partitionBy("product_id").orderBy("order_date")\
        .rowsBetween(-29, 0)

    daily_sales = (
        daily_sales
        .withColumn("sales_lag_1", lag("sales_qty", 1).over(w_product))
        .withColumn("sales_lag_7", lag("sales_qty", 7).over(w_product))
        .withColumn("sales_lag_14", lag("sales_qty", 14).over(w_product))
        .withColumn("sales_lag_30", lag("sales_qty", 30).over(w_product))
        .withColumn("sales_rolling_7d_avg",
                    spark_round(avg("sales_qty").over(w_rolling_7d), 2))
        .withColumn("sales_rolling_14d_avg",
                    spark_round(avg("sales_qty").over(w_rolling_14d), 2))
        .withColumn("sales_rolling_30d_avg",
                    spark_round(avg("sales_qty").over(w_rolling_30d), 2))
    )

    # 2.4 折扣率
    daily_sales = daily_sales.withColumn(
        "avg_discount_rate",
        spark_round(
            when(col("sales_amount") > 0,
                 col("discount_amount") / col("sales_amount"))
            .otherwise(lit(0)), 4
        )
    )

    # 2.5 GMV 别名
    daily_sales = daily_sales.withColumn("daily_gmv", col("pay_amount"))

    # 2.6 数值类型转换
    daily_sales = (
        daily_sales
        .withColumn("sales_qty", col("sales_qty").cast(LongType()))
        .withColumn("order_count", col("order_count").cast(LongType()))
        .withColumn("user_count", col("user_count").cast(LongType()))
        .withColumn("sales_amount", col("sales_amount").cast(DecimalType(18, 2)))
        .withColumn("pay_amount", col("pay_amount").cast(DecimalType(18, 2)))
        .withColumn("discount_amount", col("discount_amount").cast(DecimalType(18, 2)))
        .withColumn("sales_lag_1", col("sales_lag_1").cast(DoubleType()))
        .withColumn("sales_lag_7", col("sales_lag_7").cast(DoubleType()))
        .withColumn("sales_lag_14", col("sales_lag_14").cast(DoubleType()))
        .withColumn("sales_lag_30", col("sales_lag_30").cast(DoubleType()))
        .fillna({"sales_lag_1": 0.0, "sales_lag_7": 0.0, "sales_lag_14": 0.0, "sales_lag_30": 0.0,
                 "sales_rolling_7d_avg": 0.0, "sales_rolling_14d_avg": 0.0, "sales_rolling_30d_avg": 0.0})
    )

    # 2.7 统计
    cnt = daily_sales.count()
    elapsed = time.time() - t0
    print("[DWS]   dws_sales_daily = {:,} 行, 耗时 {:.1f}s".format(cnt, elapsed))
    print("[DWS]   样例:")
    daily_sales.select("product_id", "category", "city", "order_date",
                       "sales_qty", "sales_rolling_7d_avg", "dayofweek").show(10, truncate=False)
    print("=" * 60 + "\n")
    return daily_sales


# ============================================================
# 3. 构建 dws_inventory_daily — 每日库存宽表
# ============================================================

def build_inventory_daily(spark, inv_df):
    """
    从库存明细聚合每日库存指标 + 缺货/超量标志
    输出: dws_inventory_daily (Hive)
    """
    print("=" * 60)
    print("[DWS] 构建 dws_inventory_daily — 每日库存宽表 ...")
    t0 = time.time()

    daily_inv = (
        inv_df
        .groupBy("snapshot_date", "product_id", "category", "store_id", "city")
        .agg(
            spark_sum("stock_qty").alias("stock_qty"),
            spark_sum("safety_stock").alias("safety_stock"),
            spark_sum("waste_qty").alias("waste_qty"),
            spark_sum("reorder_point").alias("reorder_point"),
        )
        .withColumn("snapshot_date", to_date(col("snapshot_date")))
    )

    # 损耗率: waste_qty / stock_qty（stock>0 时计算）
    daily_inv = daily_inv.withColumn(
        "waste_rate",
        spark_round(
            when(col("stock_qty") > 0,
                 col("waste_qty") / col("stock_qty"))
            .otherwise(lit(0)), 4
        )
    )

    # 库存周转率: 基于 stock_qty 和 reorder_point 估算
    daily_inv = daily_inv.withColumn(
        "stock_turnover",
        spark_round(
            when(col("reorder_point") > 0,
                 col("stock_qty") / col("reorder_point"))
            .otherwise(lit(0)), 4
        )
    )

    # 缺货 / 超量标志
    daily_inv = daily_inv.withColumn(
        "understock_flag",
        when(col("stock_qty") <= col("safety_stock") * 0.5, lit(1)).otherwise(lit(0))
    ).withColumn(
        "overstock_flag",
        when(col("stock_qty") > col("safety_stock") * 3, lit(1)).otherwise(lit(0))
    )

    # 类型转换
    daily_inv = (
        daily_inv
        .withColumn("stock_qty", col("stock_qty").cast(LongType()))
        .withColumn("safety_stock", col("safety_stock").cast(LongType()))
        .withColumn("waste_qty", col("waste_qty").cast(LongType()))
        .withColumnRenamed("snapshot_date", "dt")
    )

    cnt = daily_inv.count()
    elapsed = time.time() - t0
    print("[DWS]   dws_inventory_daily = {:,} 行, 耗时 {:.1f}s".format(cnt, elapsed))
    print("[DWS]   样例:")
    daily_inv.select("dt", "product_id", "category", "city",
                     "stock_qty", "waste_rate", "understock_flag").show(10, truncate=False)
    print("=" * 60 + "\n")
    return daily_inv


# ============================================================
# 4. 构建 dws_user_rfm — 用户 RFM 分层
# ============================================================

def build_user_rfm(spark, order_df):
    """
    从订单数据计算 RFM 并分层:
    - Recency: 距最近一次购买的天数
    - Frequency: 购买次数
    - Monetary: 总消费金额
    按分位数分为 1/2/3 三档，组合出 6 类用户标签
    输出: dws_user_rfm (Hive)
    """
    print("=" * 60)
    print("[DWS] 构建 dws_user_rfm — 用户 RFM 分层 ...")
    t0 = time.time()

    # 4.1 计算用户级别的 RFM 原始值
    user_purchase = (
        order_df
        .groupBy("user_id")
        .agg(
            count("order_id").alias("purchase_count"),
            spark_sum("pay_amount").alias("total_spend"),
            spark_round(avg("pay_amount"), 2).alias("avg_order_value"),
            spark_max("order_date").alias("last_order_date"),
            spark_min("order_date").alias("first_order_date"),
        )
    )

    # 当前日期 — 取订单表的最大日期
    max_date_row = order_df.agg(spark_max("order_date")).collect()[0][0]
    if max_date_row is None:
        max_date_row = datetime.now().strftime("%Y-%m-%d")
    print("[DWS]   基准日期 (max order_date): {}".format(max_date_row))
    today = max_date_row

    user_purchase = user_purchase.withColumn(
        "recency_days", datediff(lit(today), col("last_order_date"))
    )

    # 4.2 计算 R / F / M 的 33% / 66% 分位数
    # R 越小越好 → 分位反转
    r_bounds = user_purchase.approxQuantile("recency_days", [0.33, 0.67], 0.01)
    f_bounds = user_purchase.approxQuantile("purchase_count", [0.33, 0.67], 0.01)
    m_bounds = user_purchase.approxQuantile("total_spend", [0.33, 0.67], 0.01)
    print("[DWS]   R 分位阈值: {} / {}".format(r_bounds[0], r_bounds[1]))
    print("[DWS]   F 分位阈值: {} / {}".format(f_bounds[0], f_bounds[1]))
    print("[DWS]   M 分位阈值: {} / {}".format(m_bounds[0], m_bounds[1]))

    # 4.3 分配 R / F / M 分值 (3=高, 1=低)
    # R: recency 越小越好 → 小于低分位 = 高活跃(3)
    r_0, r_1 = r_bounds
    f_0, f_1 = f_bounds
    m_0, m_1 = m_bounds

    user_purchase = (
        user_purchase
        .withColumn("r_score",
                    when(col("recency_days") <= r_0, 3)
                    .when(col("recency_days") <= r_1, 2)
                    .otherwise(1))
        .withColumn("f_score",
                    when(col("purchase_count") >= f_1, 3)
                    .when(col("purchase_count") >= f_0, 2)
                    .otherwise(1))
        .withColumn("m_score",
                    when(col("total_spend") >= m_1, 3)
                    .when(col("total_spend") >= m_0, 2)
                    .otherwise(1))
    )

    # 4.4 组合 RFM 标签
    user_purchase = user_purchase.withColumn(
        "rfm_segment",
        when((col("r_score") >= 2) & (col("f_score") >= 2) & (col("m_score") >= 2), "高价值用户")
        .when((col("r_score") >= 2) & (col("f_score") >= 2) & (col("m_score") < 2), "活跃用户")
        .when((col("r_score") < 2) & (col("f_score") >= 2) & (col("m_score") >= 2), "沉睡高价值")
        .when((col("r_score") >= 2) & (col("f_score") < 2) & (col("m_score") < 2), "新用户")
        .when((col("r_score") < 2) & (col("f_score") < 2) & (col("m_score") >= 2), "流失高价值")
        .otherwise("低价值用户")
    )

    # 4.5 类型转换
    user_purchase = (
        user_purchase
        .withColumn("r_score", col("r_score").cast(IntegerType()))
        .withColumn("f_score", col("f_score").cast(IntegerType()))
        .withColumn("m_score", col("m_score").cast(IntegerType()))
        .withColumn("recency_days", col("recency_days").cast(IntegerType()))
        .withColumn("purchase_count", col("purchase_count").cast(IntegerType()))
        .withColumn("total_spend", col("total_spend").cast(DecimalType(18, 2)))
        .withColumn("avg_order_value", col("avg_order_value").cast(DecimalType(18, 2)))
        .withColumn("last_order_date", col("last_order_date").cast(StringType()))
        .withColumn("first_order_date", col("first_order_date").cast(StringType()))
    )

    # 统计分布
    segment_stats = user_purchase.groupBy("rfm_segment").agg(
        count("user_id").alias("cnt"),
        spark_round(avg("total_spend"), 0).alias("avg_spend")
    ).orderBy(col("cnt").desc())

    cnt = user_purchase.count()
    elapsed = time.time() - t0
    print("[DWS]   dws_user_rfm = {:,} 用户, 耗时 {:.1f}s".format(cnt, elapsed))
    print("[DWS]   分层分布:")
    segment_stats.show(10, truncate=False)
    print("=" * 60 + "\n")
    return user_purchase


# ============================================================
# 5. 构建 dws_category_ranking — 品类商品排名
# ============================================================

def build_category_ranking(spark, order_df):
    """
    按品类/城市每日对商品做销量和销售额排名
    输出: dws_category_ranking (Hive)
    """
    print("=" * 60)
    print("[DWS] 构建 dws_category_ranking — 品类商品排名 ...")
    t0 = time.time()

    cat_daily = (
        order_df
        .groupBy("order_date", "category", "product_id", "city")
        .agg(
            spark_sum("quantity").alias("sales_qty"),
            spark_sum("pay_amount").alias("sales_amount"),
        )
    )

    w_qty = Window.partitionBy("order_date", "category", "city").orderBy(col("sales_qty").desc())
    w_amt = Window.partitionBy("order_date", "category", "city").orderBy(col("sales_amount").desc())

    cat_ranking = (
        cat_daily
        .withColumn("qty_rank", row_number().over(w_qty))
        .withColumn("amount_rank", row_number().over(w_amt))
        .withColumnRenamed("order_date", "dt")
        .withColumn("sales_qty", col("sales_qty").cast(LongType()))
        .withColumn("sales_amount", col("sales_amount").cast(DecimalType(18, 2)))
        .withColumn("qty_rank", col("qty_rank").cast(IntegerType()))
        .withColumn("amount_rank", col("amount_rank").cast(IntegerType()))
    )

    cnt = cat_ranking.count()
    elapsed = time.time() - t0
    print("[DWS]   dws_category_ranking = {:,} 行, 耗时 {:.1f}s".format(cnt, elapsed))
    print("[DWS]   样例:")
    cat_ranking.filter(col("qty_rank") <= 5).orderBy("dt", "category", "qty_rank").show(10, truncate=False)
    print("=" * 60 + "\n")
    return cat_ranking


# ============================================================
# 6. 构建 dws_membership_contribution — 会员贡献
# ============================================================

def build_membership_contribution(spark, order_df):
    """
    按日/会员等级统计贡献
    输出: dws_membership_contribution (Hive)
    """
    print("=" * 60)
    print("[DWS] 构建 dws_membership_contribution — 会员价值贡献 ...")
    t0 = time.time()

    membership_daily = (
        order_df
        .groupBy("order_date", "membership_level")
        .agg(
            countDistinct("user_id").alias("user_count"),
            count("order_id").alias("total_orders"),
            spark_sum("pay_amount").alias("total_spend"),
            spark_round(avg("pay_amount"), 2).alias("avg_order_value"),
        )
        .withColumnRenamed("order_date", "dt")
    )

    # 计算各会员等级支付占比
    total_pay = order_df.agg(spark_sum("pay_amount").alias("global_total")).collect()[0][0]
    total_pay = float(total_pay) if total_pay else 1.0
    membership_daily = membership_daily.withColumn(
        "pay_ratio",
        spark_round(col("total_spend") / lit(total_pay), 4)
    )

    membership_daily = (
        membership_daily
        .withColumn("user_count", col("user_count").cast(LongType()))
        .withColumn("total_orders", col("total_orders").cast(LongType()))
        .withColumn("total_spend", col("total_spend").cast(DecimalType(18, 2)))
        .withColumn("avg_order_value", col("avg_order_value").cast(DecimalType(18, 2)))
    )

    cnt = membership_daily.count()
    elapsed = time.time() - t0
    print("[DWS]   dws_membership_contribution = {:,} 行, 耗时 {:.1f}s".format(cnt, elapsed))
    print("[DWS]   样例:")
    membership_daily.orderBy(col("dt").desc(), col("total_spend").desc()).show(10, truncate=False)
    print("=" * 60 + "\n")
    return membership_daily


# ============================================================
# 7. 写入 Hive DWS 表
# ============================================================

def write_dws_to_hive(spark, df, table_name):
    """
    将 DataFrame 写入 Hive DWS 内表
    使用 saveAsTable mode=overwrite 自动处理 schema 变更
    """
    full_name = "{}.{}".format(HIVE_DATABASE, table_name)
    t0 = time.time()
    print("[HIVE] 写入 {} ...".format(full_name))
    try:
        df.write.mode("overwrite").saveAsTable(full_name)
        elapsed = time.time() - t0
        print("[HIVE] ✓ {} 写入完成, 耗时 {:.1f}s".format(full_name, elapsed))
    except Exception as e:
        print("[HIVE] ✗ {} 写入失败: {}".format(full_name, e))
        raise


# ============================================================
# 8. 写入 HDFS 中间 Parquet 供 04/05 脚本使用
# ============================================================

def prepare_intermediate_datasets(data):
    """
    将 DWD 宽表转换为下游 04/05 脚本期望的列名和衍生字段。
    返回字典 {"order_clean", "inventory_features", "user_features"}
    """
    order_df = data["order"]
    inv_df = data["inventory"]
    beh_df = data["behavior"]

    # ---- order_clean: 04/05 共用 ----
    # 需要: product_id, order_date, quantity, pay_amount, order_id, user_id, discount_rate,
    #       category, membership (not membership_level), user_tag
    order_out = order_df
    if "membership_level" in order_out.columns:
        order_out = order_out.withColumnRenamed("membership_level", "membership")
    elif "membership" not in order_out.columns:
        order_out = order_out.withColumn("membership", lit("普通会员"))

    if "discount_rate" not in order_out.columns:
        total_col = "total_amount" if "total_amount" in order_out.columns else "pay_amount"
        disc_col = "discount_amount" if "discount_amount" in order_out.columns else None
        if disc_col:
            order_out = order_out.withColumn(
                "discount_rate",
                spark_round(
                    when(col(total_col) > 0, col(disc_col) / col(total_col))
                    .otherwise(lit(0)), 4
                )
            )
        else:
            order_out = order_out.withColumn("discount_rate", lit(0.0))

    if "user_tag" not in order_out.columns:
        order_out = order_out.withColumn("user_tag", lit(""))

    if "category" not in order_out.columns:
        order_out = order_out.withColumn("category", lit("未知"))

    # ---- inventory_features: 04 专用 ----
    # 需要: product_id, store_id, stock_qty, safety_stock, waste_qty,
    #       stock_to_safety_ratio, stock_turnover_ratio, promotion_flag, snapshot_date, category
    inv_out = inv_df

    # stock_to_safety_ratio = stock_qty / safety_stock
    inv_out = inv_out.withColumn(
        "stock_to_safety_ratio",
        spark_round(
            when(col("safety_stock") > 0, col("stock_qty") / col("safety_stock"))
            .otherwise(lit(0)), 2
        )
    )

    # stock_turnover_ratio: use existing or compute fallback
    if "stock_turnover_ratio" not in inv_out.columns:
        # 优先用 outbound_qty，否则用 reorder_point 估算，最差给 0
        if "outbound_qty" in inv_out.columns:
            inv_out = inv_out.withColumn(
                "stock_turnover_ratio",
                spark_round(
                    when(col("stock_qty") > 0,
                         col("outbound_qty") / col("stock_qty"))
                    .otherwise(lit(0)), 2
                )
            )
        elif "reorder_point" in inv_out.columns:
            inv_out = inv_out.withColumn(
                "stock_turnover_ratio",
                spark_round(
                    when(col("reorder_point") > 0,
                         col("stock_qty") / col("reorder_point"))
                    .otherwise(lit(0)), 2
                )
            )
        else:
            inv_out = inv_out.withColumn("stock_turnover_ratio", lit(0.0))

    # promotion_flag
    if "promotion_flag" not in inv_out.columns:
        inv_out = inv_out.withColumn("promotion_flag", lit(0))

    # ---- user_features: 05 专用 ----
    # 需要: event_id, user_id, action (not event_type), event_hour, page, stay_seconds (not duration_sec)
    beh_out = beh_df

    if "event_type" in beh_out.columns and "action" not in beh_out.columns:
        beh_out = beh_out.withColumnRenamed("event_type", "action")
    elif "action" not in beh_out.columns:
        beh_out = beh_out.withColumn("action", lit("unknown"))

    if "event_hour" not in beh_out.columns:
        if "event_time" in beh_out.columns:
            beh_out = beh_out.withColumn(
                "event_hour",
                expr("hour(to_timestamp(event_time))").cast(IntegerType())
            )
        elif "event_date" in beh_out.columns:
            beh_out = beh_out.withColumn("event_hour", lit(12))
        else:
            beh_out = beh_out.withColumn("event_hour", lit(0))

    if "stay_seconds" not in beh_out.columns and "duration_sec" in beh_out.columns:
        beh_out = beh_out.withColumnRenamed("duration_sec", "stay_seconds")
    elif "stay_seconds" not in beh_out.columns:
        beh_out = beh_out.withColumn("stay_seconds", lit(0))

    return {
        "order_clean": order_out,
        "inventory_features": inv_out,
        "user_features": beh_out,
    }


def write_intermediate_parquet(spark, data):
    """
    将订单/库存/行为的宽表转换为下游期望格式，写出到 HDFS features/ 目录。
    04_inventory_optimization.py 读取:
      - features/inventory_features
      - features/order_clean
    05_user_behavior_analysis.py 读取:
      - features/user_features
      - features/order_clean
    """
    print("=" * 60)
    print("[HDFS] 准备 + 写入中间数据集到 HDFS {} ...".format(HDFS_FEATURES_DIR))

    datasets = prepare_intermediate_datasets(data)

    for name, df in datasets.items():
        path = HDFS_FEATURES_DIR + "/" + name
        t0 = time.time()
        cnt = df.count()
        print("[HDFS]   写入 {} -> {:,} 行, {} 列 ...".format(name, cnt, len(df.columns)))
        try:
            df.write.mode("overwrite").parquet(path)
            elapsed = time.time() - t0
            print("[HDFS]   ✓ {} 写入完成, 耗时 {:.1f}s".format(path, elapsed))
        except Exception as e:
            print("[HDFS]   ✗ {} 写入失败: {}".format(path, e))
            raise

    print("=" * 60 + "\n")


# ============================================================
# 主函数
# ============================================================

def main():
    total_start = time.time()
    print("=" * 70)
    print("[START] 盒马鲜生 特征工程 Spark 任务启动 @ {}".format(
        datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    print("[INFO]  架构: Hive DWD -> Spark 特征聚合 -> Hive DWS + HDFS features")
    print("[INFO]  Hive DB = {}".format(HIVE_DATABASE))
    print("[INFO]  HDFS Base = {}".format(HDFS_BASE_PATH))
    print("[INFO]  产出表: dws_sales_daily / dws_inventory_daily / dws_user_rfm "
          "/ dws_category_ranking / dws_membership_contribution")
    print("=" * 70)

    spark = None
    try:
        spark = create_spark_session()

        # ---------- 1) 加载数据 ----------
        print("\n" + "-" * 50)
        print("[PHASE] ===== 第 1 步: 加载 Hive DWD 源数据 + 维度 JOIN")
        print("-" * 50)
        data = load_all_data(spark)

        # ---------- 2) 构建 DWS 表 ----------
        dws_tables = {}

        print("\n" + "-" * 50)
        print("[PHASE] ===== 第 2 步: 构建 dws_sales_daily")
        print("-" * 50)
        dws_tables["dws_sales_daily"] = build_sales_daily(spark, data["order"])

        print("\n" + "-" * 50)
        print("[PHASE] ===== 第 3 步: 构建 dws_inventory_daily")
        print("-" * 50)
        dws_tables["dws_inventory_daily"] = build_inventory_daily(spark, data["inventory"])

        print("\n" + "-" * 50)
        print("[PHASE] ===== 第 4 步: 构建 dws_user_rfm")
        print("-" * 50)
        dws_tables["dws_user_rfm"] = build_user_rfm(spark, data["order"])

        print("\n" + "-" * 50)
        print("[PHASE] ===== 第 5 步: 构建 dws_category_ranking")
        print("-" * 50)
        dws_tables["dws_category_ranking"] = build_category_ranking(spark, data["order"])

        print("\n" + "-" * 50)
        print("[PHASE] ===== 第 6 步: 构建 dws_membership_contribution")
        print("-" * 50)
        dws_tables["dws_membership_contribution"] = build_membership_contribution(spark, data["order"])

        # ---------- 3) 写入 Hive ----------
        print("\n" + "-" * 50)
        print("[PHASE] ===== 第 7 步: 写入 Hive DWS 表")
        print("-" * 50)
        hive_order = [
            "dws_sales_daily",
            "dws_inventory_daily",
            "dws_user_rfm",
            "dws_category_ranking",
            "dws_membership_contribution",
        ]
        for table_name in hive_order:
            write_dws_to_hive(spark, dws_tables[table_name], table_name)

        # ---------- 4) 写入 HDFS 中间 Parquet ----------
        print("\n" + "-" * 50)
        print("[PHASE] ===== 第 8 步: 写入 HDFS 中间 Parquet (供 04/05 使用)")
        print("-" * 50)
        write_intermediate_parquet(spark, data)

        # ---------- 总结 ----------
        total_elapsed = time.time() - total_start
        print("\n" + "=" * 70)
        print("[DONE] 特征工程任务完成 @ {}".format(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        print("[DONE] 总耗时: {:.1f} 秒".format(total_elapsed))
        print("[DONE] === 产出汇总 ===")
        for table_name in hive_order:
            df = dws_tables[table_name]
            print("[DONE]   {}.{} = {:,} 行, {} 列".format(
                HIVE_DATABASE, table_name, df.count(), len(df.columns)))
        print("[DONE] HDFS 中间数据: {}".format(HDFS_FEATURES_DIR))
        print("=" * 70)

    except Exception as e:
        print("\n[FATAL] 任务执行异常: {}".format(e))
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if spark is not None:
            spark.stop()
            print("[END] SparkSession 已停止")


if __name__ == "__main__":
    main()
