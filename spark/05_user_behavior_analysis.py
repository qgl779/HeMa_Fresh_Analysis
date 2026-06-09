import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, round as spark_round, sum as spark_sum,
    count, countDistinct, avg, max as spark_max, min as spark_min,
    datediff, current_date, expr, row_number, rank, dense_rank,
    collect_list, array_contains, size, split
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType

from config.settings import SPARK_CONFIG


def create_spark_session():
    spark = SparkSession.builder \
        .appName(f"{SPARK_CONFIG['app_name']}_UserBehaviorAnalysis") \
        .master(SPARK_CONFIG["master"]) \
        .config("spark.executor.memory", SPARK_CONFIG["spark.executor.memory"]) \
        .config("spark.driver.memory", SPARK_CONFIG["spark.driver.memory"]) \
        .config("spark.sql.shuffle.partitions", "100") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_data(spark, data_dir):
    base = Path(data_dir) / "processed"
    dfs = {}
    path_map = {
        "order": base / "dwd_order_detail",
        "behavior": base / "dwd_user_behavior"
    }
    raw_prefix = Path(data_dir) / "raw"
    raw_map = {
        "user": raw_prefix / "dim_user.csv",
        "product": raw_prefix / "dim_product.csv"
    }
    for name, path in path_map.items():
        if path.exists():
            dfs[name] = spark.read.parquet(str(path))
            print(f"[LOAD] {name} → {dfs[name].count():,} rows")
        else:
            print(f"[WARN] {path} not found")
    for name, path in raw_map.items():
        if path.exists():
            dfs[name] = spark.read.option("header", True).option("inferSchema", True).csv(str(path))
            print(f"[LOAD] {name} → {dfs[name].count():,} rows")
    return dfs


def user_portrait_analysis(dfs):
    print("\n=== 用户画像分析 ===")

    order = dfs.get("order")
    user = dfs.get("user")
    if order is None:
        print("[WARN] 无订单数据")
        return

    user_purchase = order.groupBy("user_id").agg(
        count("order_id").alias("purchase_count"),
        spark_sum("pay_amount").alias("total_spend"),
        avg("pay_amount").alias("avg_order_value"),
        countDistinct("order_date").alias("active_days"),
        spark_max("order_date").alias("last_purchase_date"),
        spark_min("order_date").alias("first_purchase_date"),
        avg("discount_rate").alias("avg_discount_sensitivity"),
        spark_sum("quantity").alias("total_quantity")
    )

    today = order.agg(spark_max("order_date")).collect()[0][0]
    user_purchase = user_purchase.withColumn("recency", datediff(lit(today), col("last_purchase_date")))

    r_quantiles = user_purchase.approxQuantile("recency", [0.33, 0.66], 0.01)
    f_quantiles = user_purchase.approxQuantile("purchase_count", [0.33, 0.66], 0.01)
    m_quantiles = user_purchase.approxQuantile("total_spend", [0.33, 0.66], 0.01)

    user_purchase = user_purchase.withColumn(
        "r_score",
        when(col("recency") <= r_quantiles[0], 3)
        .when(col("recency") <= r_quantiles[1], 2)
        .otherwise(1)
    ).withColumn(
        "f_score",
        when(col("purchase_count") >= f_quantiles[1], 3)
        .when(col("purchase_count") >= f_quantiles[0], 2)
        .otherwise(1)
    ).withColumn(
        "m_score",
        when(col("total_spend") >= m_quantiles[1], 3)
        .when(col("total_spend") >= m_quantiles[0], 2)
        .otherwise(1)
    )

    user_purchase = user_purchase.withColumn(
        "rfm_segment",
        when((col("r_score") >= 2) & (col("f_score") >= 2) & (col("m_score") >= 2), "高价值用户")
        .when((col("r_score") >= 2) & (col("f_score") >= 2) & (col("m_score") < 2), "活跃用户")
        .when((col("r_score") < 2) & (col("f_score") >= 2) & (col("m_score") >= 2), "沉睡高价值")
        .when((col("r_score") >= 2) & (col("f_score") < 2) & (col("m_score") < 2), "新用户")
        .when((col("r_score") < 2) & (col("f_score") < 2) & (col("m_score") >= 2), "流失高价值")
        .otherwise("低价值用户")
    )

    rfm_summary = user_purchase.groupBy("rfm_segment").agg(
        count("user_id").alias("user_count"),
        spark_round(avg("total_spend"), 2).alias("avg_spend"),
        spark_round(avg("purchase_count"), 1).alias("avg_purchases"),
        spark_round(avg("recency"), 0).alias("avg_recency_days")
    ).orderBy(col("user_count").desc())

    print("\n--- RFM 用户分层 ---")
    rfm_summary.show(10, False)

    print("\n--- 用户分层占比 ---")
    total = user_purchase.count()
    rfm_summary.withColumn("ratio", spark_round(col("user_count") / total * 100, 1)).show(10, False)

    return user_purchase


def funnel_analysis(dfs):
    print("\n=== 用户行为漏斗分析 ===")

    behavior = dfs.get("behavior")
    if behavior is None:
        print("[WARN] 无行为数据")
        return

    action_counts = behavior.groupBy("action").agg(
        count("event_id").alias("event_count"),
        countDistinct("user_id").alias("unique_users")
    ).orderBy(col("event_count").desc())

    action_counts.show()

    funnel_order = ["view", "click_banner", "view_detail", "cart", "favorite", "share", "search"]
    funnel_data = {}
    for action in funnel_order:
        row = action_counts.filter(col("action") == action).first()
        funnel_data[action] = {"events": row["event_count"], "users": row["unique_users"]} if row else {"events": 0, "users": 0}

    print("\n--- 关键转化率 ---")
    actions_with_data = [(a, d) for a, d in funnel_data.items() if d["events"] > 0]
    for i in range(1, len(actions_with_data)):
        a1, d1 = actions_with_data[i - 1]
        a2, d2 = actions_with_data[i]
        ratio = d2["events"] / d1["events"] * 100 if d1["events"] > 0 else 0
        print(f"  {a1} → {a2}: {ratio:.1f}%")


def time_pattern_analysis(dfs):
    print("\n=== 用户行为时间模式 ===")

    behavior = dfs.get("behavior")
    if behavior is None:
        return

    hourly_pattern = behavior.groupBy("event_hour").agg(
        count("event_id").alias("event_count")
    ).orderBy("event_hour")
    print("\n--- 24小时活跃度分布 ---")
    hourly_pattern.withColumn("ratio", spark_round(col("event_count") / behavior.count() * 100, 1)).show(24, False)

    page_heatmap = behavior.groupBy("page").agg(
        count("event_id").alias("page_views"),
        countDistinct("user_id").alias("unique_visitors"),
        spark_round(avg("stay_seconds"), 1).alias("avg_stay_seconds")
    ).orderBy(col("page_views").desc())
    print("\n--- 页面热度排名 ---")
    page_heatmap.show(10, False)


def category_preference_analysis(dfs):
    print("\n=== 品类偏好分析 ===")

    order = dfs.get("order")
    product = dfs.get("product")
    if order is None or product is None:
        print("[WARN] 缺少必要数据")
        return

    order_with_cat = order.join(product.select("product_id", "category"), "product_id", "left")

    user_cat_pref = order_with_cat.groupBy("user_id", "category").agg(
        spark_sum("pay_amount").alias("category_spend"),
        count("order_id").alias("category_orders")
    )

    window_cat = Window.partitionBy("user_id").orderBy(col("category_spend").desc())
    top_cat = user_cat_pref.withColumn("rank", row_number().over(window_cat)).filter(col("rank") == 1)
    category_dist = top_cat.groupBy("category").agg(
        count("user_id").alias("preference_users")
    ).orderBy(col("preference_users").desc())

    print("\n--- 品类首选用户分布 ---")
    category_dist.show(15, False)

    order_user_count = order_with_cat.groupBy("category", "user_id").agg(
        count("order_id").alias("orders")
    )
    repurchase = order_user_count.filter(col("orders") > 1).groupBy("category").agg(
        count("user_id").alias("repurchase_users")
    )

    print("\n--- 品类复购用户数 Top10 ---")
    repurchase.orderBy(col("repurchase_users").desc()).show(10, False)


def membership_analysis(dfs):
    print("\n=== 会员价值分析 ===")

    order = dfs.get("order")
    user = dfs.get("user")
    if order is None or user is None:
        print("[WARN] 缺少必要数据")
        return

    order_with_user = order.join(user.select("user_id", "membership", "user_tag"), "user_id", "left")

    membership_stats = order_with_user.groupBy("membership").agg(
        countDistinct("user_id").alias("member_count"),
        spark_round(avg("pay_amount"), 2).alias("avg_order_value"),
        spark_round(spark_sum("pay_amount"), 2).alias("total_gmv"),
        spark_round(avg("discount_rate"), 3).alias("avg_discount_rate")
    ).orderBy(col("total_gmv").desc())

    print("\n--- 会员等级贡献 ---")
    membership_stats.show(10, False)

    tag_stats = order_with_user.groupBy("user_tag").agg(
        countDistinct("user_id").alias("user_count"),
        spark_round(avg("pay_amount"), 2).alias("avg_order_value"),
        spark_round(spark_sum("quantity"), 0).alias("total_quantity")
    ).orderBy(col("user_count").desc())

    print("\n--- 用户标签消费特征 ---")
    tag_stats.show(10, False)


if __name__ == "__main__":
    spark = create_spark_session()
    data_dir = str(Path(__file__).resolve().parents[1] / "data")

    dfs = load_data(spark, data_dir)

    rfm_result = user_portrait_analysis(dfs)

    funnel_analysis(dfs)

    time_pattern_analysis(dfs)

    category_preference_analysis(dfs)

    membership_analysis(dfs)

    print("\n=== 用户行为分析完成 ===")
    spark.stop()
