import os
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, round as spark_round, sum as spark_sum,
    count, countDistinct, avg, max as spark_max, min as spark_min,
    datediff, current_date, expr, row_number, rank, dense_rank
)
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window

from config import settings


def create_spark_session():
    print("[INIT] 正在构建 SparkSession ...")
    spark = SparkSession.builder \
        .appName("HemaFresh_UserBehaviorAnalysis") \
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
    print("[INIT] SparkSession 构建完成：appName=HemaFresh_UserBehaviorAnalysis, master={}".format(settings.SPARK_CONFIG.get("master", "yarn")))
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
        behavior_path = settings.HDFS_FEATURES_DIR + "/user_features"
        order_path = settings.HDFS_FEATURES_DIR + "/order_clean"
        print("[LOAD] 集群模式，读取 HDFS 行为路径: {}".format(behavior_path))
        try:
            if _hdfs_exists(spark, behavior_path):
                dfs["behavior"] = spark.read.parquet(behavior_path)
                print("[LOAD] user_features -> {} 行".format(dfs["behavior"].count()))
            else:
                print("[WARN] HDFS 行为特征路径不存在: {}".format(behavior_path))
        except Exception as e:
            print("[WARN] 读取 HDFS 行为特征失败: {}".format(str(e)))

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
        behavior_path = base / "dwd_user_behavior"
        order_path = base / "dwd_order_detail"
        print("[LOAD] 本地模式，行为路径: {}".format(str(behavior_path)))
        print("[LOAD] 本地模式，订单路径: {}".format(str(order_path)))
        if behavior_path.exists():
            dfs["behavior"] = spark.read.parquet(str(behavior_path))
            print("[LOAD] behavior -> {} 行".format(dfs["behavior"].count()))
        if order_path.exists():
            dfs["order"] = spark.read.parquet(str(order_path))
            print("[LOAD] order -> {} 行".format(dfs["order"].count()))

    return dfs


def user_portrait_analysis(dfs):
    print("\n=== 用户画像分析 / RFM 分层 ===")

    order = dfs.get("order")
    if order is None:
        print("[WARN] 无订单数据，跳过 RFM 分析")
        return None

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
    print("  R 分位数阈值: {}".format(r_quantiles))
    print("  F 分位数阈值: {}".format(f_quantiles))
    print("  M 分位数阈值: {}".format(m_quantiles))

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

    print("\n  === RFM 用户分层统计 ===")
    rfm_summary.show(10, False)

    total = user_purchase.count()
    print("\n  === RFM 用户分层占比 (总用户数: {}) ===".format(total))
    rfm_summary.withColumn("ratio", spark_round(col("user_count") / total * 100, 1)).show(10, False)

    return user_purchase


def funnel_analysis(dfs):
    print("\n=== 用户行为漏斗分析 ===")

    behavior = dfs.get("behavior")
    if behavior is None:
        print("[WARN] 无行为数据，跳过漏斗分析")
        return

    action_counts = behavior.groupBy("action").agg(
        count("event_id").alias("event_count"),
        countDistinct("user_id").alias("unique_users")
    ).orderBy(col("event_count").desc())

    print("  === 行为类型统计 ===")
    action_counts.show()

    funnel_order = ["view", "click_banner", "view_detail", "cart", "favorite", "share", "search"]
    funnel_data = {}
    for action in funnel_order:
        row = action_counts.filter(col("action") == action).first()
        funnel_data[action] = {"events": row["event_count"], "users": row["unique_users"]} if row else {"events": 0, "users": 0}

    print("\n  === 关键转化率 ===")
    actions_with_data = [(a, d) for a, d in funnel_data.items() if d["events"] > 0]
    for i in range(1, len(actions_with_data)):
        a1, d1 = actions_with_data[i - 1]
        a2, d2 = actions_with_data[i]
        ratio = d2["events"] / d1["events"] * 100 if d1["events"] > 0 else 0
        print("  {} -> {}: {:.1f}%".format(a1, a2, ratio))


def time_pattern_analysis(dfs):
    print("\n=== 用户行为时间模式 ===")

    behavior = dfs.get("behavior")
    if behavior is None:
        print("[WARN] 无行为数据，跳过时间模式分析")
        return

    hourly_pattern = behavior.groupBy("event_hour").agg(
        count("event_id").alias("event_count")
    ).orderBy("event_hour")
    total_events = behavior.count()
    print("  === 24 小时活跃度分布 (总事件数: {}) ===".format(total_events))
    hourly_pattern.withColumn("ratio_pct", spark_round(col("event_count") / total_events * 100, 1)).show(24, False)

    if "page" in behavior.columns:
        page_heatmap = behavior.groupBy("page").agg(
            count("event_id").alias("page_views"),
            countDistinct("user_id").alias("unique_visitors"),
            spark_round(avg("stay_seconds"), 1).alias("avg_stay_seconds")
        ).orderBy(col("page_views").desc())
        print("\n  === 页面热度排名 ===")
        page_heatmap.show(10, False)


def category_preference_analysis(dfs):
    print("\n=== 品类偏好分析 ===")

    order = dfs.get("order")
    if order is None or "category" not in order.columns:
        print("[WARN] 缺少订单或品类字段，跳过品类偏好分析")
        return

    user_cat_pref = order.groupBy("user_id", "category").agg(
        spark_sum("pay_amount").alias("category_spend"),
        count("order_id").alias("category_orders")
    )

    window_cat = Window.partitionBy("user_id").orderBy(col("category_spend").desc())
    top_cat = user_cat_pref.withColumn("rank", row_number().over(window_cat)).filter(col("rank") == 1)
    category_dist = top_cat.groupBy("category").agg(
        count("user_id").alias("preference_users")
    ).orderBy(col("preference_users").desc())

    print("  === 品类首选用户分布 ===")
    category_dist.show(15, False)

    order_user_count = order.groupBy("category", "user_id").agg(
        count("order_id").alias("orders")
    )
    repurchase = order_user_count.filter(col("orders") > 1).groupBy("category").agg(
        count("user_id").alias("repurchase_users")
    )

    print("\n  === 品类复购用户数 Top10 ===")
    repurchase.orderBy(col("repurchase_users").desc()).show(10, False)


def membership_analysis(dfs):
    print("\n=== 会员价值分析 ===")

    order = dfs.get("order")
    if order is None:
        print("[WARN] 无订单数据，跳过会员分析")
        return

    if "membership" in order.columns:
        membership_stats = order.groupBy("membership").agg(
            countDistinct("user_id").alias("member_count"),
            spark_round(avg("pay_amount"), 2).alias("avg_order_value"),
            spark_round(spark_sum("pay_amount"), 2).alias("total_gmv"),
            spark_round(avg("discount_rate"), 3).alias("avg_discount_rate")
        ).orderBy(col("total_gmv").desc())

        print("  === 会员等级贡献 ===")
        membership_stats.show(10, False)

    if "user_tag" in order.columns:
        tag_stats = order.groupBy("user_tag").agg(
            countDistinct("user_id").alias("user_count"),
            spark_round(avg("pay_amount"), 2).alias("avg_order_value"),
            spark_round(spark_sum("quantity"), 0).alias("total_quantity")
        ).orderBy(col("user_count").desc())

        print("\n  === 用户标签消费特征 ===")
        tag_stats.show(10, False)


def write_rfm_to_mysql(rfm_df):
    print("\n[MYSQL] 写入 RFM 分层结果到 MySQL hema_fresh_ads.ads_user_segment_report ...")
    try:
        rfm_df.select(
            "user_id", "rfm_segment", "r_score", "f_score", "m_score",
            "recency", "purchase_count", "total_spend", "avg_order_value"
        ).withColumnRenamed("recency", "recency_days").write.jdbc(
            url=settings.MYSQL_JDBC_URL,
            table="ads_user_segment_report",
            mode="overwrite",
            properties=settings.MYSQL_JDBC_PROPERTIES
        )
        print("[MYSQL] 写入完成，共 {} 行".format(rfm_df.count()))
    except Exception as e:
        print("[WARN] MySQL 写入失败: {}".format(str(e)))


if __name__ == "__main__":
    start_time = time.time()
    print("=" * 70)
    print("[START] 盒马鲜生 用户行为分析 Spark 任务启动 @ {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    print("[INFO]  CLUSTER_MODE = {}, master = {}".format(settings.CLUSTER_MODE, settings.SPARK_CONFIG.get("master", "yarn")))
    print("=" * 70)

    spark = create_spark_session()

    try:
        dfs = load_data(spark, settings.CLUSTER_MODE)

        rfm_result = user_portrait_analysis(dfs)

        funnel_analysis(dfs)

        time_pattern_analysis(dfs)

        category_preference_analysis(dfs)

        membership_analysis(dfs)

        if rfm_result is not None:
            if settings.CLUSTER_MODE:
                rfm_hdfs = settings.HDFS_FEATURES_DIR + "/user_rfm_segment"
                print("\n[SAVE] RFM 分层结果写入 HDFS: {}".format(rfm_hdfs))
                rfm_result.write.mode("overwrite").parquet(rfm_hdfs)
            else:
                local_base = Path(__file__).resolve().parents[1] / "data" / "features"
                local_base.mkdir(parents=True, exist_ok=True)
                rfm_local = str(local_base / "user_rfm_segment")
                print("\n[SAVE] RFM 分层结果写入本地: {}".format(rfm_local))
                rfm_result.write.mode("overwrite").parquet(rfm_local)

            write_rfm_to_mysql(rfm_result)

        elapsed = time.time() - start_time
        print("\n" + "=" * 70)
        print("[DONE] 用户行为分析任务完成，耗时 {:.1f} 秒".format(elapsed))
        if rfm_result is not None:
            print("  用户分层样本数: {}".format(rfm_result.count()))
        print("=" * 70)
    except Exception as e:
        print("[FATAL] 任务执行异常: {}".format(str(e)))
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        print("[END] SparkSession 已停止")
