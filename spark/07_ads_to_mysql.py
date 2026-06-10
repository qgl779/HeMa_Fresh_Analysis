# -*- coding: utf-8 -*-
"""
07_ads_to_mysql.py
====================================
从 Hive DWS 表读取数据，聚合后写入 MySQL ADS 层:
  - ads_daily_sales_summary   (dws_sales_daily 按天聚合)
  - ads_membership_contribution (dws_membership_contribution 按会员等级聚合)
"""
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, countDistinct, count,
    round as spark_round, lit, when, current_date, to_date
)

MYSQL_JDBC_URL = "jdbc:mysql://192.168.10.144:3306/hema_fresh_ads?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai"
MYSQL_JDBC_PROPERTIES = {
    "user": "hema_ads",
    "password": "hema2024",
    "driver": "com.mysql.cj.jdbc.Driver"
}


def create_spark_session():
    builder = (
        SparkSession.builder
        .appName("HemaFresh_ADSToMySQL")
        .master("yarn")
        .config("spark.submit.deployMode", "client")
        .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.10.128:9000")
        .config("spark.executor.instances", "3")
        .config("spark.executor.cores", "2")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "2g")
        .config("spark.driver.host", "192.168.10.128")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.adaptive.enabled", "true")
        .config("hive.metastore.uris", "thrift://192.168.10.128:9083")
        .enableHiveSupport()
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_daily_sales_summary(spark):
    """从 dws_sales_daily 聚合每日数据 → MySQL ads_daily_sales_summary"""
    print("\n" + "=" * 60)
    print("[TASK] 1/2: ads_daily_sales_summary")
    print("=" * 60)
    t0 = time.time()

    spark.sql("USE hema_fresh")
    dws = spark.table("dws_sales_daily")

    # dws_sales_daily is product-day level, aggregate to day level
    daily = (
        dws.groupBy("order_date")
        .agg(
            spark_sum("order_count").alias("total_orders"),
            spark_sum("pay_amount").alias("total_sales"),
            spark_sum("user_count").alias("total_users"),
            spark_sum("discount_amount").alias("total_discount"),
            spark_sum("sales_amount").alias("total_sales_orig"),
        )
        .withColumn(
            "avg_order_value",
            spark_round(col("total_sales") / when(col("total_orders") > 0, col("total_orders")).otherwise(lit(1)), 2)
        )
        .withColumn(
            "discount_rate",
            spark_round(
                when(col("total_sales_orig") > 0, col("total_discount") / col("total_sales_orig"))
                .otherwise(lit(0.0)), 4
            )
        )
        .select(
            col("order_date").alias("dt"),
            col("total_orders").cast("int"),
            col("total_sales").cast("double"),
            col("total_users").cast("int"),
            col("avg_order_value").cast("double"),
            col("discount_rate").cast("double"),
            lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("create_time"),
        )
    )

    cnt = daily.count()
    print("[INFO]  聚合后: {:,} 天".format(cnt))
    daily.orderBy("dt").show(5, False)

    daily.write.jdbc(
        url=MYSQL_JDBC_URL,
        table="ads_daily_sales_summary",
        mode="overwrite",
        properties=MYSQL_JDBC_PROPERTIES
    )
    elapsed = time.time() - t0
    print("[DONE] ads_daily_sales_summary: {:,} 行写入, 耗时 {:.1f}s".format(cnt, elapsed))


def write_membership_contribution(spark):
    """从 dws_membership_contribution 聚合会员数据 → MySQL ads_membership_contribution"""
    print("\n" + "=" * 60)
    print("[TASK] 2/2: ads_membership_contribution")
    print("=" * 60)
    t0 = time.time()

    spark.sql("USE hema_fresh")
    dws = spark.table("dws_membership_contribution")

    # dws_membership_contribution has dt + membership_level level, aggregate to membership only
    membership = (
        dws.groupBy("membership_level")
        .agg(
            spark_sum("user_count").alias("user_count"),
            spark_sum("total_orders").alias("total_orders"),
            spark_sum("total_spend").alias("total_spend"),
        )
    )

    # Calculate avg_order_value and pay_ratio
    total_spend_all = membership.agg(spark_sum("total_spend").alias("grand_total")).collect()[0][0]
    total_spend_all = float(total_spend_all) if total_spend_all else 1.0

    result = membership.withColumn(
        "avg_order_value",
        spark_round(
            col("total_spend") / when(col("total_orders") > 0, col("total_orders")).otherwise(lit(1)), 2
        )
    ).withColumn(
        "pay_ratio",
        spark_round(col("total_spend") / lit(total_spend_all), 4)
    ).select(
        "membership_level",
        col("user_count").cast("int"),
        col("total_orders").cast("int"),
        col("total_spend").cast("double"),
        col("avg_order_value").cast("double"),
        col("pay_ratio").cast("double"),
        lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("update_time"),
    )

    cnt = result.count()
    print("[INFO]  聚合后: {:,} 个会员等级".format(cnt))
    result.show(10, False)

    result.write.jdbc(
        url=MYSQL_JDBC_URL,
        table="ads_membership_contribution",
        mode="overwrite",
        properties=MYSQL_JDBC_PROPERTIES
    )
    elapsed = time.time() - t0
    print("[DONE] ads_membership_contribution: {:,} 行写入, 耗时 {:.1f}s".format(cnt, elapsed))


def main():
    start = time.time()
    print("=" * 70)
    print("[START] ADS → MySQL 写入任务 @ {}".format(
        datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    print("=" * 70)

    spark = create_spark_session()

    try:
        write_daily_sales_summary(spark)
        write_membership_contribution(spark)

        total_elapsed = time.time() - start
        print("\n" + "=" * 70)
        print("[ALL DONE] ADS → MySQL 写入完成, 总耗时 {:.1f}s".format(total_elapsed))
        print("=" * 70)

    except Exception as e:
        print("\n[FATAL] 任务异常: {}".format(e))
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        print("[END] SparkSession 已停止")


if __name__ == "__main__":
    main()
