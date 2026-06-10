# -*- coding: utf-8 -*-
"""
06_category_ranking.py
====================================
品类销售排名: 从 Hive DWD 订单表 + ODS 商品表 ->
计算每日品类 GMV 排名/增长率/份额 -> 写入 MySQL ads.ads_category_ranking
"""
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, rank, lag, round as spark_round,
    when, lit
)
from pyspark.sql.window import Window

try:
    from config.settings import MYSQL_JDBC_URL, MYSQL_JDBC_PROPERTIES
except Exception:
    MYSQL_JDBC_URL = "jdbc:mysql://192.168.10.144:3306/hema_fresh_ads?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai"
    MYSQL_JDBC_PROPERTIES = {
        "user": "hema_ads",
        "password": "hema2024",
        "driver": "com.mysql.cj.jdbc.Driver"
    }


def create_spark_session():
    builder = (
        SparkSession.builder
        .appName("HemaFresh_CategoryRanking")
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


def main():
    start = time.time()
    print("=" * 70)
    print("[START] 品类销售排名任务 @ {}".format(
        datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    print("=" * 70)

    spark = create_spark_session()

    try:
        # --- 读取订单(Hive) + 商品(HDFS Parquet) ---
        print("\n[LOAD] 读取 dwd_order_detail (Hive) + dim_product (HDFS Parquet) ...")
        t0 = time.time()

        spark.sql("USE hema_fresh")
        order_df = spark.table("dwd_order_detail")
        print("[LOAD] dwd_order_detail schema: order_id={}, product_id={}".format(
            order_df.schema["order_id"].dataType, order_df.schema["product_id"].dataType))

        # dim_product 没有注册为 Hive 表，直接读 HDFS parquet
        dim_product_path = "hdfs://192.168.10.128:9000/hema_fresh/ods_raw/dim_product"
        product_df = spark.read.parquet(dim_product_path).select(
            col("product_id"), "category"
        )
        print("[LOAD] dim_product schema: product_id={}".format(
            product_df.schema["product_id"].dataType))

        product_loaded = product_df.count()
        print("[LOAD] dim_product (HDFS): {:,} 行, sample product_ids:".format(product_loaded))
        product_df.select("product_id").show(5, False)

        # 查看 order 的 product_id 样例
        print("[LOAD] dwd_order_detail sample product_ids:")
        order_df.select("product_id").show(5, False)

        # JOIN: 统一 cast 为 string 避免类型不匹配
        order_df2 = order_df.withColumn("_pid", col("product_id").cast("string"))
        product_df2 = product_df.withColumn("_pid", col("product_id").cast("string"))

        df = order_df2.join(product_df2, "_pid", "left") \
            .filter(col("category").isNotNull())

        cnt = df.count()
        elapsed = time.time() - t0
        print("[LOAD] 已加载 {:,} 行订单, 耗时 {:.1f}s".format(cnt, elapsed))

        # --- 每日品类 GMV 聚合 ---
        print("\n[ANALYSIS] 计算每日品类 GMV ...")
        t0 = time.time()

        daily_cat = df.groupBy("order_date", "category").agg(
            spark_sum("pay_amount").alias("gmv")
        ).cache()

        cat_cnt = daily_cat.count()
        print("[ANALYSIS] 品类-日期组合: {:,} 行".format(cat_cnt))

        # --- 窗口函数: 排名 / 环比 / 份额 ---
        win_rank = Window.partitionBy("order_date").orderBy(col("gmv").desc())
        win_cat = Window.partitionBy("category").orderBy("order_date")
        win_date = Window.partitionBy("order_date")

        result = (
            daily_cat
            .withColumn("rank_no", rank().over(win_rank))
            .withColumn(
                "gmv_growth_rate",
                spark_round(
                    (col("gmv") - lag("gmv", 7).over(win_cat))
                    / when(lag("gmv", 7).over(win_cat) == 0, lit(None))
                    .otherwise(lag("gmv", 7).over(win_cat)),
                    3
                )
            )
            .withColumn(
                "gmv_share",
                spark_round(
                    col("gmv") / spark_sum("gmv").over(win_date),
                    3
                )
            )
            .select(
                col("order_date").alias("report_date"),
                "category",
                "rank_no",
                "gmv",
                "gmv_growth_rate",
                "gmv_share"
            )
        )

        print("[ANALYSIS] 品类排名样例 (前 10 行):")
        result.orderBy("report_date", "rank_no").show(10, False)

        # --- 写入 MySQL ---
        print("\n[MYSQL] 写入 ads_category_ranking ...")
        t0 = time.time()
        result_cnt = result.count()

        result.write.jdbc(
            url=MYSQL_JDBC_URL,
            table="ads_category_ranking",
            mode="overwrite",
            properties=MYSQL_JDBC_PROPERTIES
        )

        elapsed = time.time() - t0
        print("[MYSQL] 写入完成: {:,} 行, 耗时 {:.1f}s".format(result_cnt, elapsed))

        daily_cat.unpersist()
        total_elapsed = time.time() - start
        print("\n" + "=" * 70)
        print("[DONE] 品类销售排名任务完成, 总耗时 {:.1f}s".format(total_elapsed))
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
