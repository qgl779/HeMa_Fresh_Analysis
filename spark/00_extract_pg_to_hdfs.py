# -*- coding: utf-8 -*-

import sys
import os
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.settings import PG_JDBC_URL, PG_JDBC_PROPERTIES, HDFS_BASE_PATH
from pyspark.sql import SparkSession

HDFS_RAW_DIR = HDFS_BASE_PATH + "/ods_raw"

TABLES = [
    # (PG schema.table,   HDFS 子目录,  Hive 表名) — 子目录必须和 02-hive-ddl.sql 的 LOCATION 一致
    ("ods.ods_order_info",         "order_info",          "hema_fresh.ods_order_info"),
    ("ods.ods_inventory_snapshot", "inventory_snapshot",  "hema_fresh.ods_inventory_snapshot"),
    ("ods.ods_user_behavior",      "user_behavior",       "hema_fresh.ods_user_behavior"),
    ("ods.dim_product",            "dim_product",         "hema_fresh.dim_product"),
    ("ods.dim_store",              "dim_store",           "hema_fresh.dim_store"),
    ("ods.dim_user",               "dim_user",            "hema_fresh.dim_user"),
]


def main():
    spark = (
        SparkSession.builder
        .appName("HemaFresh_Extract_PG_To_HDFS")
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
        .getOrCreate()
    )

    start_time = time.time()

    # 先切换到 hema_fresh 库
    spark.sql("CREATE DATABASE IF NOT EXISTS hema_fresh")
    spark.sql("USE hema_fresh")

    for pg_table, hdfs_subdir, hive_full_name in TABLES:
        df = (
            spark.read.jdbc(url=PG_JDBC_URL, table=pg_table, properties=PG_JDBC_PROPERTIES)
            .cache()
        )
        row_count = df.count()
        print("[INFO] 读取表 {0} 行数: {1}".format(pg_table, row_count))

        output_path = HDFS_RAW_DIR + "/" + hdfs_subdir
        df.write.mode("overwrite").parquet(output_path)
        print("[INFO] 写出路径: " + output_path + "  (对应 Hive 表: " + hive_full_name + ")")

        df.unpersist()

    # 刷新 Hive 元数据，确保外表能读到新文件
    for _, _, hive_full_name in TABLES:
        try:
            spark.sql("MSCK REPAIR TABLE " + hive_full_name)
            print("[INFO] 刷新分区/元数据: " + hive_full_name)
        except Exception as e:
            print("[WARN] 刷新 {0} 失败(外表无需分区刷新, 可忽略): {1}".format(hive_full_name, e))

    total_seconds = int(time.time() - start_time)
    print("[INFO] 总耗时: {0} 秒".format(total_seconds))

    spark.stop()


if __name__ == "__main__":
    main()
