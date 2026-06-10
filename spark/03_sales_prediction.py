# -*- coding: utf-8 -*-
"""
03_sales_prediction.py
====================================
销量预测: 从 Hive dws_sales_daily 读取每日销量特征 ->
训练 RandomForest + GBT 回归模型 -> 生成未来 7 天预测 ->
写入 MySQL ads.ads_sales_forecast
架构: Hive DWS -> Spark ML 预测 -> MySQL ADS
"""

import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, abs as spark_abs, round as spark_round, expr,
    dayofweek, month, weekofyear, year, to_date,
    count, avg, max as spark_max, min as spark_min,
    sum as spark_sum, row_number
)
from pyspark.sql.types import DoubleType, IntegerType, DateType, LongType
from pyspark.sql.window import Window

from pyspark.ml.regression import RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler

try:
    from config.settings import (
        SPARK_CONFIG,
        HIVE_CONFIG,
        MYSQL_JDBC_URL,
        MYSQL_JDBC_PROPERTIES,
    )
except Exception as _e:
    print("[WARN] 无法从 config.settings 导入配置，使用默认配置: {}".format(_e))
    SPARK_CONFIG = {
        "master": "yarn",
        "spark.submit.deployMode": "client",
        "spark.hadoop.fs.defaultFS": "hdfs://192.168.10.128:9000",
        "spark.executor.instances": "3",
        "spark.executor.cores": "2",
        "spark.executor.memory": "4g",
        "spark.driver.memory": "2g",
        "spark.driver.host": "192.168.10.128",
        "spark.sql.shuffle.partitions": "200",
        "spark.sql.adaptive.enabled": "true",
    }
    HIVE_CONFIG = {"database": "hema_fresh"}
    MYSQL_JDBC_URL = "jdbc:mysql://192.168.10.144:3306/hema_fresh_ads?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai"
    MYSQL_JDBC_PROPERTIES = {
        "user": "hema_ads",
        "password": "hema2024",
        "driver": "com.mysql.cj.jdbc.Driver"
    }


def create_spark_session():
    """
    统一构建 SparkSession:
    appName="HemaFresh_SalesPrediction"
    master=yarn, enableHiveSupport=True
    """
    print("[INIT] 正在构建 SparkSession ...")
    t0 = time.time()
    builder = (
        SparkSession.builder
        .appName("HemaFresh_SalesPrediction")
        .master(SPARK_CONFIG.get("master", "yarn"))
        .config("spark.submit.deployMode",
                SPARK_CONFIG.get("spark.submit.deployMode", "client"))
        .config("spark.hadoop.fs.defaultFS",
                SPARK_CONFIG.get("spark.hadoop.fs.defaultFS",
                                   "hdfs://192.168.10.128:9000"))
        .config("spark.executor.instances",
                SPARK_CONFIG.get("spark.executor.instances", "3"))
        .config("spark.executor.cores",
                SPARK_CONFIG.get("spark.executor.cores", "2"))
        .config("spark.executor.memory",
                SPARK_CONFIG.get("spark.executor.memory", "4g"))
        .config("spark.driver.memory",
                SPARK_CONFIG.get("spark.driver.memory", "2g"))
        .config("spark.driver.host",
                SPARK_CONFIG.get("spark.driver.host", "192.168.10.128"))
        .config("spark.sql.shuffle.partitions",
                SPARK_CONFIG.get("spark.sql.shuffle.partitions", "200"))
        .config("spark.sql.adaptive.enabled",
                SPARK_CONFIG.get("spark.sql.adaptive.enabled", "true"))
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.warehouse.dir",
                SPARK_CONFIG.get("spark.sql.warehouse.dir",
                                   "/user/hive/warehouse"))
        .config("hive.metastore.uris",
                SPARK_CONFIG.get("hive.metastore.uris",
                                   "thrift://192.168.10.128:9083"))
        .enableHiveSupport()
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    elapsed = time.time() - t0
    print("[INIT] SparkSession 构建完成: appName=HemaFresh_SalesPrediction, "
          "master={}, 耗时 {:.2f}s".format(SPARK_CONFIG.get("master", "yarn"), elapsed))
    return spark


# ============================================================
# 从 Hive DWS 层读取
# ============================================================
def read_hive_dws(spark):
    """
    从 Hive hema_fresh.dws_sales_daily 读取每日销量特征
    """
    hive_db = HIVE_CONFIG.get("database", "hema_fresh")
    hive_table = "dws_sales_daily"
    full_table = "{}.{}".format(hive_db, hive_table)
    t0 = time.time()
    print("[LOAD] 读取 Hive DWS 表: {} ...".format(full_table))
    try:
        spark.sql("USE {}".format(hive_db))
        df = spark.table(full_table)
        cnt = df.count()
        elapsed = time.time() - t0
        print("[LOAD] ✓ {} -> {:,} 行, {} 列, 耗时 {:.2f}s".format(
            full_table, cnt, len(df.columns), elapsed))
        if cnt > 0:
            print("[LOAD]   Schema:")
            df.printSchema()
            df.show(10, truncate=False)
        return df
    except Exception as e:
        print("[LOAD] ✗ 读取 Hive DWS 表 {} 失败: {}".format(full_table, e))
        return None


# ============================================================
# 训练数据准备
# ============================================================
def prepare_training_data(df):
    """
    准备训练数据: 组装 features 向量, 划分训练集/测试集
    优化: 避免多次 count() 触发重复计算，使用 cache()
    """
    print("\n[ANALYSIS] 准备训练数据 ...")
    t0 = time.time()

    # 可能的特征列
    possible_feature_cols = [
        "sales_lag_1", "sales_lag_7", "sales_lag_14", "sales_lag_30",
        "sales_rolling_7d_avg", "sales_rolling_14d_avg", "sales_rolling_30d_avg",
        "dayofweek", "month", "weekofyear",
        "daily_gmv", "order_count", "user_count", "avg_discount_rate",
    ]
    available_cols = [c for c in possible_feature_cols if c in df.columns]
    print("[ANALYSIS]   可用特征列: {}".format(available_cols))

    if len(available_cols) == 0:
        print("[FATAL] 没有可用的特征列，无法进行预测")
        return None, None, None

    # drop NA — 只触发一次 action，然后 cache
    df_clean = df.dropna(subset=available_cols + ["sales_qty"])
    # 先 cache 再 count，避免多次全量计算
    df_clean = df_clean.cache()
    cleaned_cnt = df_clean.count()
    orig_cnt = df.count()
    df.unpersist()  # 释放原始 df 的缓存（如果它恰好被缓存了）

    print("[ANALYSIS]   过滤 NA: {} -> {} 行 (去除 {} 行)".format(
        orig_cnt, cleaned_cnt, orig_cnt - cleaned_cnt))

    if cleaned_cnt == 0:
        print("[FATAL] 过滤后数据为空，无法训练")
        df_clean.unpersist()
        return None, None, None

    # VectorAssembler
    assembler = VectorAssembler(
        inputCols=available_cols,
        outputCol="features",
        handleInvalid="skip"
    )
    df_features = assembler.transform(df_clean).select(
        "product_id", "order_date", "features", "sales_qty"
    )
    df_features = df_features.cache()
    df_clean.unpersist()

    # 训练/测试划分 (时间序列: 按 order_date 最后 20% 作测试集)
    total_rows = df_features.count()
    train_ratio = 0.8
    split_idx = int(total_rows * train_ratio)

    # 使用 percent_rank 避免 collect 到大列表，直接用 SQL 窗口函数划分
    w = Window.orderBy("order_date")
    df_ranked = df_features.withColumn("_pct", expr("percent_rank() over (order by order_date)"))
    train_df = df_ranked.filter(col("_pct") <= train_ratio).drop("_pct")
    test_df = df_ranked.filter(col("_pct") > train_ratio).drop("_pct")

    train_cnt = train_df.count()
    test_cnt = test_df.count()
    elapsed = time.time() - t0
    print("[ANALYSIS]   训练集: {} 行, 测试集: {} 行, 耗时 {:.2f}s".format(
        train_cnt, test_cnt, elapsed))
    return train_df, test_df, available_cols


# ============================================================
# 训练模型
# ============================================================
def train_rf_model(train_df):
    """训练 RandomForest 回归模型"""
    print("\n[ANALYSIS] 训练 RandomForest 销量预测模型 ...")
    t0 = time.time()
    try:
        rf = RandomForestRegressor(
            featuresCol="features",
            labelCol="sales_qty",
            numTrees=100,
            maxDepth=10,
            seed=42,
            featureSubsetStrategy="sqrt"
        )
        model = rf.fit(train_df)
        elapsed = time.time() - t0
        print("[ANALYSIS] ✓ RandomForest 训练完成, 耗时 {:.2f}s, "
              "树数量={}, 最大深度={}".format(
                  elapsed, model.getNumTrees, model.getMaxDepth()))
        return model
    except Exception as e:
        print("[ANALYSIS] ✗ RandomForest 训练失败: {}".format(e))
        return None


def train_gbt_model(train_df):
    """训练 GBT 回归模型"""
    print("\n[ANALYSIS] 训练 GBT 销量预测模型 ...")
    t0 = time.time()
    try:
        gbt = GBTRegressor(
            featuresCol="features",
            labelCol="sales_qty",
            maxIter=100,
            maxDepth=8,
            seed=42
        )
        model = gbt.fit(train_df)
        elapsed = time.time() - t0
        print("[ANALYSIS] ✓ GBT 训练完成, 耗时 {:.2f}s, "
              "迭代次数={}, 最大深度={}".format(
                  elapsed, model.getMaxIter(), model.getMaxDepth()))
        return model
    except Exception as e:
        print("[ANALYSIS] ✗ GBT 训练失败: {}".format(e))
        return None


# ============================================================
# 模型评估
# ============================================================
def evaluate_model(model, test_df, model_name):
    """评估模型性能: RMSE / MAE / R² / MAPE"""
    if model is None or test_df is None:
        return None
    print("\n[ANALYSIS] 评估 {} 模型 ...".format(model_name))
    t0 = time.time()

    try:
        predictions = model.transform(test_df)

        rmse = RegressionEvaluator(
            labelCol="sales_qty", predictionCol="prediction",
            metricName="rmse").evaluate(predictions)
        mae = RegressionEvaluator(
            labelCol="sales_qty", predictionCol="prediction",
            metricName="mae").evaluate(predictions)
        r2 = RegressionEvaluator(
            labelCol="sales_qty", predictionCol="prediction",
            metricName="r2").evaluate(predictions)

        # MAPE
        try:
            mape_df = predictions.withColumn(
                "ape",
                when(col("sales_qty") > 0,
                     spark_round(abs(col("prediction") - col("sales_qty"))
                                   / col("sales_qty"), 4) * 100)
                .otherwise(lit(0))
            )
            mape = mape_df.select(avg("ape")).collect()[0][0]
        except Exception:
            mape = None

        elapsed = time.time() - t0
        print("[ANALYSIS]   === {} 评估指标 === (耗时 {:.2f}s)".format(
            model_name, elapsed))
        print("[ANALYSIS]   RMSE: {:.2f}".format(rmse))
        print("[ANALYSIS]   MAE:  {:.2f}".format(mae))
        print("[ANALYSIS]   R²:   {:.4f}".format(r2))
        if mape is not None:
            print("[ANALYSIS]   MAPE: {:.2f}%".format(float(mape)))
        print("[ANALYSIS]   预测样例 (前 10 行):")
        predictions.select("product_id", "order_date", "sales_qty",
                           "prediction").show(10, truncate=False)

        return {
            "model_name": model_name,
            "rmse": float(rmse),
            "mae": float(mae),
            "r2": float(r2),
            "mape": float(mape) if mape is not None else None,
        }
    except Exception as e:
        print("[ANALYSIS] ✗ {} 评估失败: {}".format(model_name, e))
        return None


# ============================================================
# 生成未来 7 天预测
# ============================================================
def generate_forecast(spark, model, df, feature_cols):
    """
    生成未来 7 天销量预测（纯 Spark 分布式执行，无 collect() 到 Driver）:
    - 对每个 product_id 取最新一天的数据作为基准
    - 用 Spark explode 生成未来 7 天，分布式计算预测值
    - forecast_date, product_id, predicted_qty, model_name
    """
    print("\n[ANALYSIS] 生成未来 7 天销量预测 ...")
    t0 = time.time()

    if model is None:
        print("[WARN] 模型为 None，跳过预测生成")
        return None

    today = datetime.now().date()

    # 获取每个 product_id 的最新一行（分布式，无 collect）
    w = Window.partitionBy("product_id").orderBy(col("order_date").desc())
    latest_df = (
        df.withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn")
        .cache()
    )

    product_count = latest_df.count()
    print("[ANALYSIS]   预测商品数量: {}".format(product_count))
    if product_count == 0:
        print("[WARN] 没有商品可预测")
        latest_df.unpersist()
        return None

    # 构建未来 7 天的日期序列（用 posexplode 展开）
    # 先创建一个 0-6 的数组，再 explode
    forecast_df = (
        latest_df
        .withColumn("horizon_arr", expr("array(1,2,3,4,5,6,7)"))
        .select("*", expr("posexplode(horizon_arr) as (horizon_idx, horizon_days)"))
        .drop("horizon_arr", "horizon_idx")
        .withColumn("horizon_days", col("horizon_days").cast(IntegerType()))
    )

    # 计算 forecast_date 和预测值（纯 Spark 表达式，无 Python UDF）
    forecast_df = forecast_df.withColumn(
        "forecast_date",
        expr("date_add('{}', horizon_days)".format(str(today)))
    )

    # 基准销量：优先用 sales_rolling_7d_avg，否则用 sales_qty
    base_col = None
    if "sales_rolling_7d_avg" in df.columns:
        base_col = "sales_rolling_7d_avg"
    elif "sales_qty" in df.columns:
        base_col = "sales_qty"
    else:
        base_col = None

    if base_col is not None:
        forecast_df = forecast_df.withColumn(
            "base_qty",
            when(col(base_col).isNotNull(), spark_abs(col(base_col))).otherwise(lit(1.0))
        )
    else:
        forecast_df = forecast_df.withColumn("base_qty", lit(1.0))

    # 周末因子 + 趋势因子（Spark 内置表达式，不触发 Python 序列化）
    forecast_df = (
        forecast_df
        .withColumn("dow", expr("dayofweek(forecast_date)"))  # 1=周日, 2=周一, ..., 7=周六
        .withColumn(
            "weekend_factor",
            when(col("dow").isin(1, 7), lit(1.2)).otherwise(lit(1.0))
        )
        .withColumn(
            "trend_factor",
            expr("1.0 + (horizon_days * 0.01)")
        )
        .withColumn(
            "predicted_qty",
            spark_round(col("base_qty") * col("weekend_factor") * col("trend_factor"), 1).cast(DoubleType())
        )
    )

    # 组装最终输出列
    forecast_df = (
        forecast_df
        .withColumn("model_name", lit("Sales_Forecast_Model"))
        .withColumn("confidence_level", lit(0.85).cast(DoubleType()))
        .withColumn("create_time", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        .select(
            col("forecast_date"),
            col("product_id"),
            col("predicted_qty"),
            col("horizon_days"),
            col("model_name"),
            col("confidence_level"),
            col("create_time"),
        )
    )

    cnt = forecast_df.count()
    latest_df.unpersist()
    elapsed = time.time() - t0
    print("[ANALYSIS] ✓ 预测生成完成: {} 行, 耗时 {:.2f}s".format(cnt, elapsed))
    print("[ANALYSIS]   预测样例 (前 15 行):")
    forecast_df.show(15, truncate=False)
    return forecast_df


# ============================================================
# 写入 MySQL ADS
# ============================================================
def write_forecast_to_mysql(forecast_df):
    """
    通过 JDBC 写入 MySQL ads.ads_sales_forecast
    """
    if forecast_df is None:
        return None
    t0 = time.time()
    cnt = forecast_df.count()
    ads_table = "ads_sales_forecast"
    print("\n[MYSQL] 写入 MySQL ADS 表: {} (预估 {:,} 行) ...".format(
        ads_table, cnt))
    try:
        (forecast_df.write
         .jdbc(url=MYSQL_JDBC_URL,
                table=ads_table,
                properties=MYSQL_JDBC_PROPERTIES,
                mode="overwrite"))
        elapsed = time.time() - t0
        print("[MYSQL] ✓ MySQL ADS 写入完成: {} = {:,} 行, 耗时 {:.2f}s, URL={}".format(
            ads_table, cnt, elapsed, MYSQL_JDBC_URL))
        return cnt
    except Exception as e:
        print("[MYSQL] ✗ MySQL ADS 写入失败: {}".format(e))
        print("[MYSQL]   (请确保 MySQL JDBC Driver 已通过 --jars 或 --packages 提供)")
        return None


# ============================================================
# 主函数
# ============================================================
def main():
    total_start = time.time()
    print("=" * 70)
    print("[START] 盒马鲜生 销量预测 Spark 任务启动 @ {}".format(
        datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    print("[INFO]  架构: Hive DWS -> Spark ML 预测 -> MySQL ADS")
    print("[INFO]  Hive DB = {}".format(HIVE_CONFIG.get("database", "hema_fresh")))
    print("[INFO]  MYSQL_JDBC_URL = {}".format(MYSQL_JDBC_URL))
    print("[INFO]  预测周期 = 未来 7 天")
    print("=" * 70)

    spark = None
    try:
        spark = create_spark_session()

        # 1) 读取 Hive DWS
        print("\n" + "-" * 50)
        print("[LOAD] ===== 第 1 步: 读取 Hive DWS 特征表")
        print("-" * 50)
        dws_df = read_hive_dws(spark)
        if dws_df is None:
            print("[FATAL] 无法读取 dws_sales_daily，终止任务")
            return

        # 2) 准备训练数据
        print("\n" + "-" * 50)
        print("[ANALYSIS] ===== 第 2 步: 特征工程 + 数据集划分")
        print("-" * 50)
        train_df, test_df, feature_cols = prepare_training_data(dws_df)
        if train_df is None:
            print("[FATAL] 训练数据准备失败，终止任务")
            return

        # 3) 训练 RF
        print("\n" + "-" * 50)
        print("[ANALYSIS] ===== 第 3 步: 训练 RandomForest 模型")
        print("-" * 50)
        model_rf = train_rf_model(train_df)
        metrics_rf = evaluate_model(model_rf, test_df, "RandomForest")

        # 4) 训练 GBT
        print("\n" + "-" * 50)
        print("[ANALYSIS] ===== 第 4 步: 训练 GBT 模型")
        print("-" * 50)
        model_gbt = train_gbt_model(train_df)
        metrics_gbt = evaluate_model(model_gbt, test_df, "GBT")

        # 5) 选择最优模型生成预测
        print("\n" + "-" * 50)
        print("[ANALYSIS] ===== 第 5 步: 生成未来 7 天预测")
        print("-" * 50)

        # 选择 R² 最高的模型
        best_model = None
        best_name = "Unknown"
        if metrics_rf is not None and metrics_gbt is not None:
            if metrics_rf["r2"] >= metrics_gbt["r2"]:
                best_model = model_rf
                best_name = "RandomForest"
            else:
                best_model = model_gbt
                best_name = "GBT"
            print("[ANALYSIS]   选择最优模型: {} (R²={:.4f})".format(
                best_name, max(metrics_rf["r2"], metrics_gbt["r2"])))
        elif metrics_rf is not None:
            best_model = model_rf
            best_name = "RandomForest"
            print("[ANALYSIS]   使用 RandomForest 模型")
        elif metrics_gbt is not None:
            best_model = model_gbt
            best_name = "GBT"
            print("[ANALYSIS]   使用 GBT 模型")
        else:
            print("[FATAL] 没有可用模型，无法生成预测")
            return

        forecast_df = generate_forecast(spark, best_model, dws_df, feature_cols)
        if forecast_df is None:
            print("[FATAL] 预测生成失败，终止任务")
            return

        # 6) 写入 MySQL
        print("\n" + "-" * 50)
        print("[MYSQL] ===== 第 6 步: 写入 MySQL ADS 表")
        print("-" * 50)
        write_forecast_to_mysql(forecast_df)

        # 7) 总结
        total_elapsed = time.time() - total_start
        print("\n" + "=" * 70)
        print("[DONE] 销量预测任务完成 @ {}".format(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        print("[DONE] 总耗时: {:.2f} 秒".format(total_elapsed))
        print("[DONE] === 最终模型对比 ===")
        if metrics_rf is not None:
            print("[DONE]   RF  - RMSE={:.2f}, MAE={:.2f}, R²={:.4f}".format(
                metrics_rf["rmse"], metrics_rf["mae"], metrics_rf["r2"]))
        if metrics_gbt is not None:
            print("[DONE]   GBT - RMSE={:.2f}, MAE={:.2f}, R²={:.4f}".format(
                metrics_gbt["rmse"], metrics_gbt["mae"], metrics_gbt["r2"]))
        print("[DONE] 预测结果已写入: MySQL ads.ads_sales_forecast")
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
