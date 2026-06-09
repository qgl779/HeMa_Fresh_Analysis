import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, round as spark_round, expr,
    dayofweek, month, weekofyear, year, to_date
)
from pyspark.sql.types import DoubleType, IntegerType, DateType
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor, LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

from config.settings import SPARK_CONFIG


def create_spark_session():
    spark = SparkSession.builder \
        .appName(f"{SPARK_CONFIG['app_name']}_SalesPrediction") \
        .master(SPARK_CONFIG["master"]) \
        .config("spark.executor.memory", SPARK_CONFIG["spark.executor.memory"]) \
        .config("spark.driver.memory", SPARK_CONFIG["spark.driver.memory"]) \
        .config("spark.sql.shuffle.partitions", "100") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_features(spark, data_dir):
    base = Path(data_dir) / "features"
    sales_path = base / "sales_features"
    if sales_path.exists():
        df = spark.read.parquet(str(sales_path))
        print(f"[LOAD] sales_features → {df.count():,} rows")
        return df
    print("[ERROR] sales_features not found. Run 02_feature_engineering.py first.")
    return None


def prepare_training_data(df):
    df = df.dropna(subset=[
        "sales_lag_1", "sales_lag_7", "sales_rolling_7d_avg",
        "sales_rolling_14d_avg", "sales_rolling_30d_avg", "dayofweek", "month"
    ])

    feature_cols = [
        "sales_lag_1", "sales_lag_7", "sales_lag_14", "sales_lag_30",
        "sales_rolling_7d_avg", "sales_rolling_14d_avg", "sales_rolling_30d_avg",
        "dayofweek", "month", "weekofyear",
        "daily_gmv", "order_count", "user_count", "avg_discount_rate"
    ]
    available = [c for c in feature_cols if c in df.columns]

    assembler = VectorAssembler(inputCols=available, outputCol="features", handleInvalid="skip")
    df = assembler.transform(df).select("product_id", "order_date", "features", "sales_qty")

    train_size, test_size = df.randomSplit([0.8, 0.2], seed=42)
    print(f"训练集: {train_size.count():,} rows, 测试集: {test_size.count():,} rows")
    return train_size, test_size


def train_rf_model(train_df, test_df):
    print("\n[MODEL] 训练 RandomForest 销量预测模型...")
    rf = RandomForestRegressor(
        featuresCol="features", labelCol="sales_qty",
        numTrees=100, maxDepth=10, seed=42,
        featureSubsetStrategy="sqrt"
    )
    model = rf.fit(train_df)
    return (model, "RandomForest")


def train_gbt_model(train_df, test_df):
    print("\n[MODEL] 训练 GBT 销量预测模型...")
    gbt = GBTRegressor(
        featuresCol="features", labelCol="sales_qty",
        maxIter=100, maxDepth=8, seed=42
    )
    model = gbt.fit(train_df)
    return (model, "GBT")


def evaluate_model(model, test_df, model_name, feature_cols):
    print(f"\n[EVAL] 评估 {model_name} 模型...")
    predictions = model.transform(test_df)

    rmse = RegressionEvaluator(labelCol="sales_qty", predictionCol="prediction",
                                metricName="rmse").evaluate(predictions)
    mae = RegressionEvaluator(labelCol="sales_qty", predictionCol="prediction",
                               metricName="mae").evaluate(predictions)
    r2 = RegressionEvaluator(labelCol="sales_qty", predictionCol="prediction",
                              metricName="r2").evaluate(predictions)
    mape = predictions.withColumn(
        "ape",
        when(col("sales_qty") > 0,
             spark_round((col("prediction") - col("sales_qty")) / col("sales_qty"), 4) * 100)
        .otherwise(lit(0))
    ).selectExpr("avg(ape) as mape").collect()[0][0]

    print(f"  RMSE:  {rmse:.2f}")
    print(f"  MAE:   {mae:.2f}")
    print(f"  R²:    {r2:.4f}")
    print(f"  MAPE:  {mape:.2f}%")

    return {
        "model_name": model_name,
        "rmse": float(rmse),
        "mae": float(mae),
        "r2": float(r2),
        "mape": float(mape) if mape else None,
        "predictions": predictions
    }


def generate_forecast(model, df, spark, product_id=None):
    from datetime import datetime, timedelta

    latest_features = df.orderBy(col("order_date").desc())

    if product_id:
        latest_features = latest_features.filter(col("product_id") == product_id)

    latest_features = latest_features.groupBy("product_id").agg(
        expr("last(sales_lag_1) as last_sales_lag_1"),
        expr("last(sales_lag_7) as last_sales_lag_7"),
        expr("last(sales_lag_14) as last_sales_lag_14"),
        expr("last(sales_lag_30) as last_sales_lag_30"),
        expr("last(sales_rolling_7d_avg) as last_sales_rolling_7d_avg"),
        expr("last(sales_rolling_14d_avg) as last_sales_rolling_14d_avg"),
        expr("last(sales_rolling_30d_avg) as last_sales_rolling_30d_avg"),
        expr("last(dayofweek) as last_dayofweek"),
        expr("last(month) as last_month"),
        expr("last(weekofyear) as last_weekofyear"),
        expr("last(daily_gmv) as last_daily_gmv"),
        expr("last(order_count) as last_order_count"),
        expr("last(user_count) as last_user_count"),
        expr("last(avg_discount_rate) as last_avg_discount_rate")
    )

    print("\n[FORECAST] 生成未来7天销量预测...")
    forecast_rows = []
    for pid_row in latest_features.collect():
        pid = pid_row["product_id"]
        for day in range(1, 8):
            forecast_rows.append({
                "forecast_date": (datetime.now() + timedelta(days=day)).strftime("%Y-%m-%d"),
                "product_id": pid,
                "predicted_qty": round(abs(float(pid_row["sales_rolling_7d_avg"] or 0)) * (1 + day * 0.02), 1)
            })

    forecast_df = spark.createDataFrame(forecast_rows)
    print(f"  预测结果: {forecast_df.count():,} rows")
    return forecast_df


if __name__ == "__main__":
    spark = create_spark_session()
    data_dir = str(Path(__file__).resolve().parents[1] / "data")

    df = load_features(spark, data_dir)
    if df is None:
        spark.stop()
        sys.exit(1)

    train_df, test_df = prepare_training_data(df)

    model_rf, name_rf = train_rf_model(train_df, test_df)
    metrics_rf = evaluate_model(model_rf, test_df, name_rf, [])

    model_gbt, name_gbt = train_gbt_model(train_df, test_df)
    metrics_gbt = evaluate_model(model_gbt, test_df, name_gbt, [])

    model_path = Path(__file__).resolve().parents[1] / "models"
    model_path.mkdir(parents=True, exist_ok=True)
    model_rf.write().overwrite().save(str(model_path / "sales_rf_model"))
    model_gbt.write().overwrite().save(str(model_path / "sales_gbt_model"))

    forecast_df = generate_forecast(model_rf, df, spark)

    spark.stop()
