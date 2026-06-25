"""
Prefect Flow — Retrain otomatis tiap 24 jam.
1. Bulk read dari Cassandra via spark-cassandra connector
2. Feature engineering
3. Train Random Forest
4. Save model + upload ke MinIO
5. Champion/challenger: bandingkan MAE dengan BEST
"""
import os
import sys
import shutil

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from prefect import flow, task

import config
from features import compute_features, FEATURE_COLS
from minio_utils import (
    get_client, ensure_bucket, version_tag,
    upload_model, upload_features, upload_metrics,
    write_tag, read_metrics
)

MODEL_PATH = os.path.join(os.path.dirname(__file__), 'spark_rf_model')
FEATURES_PATH = os.path.join(os.path.dirname(__file__), 'latest_features.json')


@task(log_prints=True)
def read_cassandra(spark):
    print("Bulk reading earthquake_history from Cassandra...")
    df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "earthquake_db") \
        .option("table", "earthquake_history") \
        .load()
    count = df.count()
    print(f"  {count} rows read.")
    return df


@task(log_prints=True)
def feature_engineering(df_spark):
    print("Feature engineering...")
    df_pd = df_spark.toPandas()
    df_pd = df_pd.rename(columns={
        'time': 'waktu',
        'magnitude': 'magnitudo',
        'depth': 'kedalaman',
    })
    df_features = compute_features(df_pd, compute_target=True, max_hari_susulan=3.0)
    print(f"  {len(df_features)} feature rows, {df_features['grid_id'].nunique()} grids.")
    return df_features


@task(log_prints=True)
def train_model(spark, df_features):
    from pyspark.ml.regression import RandomForestRegressor
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.evaluation import RegressionEvaluator

    df_features[FEATURE_COLS] = df_features[FEATURE_COLS].fillna(0.0)
    cols_to_keep = FEATURE_COLS + ['days_until_next_earthquake']
    df_clean = df_features[cols_to_keep].copy()

    df_spark = spark.createDataFrame(df_clean)
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
    df_assembled = assembler.transform(df_spark)

    train_data, test_data = df_assembled.randomSplit([0.8, 0.2], seed=42)

    rf = RandomForestRegressor(
        featuresCol="features", labelCol="days_until_next_earthquake",
        numTrees=100, maxDepth=8, maxBins=32, seed=42
    )
    rf_model = rf.fit(train_data)

    predictions = rf_model.transform(test_data)
    evaluator = RegressionEvaluator(labelCol="days_until_next_earthquake", predictionCol="prediction")
    rmse = evaluator.setMetricName("rmse").evaluate(predictions)
    r2 = evaluator.setMetricName("r2").evaluate(predictions)
    mae = evaluator.setMetricName("mae").evaluate(predictions)
    print(f"  Metrics — MAE: {mae:.2f}  RMSE: {rmse:.2f}  R²: {r2:.2f}")

    if os.path.exists(MODEL_PATH):
        shutil.rmtree(MODEL_PATH)
    rf_model.write().overwrite().save(MODEL_PATH)
    print(f"  Model saved to {MODEL_PATH}")

    return rf_model, {"mae": round(mae, 4), "rmse": round(rmse, 4), "r2": round(r2, 4)}


@task(log_prints=True)
def save_latest_features(df_features):
    latest = df_features.groupby('grid_id').tail(1).copy()
    latest.to_json(FEATURES_PATH, orient='records')
    print(f"  {FEATURES_PATH} updated ({len(latest)} grids).")


@task(log_prints=True)
def upload_to_minio(metrics: dict, version: str):
    client = get_client()
    ensure_bucket(client)
    upload_model(client, MODEL_PATH, version)
    upload_features(client, FEATURES_PATH, version)
    upload_metrics(client, metrics, version)
    write_tag(client, "LATEST", version)
    print(f"  LATEST = {version}")
    return client


@task(log_prints=True)
def champion_challenger(client, metrics: dict, version: str):
    try:
        best_tag = client.get_object(config.MINIO_BUCKET, "BEST")
        best_version = best_tag.read().decode().strip()
        best_tag.close()

        best_metrics = read_metrics(client, best_version)
        if best_metrics and metrics["mae"] < best_metrics["mae"]:
            write_tag(client, "BEST", version)
            print(f"  ✅ Better MAE ({metrics['mae']:.4f} < {best_metrics['mae']:.4f}) → BEST = {version}")
        else:
            print(f"  ❌ Worse/equal MAE ({metrics['mae']:.4f} >= {best_metrics.get('mae', float('inf')):.4f}) → keeping BEST = {best_version}")
    except:
        write_tag(client, "BEST", version)
        print(f"  First model → BEST = {version}")


@flow(log_prints=True)
def retrain_flow():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("EarthquakeRetrain") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.cassandra.connection.host", config.CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", config.CASSANDRA_PORT) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df_raw = read_cassandra(spark)
    df_features = feature_engineering(df_raw)
    rf_model, metrics = train_model(spark, df_features)
    save_latest_features(df_features)

    version = version_tag()
    client = upload_to_minio(metrics, version)
    champion_challenger(client, metrics, version)

    spark.stop()
    print("Retrain complete.")


if __name__ == "__main__":
    retrain_flow()
    retrain_flow.serve(
        name="daily_retrain",
        cron="0 3 * * *"
    )
