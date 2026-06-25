"""
Prefect Flow — Retrain otomatis tiap 24 jam.
1. Bulk read dari Cassandra via spark-cassandra connector
2. Feature engineering
3. Train Random Forest
4. Save model + update latest_features.json
5. Predict all grids → overwrite latest_events
"""
import os
import sys
import shutil
import subprocess

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from prefect import flow, task

import config
from features import compute_features, FEATURE_COLS

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

    return rf_model


@task(log_prints=True)
def save_latest_features(df_features):
    latest = df_features.groupby('grid_id').tail(1).copy()
    latest.to_json(FEATURES_PATH, orient='records')
    print(f"  {FEATURES_PATH} updated ({len(latest)} grids).")


@task(log_prints=True)
def predict_all(spark, rf_model, df_features):
    from pyspark.ml.feature import VectorAssembler

    latest = df_features.groupby('grid_id').tail(1).reset_index(drop=True)
    latest[FEATURE_COLS] = latest[FEATURE_COLS].fillna(0.0)

    df_spark = spark.createDataFrame(latest)
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
    df_assembled = assembler.transform(df_spark)
    df_pred = rf_model.transform(df_assembled)

    df_pred = df_pred.select(
        "grid_id", "time", "id", "place", "magnitude", "longitude", "latitude",
        "depth", "signifikansi", "energy", "is_small_eq",
        "prediction", "prediction"
    ).withColumnRenamed("prediction", "prediction_days") \
     .withColumnRenamed("time", "event_time")

    df_pred.createOrReplaceTempView("preds")
    df_final = spark.sql("""
        SELECT grid_id, id, event_time as time, place, magnitude, longitude, latitude,
               depth, signifikansi, energy, is_small_eq,
               ROUND(prediction_days, 2) as prediction_days,
               CASE WHEN prediction_days <= 1.0 THEN 'HIGH'
                    WHEN prediction_days <= 3.0 THEN 'MEDIUM'
                    ELSE 'LOW' END as status
        FROM preds
    """)

    print("Overwriting latest_events in Cassandra...")
    df_final.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "earthquake_db") \
        .option("table", "latest_events") \
        .mode("overwrite") \
        .save()
    print("  latest_events updated.")


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
    rf_model = train_model(spark, df_features)
    save_latest_features(df_features)
    predict_all(spark, rf_model, df_features)

    spark.stop()
    print("Retrain complete.")


if __name__ == "__main__":
    retrain_flow.serve(
        name="daily_retrain",
        cron="0 3 * * *"
    )
