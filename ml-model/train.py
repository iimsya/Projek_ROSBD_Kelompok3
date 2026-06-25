import os
import shutil
import pandas as pd
import sys

from pyspark.sql import SparkSession
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator

from features import compute_features, FEATURE_COLS

DATA_PATH = os.path.join(os.path.dirname(__file__), "../dataset_gempa_bigdata.csv")
MODEL_PATH = os.path.join(os.path.dirname(__file__), "spark_rf_model")
LATEST_FEATURES_PATH = os.path.join(os.path.dirname(__file__), "latest_features.json")

def create_features_and_target(df):
    print("Preprocessing data and engineering features (Pandas)...")
    df_final = compute_features(df, compute_target=True, max_hari_susulan=3.0)

    latest_features = df_final.groupby('grid_id').tail(1).copy()
    latest_features.to_json(LATEST_FEATURES_PATH, orient='records')

    cols = ['grid_id', 'latitude', 'longitude', 'kedalaman', 'magnitudo',
            'energy_accum_7d', 'energy_accum_30d', 'energy_accum_90d', 'small_eq_freq_30d',
            'days_until_next_earthquake', 'potensi_tsunami', 'peringatan', 'signifikansi', 'mmi']
    cols = [c for c in cols if c in df_final.columns]
    return df_final[cols].copy()

def train_and_evaluate():
    if not os.path.exists(DATA_PATH):
        print(f"Dataset not found at {DATA_PATH}. Please run historis_ingestion.py first.")
        return
        
    df_raw = pd.read_csv(DATA_PATH)
    for col in ['potensi_tsunami', 'peringatan', 'signifikansi', 'mmi']:
        if col not in df_raw.columns:
            df_raw[col] = 0.0

    df_pd = create_features_and_target(df_raw)
    df_pd[FEATURE_COLS] = df_pd[FEATURE_COLS].fillna(0.0)
    cols_to_keep = FEATURE_COLS + ['days_until_next_earthquake']
    df_pd_clean = df_pd[cols_to_keep].copy()

    # --- SPARK ML ---
    os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    spark = SparkSession.builder \
        .appName("EarthquakeMLTraining") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    print("\n--- Converting Pandas DF to Spark DF ---")
    df_spark = spark.createDataFrame(df_pd_clean)

    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
    df_assembled = assembler.transform(df_spark)

    train_data, test_data = df_assembled.randomSplit([0.8, 0.2], seed=42)

    print("\n--- Training Spark ML Random Forest ---")
    rf = RandomForestRegressor(featuresCol="features", labelCol="days_until_next_earthquake", numTrees=100, maxDepth=8, maxBins=32, seed=42)
    rf_model = rf.fit(train_data)

    predictions = rf_model.transform(test_data)
    
    evaluator_rmse = RegressionEvaluator(labelCol="days_until_next_earthquake", predictionCol="prediction", metricName="rmse")
    evaluator_r2 = RegressionEvaluator(labelCol="days_until_next_earthquake", predictionCol="prediction", metricName="r2")
    evaluator_mae = RegressionEvaluator(labelCol="days_until_next_earthquake", predictionCol="prediction", metricName="mae")

    rmse = evaluator_rmse.evaluate(predictions)
    r2 = evaluator_r2.evaluate(predictions)
    mae = evaluator_mae.evaluate(predictions)

    print(f"Spark Random Forest | MAE: {mae:.2f} | RMSE: {rmse:.2f} | R²: {r2:.2f}")

    print("\n--- Saving Spark Model ---")
    if os.path.exists(MODEL_PATH):
        shutil.rmtree(MODEL_PATH)
    
    rf_model.write().overwrite().save(MODEL_PATH)
    print(f"Model saved to {MODEL_PATH}")
    print(f"Latest features saved to {LATEST_FEATURES_PATH}")
    spark.stop()

if __name__ == "__main__":
    train_and_evaluate()
