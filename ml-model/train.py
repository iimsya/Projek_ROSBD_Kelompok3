import os
import glob
import shutil
import pandas as pd
import numpy as np
import sys

# Konfigurasi otomatis Environment Variables untuk Java 17 dan Hadoop Winutils
java_dirs = glob.glob(r"C:\Program Files\Microsoft\jdk-17*")
if java_dirs:
    os.environ["JAVA_HOME"] = java_dirs[0]

os.environ["HADOOP_HOME"] = r"D:\hadoop"
os.environ["PATH"] = os.environ["HADOOP_HOME"] + r"\bin;" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator

DATA_PATH = os.path.join(os.path.dirname(__file__), "../dataset_gempa_bigdata.csv")
MODEL_PATH = os.path.join(os.path.dirname(__file__), "spark_rf_model")
LATEST_FEATURES_PATH = os.path.join(os.path.dirname(__file__), "latest_features.json")

def filter_dense_grids(df: pd.DataFrame, min_events: int = 10, window_days: int = 365) -> pd.DataFrame:
    valid_grids = []
    for grid_id, group in df.groupby('grid_id'):
        times = group['waktu'].sort_values().values
        if len(times) < min_events:
            continue
        start = 0
        for end in range(len(times)):
            while times[end] - times[start] > np.timedelta64(window_days, 'D'):
                start += 1
            if (end - start + 1) >= min_events:
                valid_grids.append(grid_id)
                break
    return df[df['grid_id'].isin(valid_grids)].copy()

def create_features_and_target(df):
    print("Preprocessing data and engineering features (Pandas)...")
    if 'tipe' in df.columns:
        df = df[df['tipe'] == 'earthquake'].copy()
        
    df['waktu'] = pd.to_datetime(df['waktu'], errors='coerce')
    df = df.dropna(subset=['waktu', 'latitude', 'longitude', 'kedalaman', 'magnitudo']).reset_index(drop=True)
    
    df['grid_lat'] = np.floor(df['latitude'] / 1.0) * 1.0
    df['grid_lon'] = np.floor(df['longitude'] / 1.0) * 1.0
    df['grid_id'] = df['grid_lat'].round(2).astype(str) + "_" + df['grid_lon'].round(2).astype(str)
    
    df = filter_dense_grids(df, min_events=10, window_days=365)
    
    df['energy'] = 10 ** (4.8 + 1.5 * df['magnitudo'])
    df['is_small_eq'] = (df['magnitudo'] < 4.5).astype(float)
    
    df = df.sort_values(by=['grid_id', 'waktu']).reset_index(drop=True)
    df = df.set_index('waktu')
    
    features_list = []
    sig_mag_threshold = 4.0
    
    for grid, group in df.groupby('grid_id'):
        group = group.sort_index()
        group['energy_accum_7d'] = group['energy'].rolling('7D', closed='left').sum()
        group['energy_accum_30d'] = group['energy'].rolling('30D', closed='left').sum()
        group['energy_accum_90d'] = group['energy'].rolling('90D', closed='left').sum()
        group['small_eq_freq_30d'] = group['is_small_eq'].rolling('30D', closed='left').sum()
        group = group.reset_index()
        
        sig_times = group.loc[group['magnitudo'] >= sig_mag_threshold, 'waktu'].values
        if len(sig_times) == 0:
            group['days_until_next_earthquake'] = np.nan
        else:
            current_times = group['waktu'].values
            next_indices = np.searchsorted(sig_times, current_times, side='right')
            next_sig_times = [sig_times[idx] if idx < len(sig_times) else np.datetime64('NaT') for idx in next_indices]
            group['days_until_next_earthquake'] = (np.array(next_sig_times) - current_times) / np.timedelta64(1, 'D')
            
        features_list.append(group)
        
    df_final = pd.concat(features_list).reset_index(drop=True)
    
    latest_features = df_final.groupby('grid_id').tail(1).copy()
    latest_features.to_json(LATEST_FEATURES_PATH, orient='records')
    
    MAX_HARI_SUSULAN = 3.0
    df_final = df_final[df_final['days_until_next_earthquake'] <= MAX_HARI_SUSULAN].copy()
    df_final = df_final.dropna(subset=['days_until_next_earthquake'])
    
    cols = ['grid_id', 'latitude', 'longitude', 'kedalaman', 'magnitudo', 
            'energy_accum_7d', 'energy_accum_30d', 'energy_accum_90d', 'small_eq_freq_30d', 
            'days_until_next_earthquake', 'potensi_tsunami', 'peringatan', 'signifikansi', 'mmi']
    
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
    feature_cols = ['latitude', 'longitude', 'kedalaman', 'magnitudo', 
                    'energy_accum_7d', 'energy_accum_30d', 'energy_accum_90d', 
                    'small_eq_freq_30d', 'signifikansi']
    
    df_pd[feature_cols] = df_pd[feature_cols].fillna(0.0)
    
    # We only need features and target for Spark ML
    cols_to_keep = feature_cols + ['days_until_next_earthquake']
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

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
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
