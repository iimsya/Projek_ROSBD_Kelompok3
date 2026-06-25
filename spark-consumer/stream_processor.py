import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ml-model"))

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, expr, round, concat, lit, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark.ml.feature import VectorAssembler
import pandas as pd

import config

KAFKA_BROKER = config.KAFKA_BROKER
KAFKA_TOPIC = "earthquake_stream"
CASSANDRA_HOST = config.CASSANDRA_HOST
CASSANDRA_PORT = str(config.CASSANDRA_PORT)

MODEL_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../ml-model/spark_rf_model"))
FEATURES_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../ml-model/latest_features.json"))
CHECKPOINT_HISTORY = "checkpoint_dir_history"
CHECKPOINT_PRED = "checkpoint_dir_cassandra"
WATCH_INTERVAL = 300

schema = StructType([
    StructField("id", StringType(), True),
    StructField("time", TimestampType(), True),
    StructField("magnitude", DoubleType(), True),
    StructField("place", StringType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("depth", DoubleType(), True),
    StructField("type", StringType(), True),
    StructField("potensi_tsunami", StringType(), True),
    StructField("peringatan", StringType(), True),
    StructField("signifikansi", DoubleType(), True),
    StructField("mmi", DoubleType(), True)
])

from cassandra.cluster import Cluster


def init_cassandra():
    print("Inisialisasi Cassandra Keyspace dan Table...")
    try:
        cluster = Cluster([CASSANDRA_HOST], port=int(CASSANDRA_PORT))
        session = cluster.connect()
        session.execute("""
        CREATE KEYSPACE IF NOT EXISTS earthquake_db
        WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
        """)
        session.execute("""
        CREATE TABLE IF NOT EXISTS earthquake_db.earthquake_history (
            grid_id text,
            time timestamp,
            id text,
            place text,
            magnitude double,
            longitude double,
            latitude double,
            depth double,
            signifikansi double,
            energy double,
            is_small_eq int,
            PRIMARY KEY ((grid_id), time, id)
        ) WITH CLUSTERING ORDER BY (time DESC, id ASC)
        """)
        session.execute("""
        CREATE TABLE IF NOT EXISTS earthquake_db.latest_events (
            grid_id text PRIMARY KEY,
            id text,
            time timestamp,
            place text,
            magnitude double,
            longitude double,
            latitude double,
            depth double,
            signifikansi double,
            energy double,
            is_small_eq int,
            prediction_days double,
            status text
        )
        """)
        session.shutdown()
        cluster.shutdown()
        print("Cassandra siap!")
    except Exception as e:
        print(f"Gagal inisialisasi Cassandra: {e}")


def create_spark_session():
    os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2,com.datastax.spark:spark-cassandra-connector_2.13:3.5.0 pyspark-shell'

    spark = SparkSession.builder \
        .appName("EarthquakeStreamProcessor") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.streaming.checkpointLocation", "checkpoint_dir") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_best_version_from_minio(client=None):
    try:
        if client is None:
            from minio_utils import get_client
            client = get_client()
        best_tag = client.get_object(config.MINIO_BUCKET, "BEST")
        version = best_tag.read().decode().strip()
        best_tag.close()
        return version
    except:
        return None


def download_best_model(client, version: str):
    from minio_utils import download_model, download_features
    download_model(client, version, MODEL_PATH)
    download_features(client, version, FEATURES_PATH)


def load_model_and_features(spark, model_path, features_path):
    print(f"  Loading model from {model_path}...")
    rf_model = RandomForestRegressionModel.load(model_path)
    print(f"  Loading features from {features_path}...")
    df_pd = pd.read_json(features_path)
    df = spark.createDataFrame(df_pd)
    df_history = df.select(
        "grid_id",
        "energy_accum_7d", "energy_accum_30d", "energy_accum_90d", "small_eq_freq_30d"
    )
    return rf_model, df_history


def start_queries(spark, rf_model, df_history):
    feature_cols = ['latitude', 'longitude', 'kedalaman', 'magnitudo',
                    'energy_accum_7d', 'energy_accum_30d', 'energy_accum_90d',
                    'small_eq_freq_30d', 'signifikansi']

    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")

    df_cleaned = df_parsed \
        .fillna({"mmi": 0.0, "peringatan": "none", "potensi_tsunami": "0", "signifikansi": 0.0}) \
        .filter(col("magnitude").isNotNull() & col("latitude").isNotNull() & col("longitude").isNotNull())

    df_realtime = df_cleaned \
        .withColumn("grid_lat", round(col("latitude"), 0)) \
        .withColumn("grid_lon", round(col("longitude"), 0)) \
        .withColumn("grid_id", concat(col("grid_lon"), lit("_"), col("grid_lat"))) \
        .withColumn("energy", expr("pow(10, 4.8 + 1.5 * magnitude)")) \
        .withColumn("is_small_eq", expr("CASE WHEN magnitude < 4.5 THEN 1 ELSE 0 END"))

    df_joined = df_realtime.join(df_history, "grid_id", "left") \
        .fillna({
            "energy_accum_7d": 0.0,
            "energy_accum_30d": 0.0,
            "energy_accum_90d": 0.0,
            "small_eq_freq_30d": 0.0
        })

    df_for_ml = df_joined \
        .withColumn("kedalaman", col("depth")) \
        .withColumn("magnitudo", col("magnitude"))

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df_assembled = assembler.transform(df_for_ml)
    df_pred = rf_model.transform(df_assembled)

    df_final = df_pred \
        .withColumn("prediction_days", round(col("prediction"), 2)) \
        .withColumn("status",
            when(col("prediction") <= 1.0, "HIGH")
            .when(col("prediction") <= 3.0, "MEDIUM")
            .otherwise("LOW")
        )

    q_history = df_realtime.select(
        "grid_id", "id", "time", "place", "magnitude", "longitude",
        "latitude", "depth", "signifikansi", "energy", "is_small_eq"
    ).writeStream \
        .outputMode("append") \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "earthquake_db") \
        .option("table", "earthquake_history") \
        .option("checkpointLocation", CHECKPOINT_HISTORY) \
        .start()

    q_pred = df_final.select(
        "grid_id", "id", "time", "place", "magnitude", "longitude",
        "latitude", "depth", "signifikansi", "energy", "is_small_eq",
        "prediction_days", "status"
    ).writeStream \
        .outputMode("append") \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "earthquake_db") \
        .option("table", "latest_events") \
        .option("checkpointLocation", CHECKPOINT_PRED) \
        .start()

    return q_history, q_pred


def catchup_from_usgs(spark, rf_model, df_history):
    """Fetch USGS events from last history timestamp → now, predict, insert."""
    from datetime import datetime, timezone, timedelta
    import requests
    import numpy as np
    from cassandra.cluster import Cluster

    cluster_cql = Cluster([CASSANDRA_HOST], port=int(CASSANDRA_PORT))
    session_cql = cluster_cql.connect()
    row = session_cql.execute("SELECT MAX(time) as max_ts FROM earthquake_db.earthquake_history").one()
    last_ts = row.max_ts if row and row.max_ts else None
    session_cql.shutdown()
    cluster_cql.shutdown()

    if last_ts:
        if last_ts.tzinfo is None:
            last_ts = last_ts.replace(tzinfo=timezone.utc)
        print(f"Last event in history: {last_ts}")

    now = datetime.now(timezone.utc)
    if last_ts and (now - last_ts).total_seconds() < 3600:
        print("  Data up to date (<1h), skipping USGS catchup.")
        return

    start_str = (last_ts if last_ts else now - timedelta(days=7)).strftime('%Y-%m-%dT%H:%M:%S')
    end_str = now.strftime('%Y-%m-%dT%H:%M:%S')
    print(f"Catching up from USGS: {start_str} → {end_str}")

    params = {"format": "geojson", "starttime": start_str, "endtime": end_str, "minmagnitude": "0.0"}
    try:
        resp = requests.get("https://earthquake.usgs.gov/fdsnws/event/1/query", params=params, timeout=30)
        if resp.status_code != 200:
            print(f"  USGS API error: {resp.status_code}")
            return
        features = resp.json().get('features', [])
        print(f"  Got {len(features)} events from USGS.")
    except Exception as e:
        print(f"  USGS fetch failed: {e}")
        return

    if not features:
        print("  No new events.")
        return

    rows = []
    for f in features:
        prop = f['properties']
        geom = f['geometry']['coordinates']
        ts = datetime.fromtimestamp(prop['time'] / 1000.0, tz=timezone.utc)
        lat, lon = geom[1], geom[0]
        grid_lat = np.floor(lat / 1.0) * 1.0
        grid_lon = np.floor(lon / 1.0) * 1.0
        grid_id = f"{grid_lat:.2f}_{grid_lon:.2f}"
        rows.append({
            'grid_id': grid_id,
            'id': f['id'],
            'waktu': ts,
            'place': prop.get('place', ''),
            'magnitudo': prop['mag'],
            'longitude': lon,
            'latitude': lat,
            'kedalaman': geom[2],
            'signifikansi': prop.get('sig', 0),
            'energy': 10 ** (4.8 + 1.5 * prop['mag']),
            'is_small_eq': 1 if prop['mag'] < 4.5 else 0,
        })

    df_catchup = spark.createDataFrame(pd.DataFrame(rows))
    df_joined = df_catchup.join(df_history, "grid_id", "left").fillna(0.0)

    assembler = VectorAssembler(
        inputCols=['latitude', 'longitude', 'kedalaman', 'magnitudo',
                   'energy_accum_7d', 'energy_accum_30d', 'energy_accum_90d',
                   'small_eq_freq_30d', 'signifikansi'],
        outputCol="features"
    )
    df_assembled = assembler.transform(df_joined)
    df_pred = rf_model.transform(df_assembled)
    predictions = df_pred.collect()
    print(f"  Predicted {len(predictions)} catchup events.")

    cluster_cql = Cluster([CASSANDRA_HOST], port=int(CASSANDRA_PORT))
    session_cql = cluster_cql.connect()
    insert_history = session_cql.prepare("""
    INSERT INTO earthquake_db.earthquake_history
        (grid_id, time, id, place, magnitude, longitude, latitude, depth,
         signifikansi, energy, is_small_eq)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)
    insert_pred = session_cql.prepare("""
    INSERT INTO earthquake_db.latest_events
        (grid_id, id, time, place, magnitude, longitude, latitude, depth,
         signifikansi, energy, is_small_eq, prediction_days, status)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    inserted = 0
    for row in predictions:
        pred_days = float(f"{row.prediction:.2f}")
        if pred_days <= 1.0:
            status = "HIGH"
        elif pred_days <= 3.0:
            status = "MEDIUM"
        else:
            status = "LOW"
        try:
            session_cql.execute(insert_history, (
                row.grid_id, row.waktu, row.id, row.place,
                float(row.magnitudo), float(row.longitude), float(row.latitude),
                float(row.kedalaman), float(row.signifikansi),
                float(row.energy), int(row.is_small_eq)
            ))
            session_cql.execute(insert_pred, (
                row.grid_id, row.id, row.waktu, row.place,
                float(row.magnitudo), float(row.longitude), float(row.latitude),
                float(row.kedalaman), float(row.signifikansi),
                float(row.energy), int(row.is_small_eq),
                pred_days, status
            ))
            inserted += 1
        except Exception as e:
            print(f"  [ERROR] Insert failed for {row.grid_id}: {e}")

    session_cql.shutdown()
    cluster_cql.shutdown()
    print(f"  Inserted {inserted} catchup events to earthquake_history + latest_events.")


def process_stream():
    init_cassandra()
    spark = create_spark_session()

    client = None
    try:
        from minio_utils import get_client
        client = get_client()
    except Exception as e:
        print(f"MinIO not available, using local model: {e}")

    # Initial load — coba dari MinIO BEST, fallback ke local
    best_version = get_best_version_from_minio(client) if client else None
    if best_version and client:
        print(f"Downloading BEST model from MinIO: {best_version}")
        download_best_model(client, best_version)
    else:
        print("Using local model files")

    rf_model, df_history = load_model_and_features(spark, MODEL_PATH, FEATURES_PATH)

    # Catchup: ambil USGS dari last_ts → now, predict, insert ke Cassandra
    catchup_from_usgs(spark, rf_model, df_history)

    q1, q2 = start_queries(spark, rf_model, df_history)
    print(f"Started Spark Streaming: raw → earthquake_history, predictions → latest_events")

    last_best = best_version or "local"
    while True:
        time.sleep(WATCH_INTERVAL)
        if not client:
            continue

        current_best = get_best_version_from_minio(client)
        if current_best and current_best != last_best:
            print(f"New BEST model detected: {current_best} (was {last_best})")
            print("Stopping queries...")
            q1.stop()
            q2.stop()
            q1.awaitTermination()
            q2.awaitTermination()
            print("Queries stopped.")

            download_best_model(client, current_best)
            rf_model, df_history = load_model_and_features(spark, MODEL_PATH, FEATURES_PATH)
            q1, q2 = start_queries(spark, rf_model, df_history)
            last_best = current_best
            print(f"Queries restarted with BEST = {current_best}")


if __name__ == "__main__":
    process_stream()
