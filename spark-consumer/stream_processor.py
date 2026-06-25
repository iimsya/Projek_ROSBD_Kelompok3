import os
import glob
import sys
import platform

# Konfigurasi otomatis Environment Variables untuk Java 17 dan Hadoop Winutils (Hanya di Windows)
if platform.system() == "Windows":
    java_dirs = glob.glob(r"C:\Program Files\Microsoft\jdk-17*")
    if java_dirs:
        os.environ["JAVA_HOME"] = java_dirs[0]

    os.environ["HADOOP_HOME"] = r"D:\hadoop"
    os.environ["PATH"] = os.environ["HADOOP_HOME"] + r"\bin;" + os.environ.get("PATH", "")

    # Untuk Spark di Windows
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

# Configuration (Supports Environment Variables)
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "earthquake_stream")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT", "9042")

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
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def process_stream():
    init_cassandra()
    
    spark = create_spark_session()
    print("Spark Session created successfully.")

    # 1. Load ML Model & Historical Features
    model_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../ml-model/spark_rf_model"))
    features_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../ml-model/latest_features.json"))
    
    print("Loading PySpark ML Model...")
    rf_model = RandomForestRegressionModel.load(model_path)
    
    print("Loading Historical Features...")
    df_static_pd = pd.read_json(features_path)
    df_static_spark = spark.createDataFrame(df_static_pd)
    
    # We only need the historical aggregation columns
    df_history = df_static_spark.select(
        "grid_id", 
        "energy_accum_7d", "energy_accum_30d", "energy_accum_90d", "small_eq_freq_30d"
    )

    # 2. Read from Kafka
    df_kafka = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")

    # 3. Clean Data & Handle Missing Values
    df_cleaned = df_parsed \
        .fillna({"mmi": 0.0, "peringatan": "none", "potensi_tsunami": "0", "signifikansi": 0.0}) \
        .filter(col("magnitude").isNotNull() & col("latitude").isNotNull() & col("longitude").isNotNull())

    # 4. Feature Engineering
    df_realtime = df_cleaned \
        .withColumn("grid_lat", round(col("latitude"), 0)) \
        .withColumn("grid_lon", round(col("longitude"), 0)) \
        .withColumn("grid_id", concat(col("grid_lon"), lit("_"), col("grid_lat"))) \
        .withColumn("energy", expr("pow(10, 4.8 + 1.5 * magnitude)")) \
        .withColumn("is_small_eq", expr("CASE WHEN magnitude < 4.5 THEN 1 ELSE 0 END"))

    # 5. Static-Streaming Join
    df_joined = df_realtime.join(df_history, "grid_id", "left") \
        .fillna({
            "energy_accum_7d": 0.0,
            "energy_accum_30d": 0.0,
            "energy_accum_90d": 0.0,
            "small_eq_freq_30d": 0.0
        })

    # 6. ML Prediction
    feature_cols = ['latitude', 'longitude', 'kedalaman', 'magnitudo', 
                    'energy_accum_7d', 'energy_accum_30d', 'energy_accum_90d', 
                    'small_eq_freq_30d', 'signifikansi']
                    
    df_for_ml = df_joined \
        .withColumn("kedalaman", col("depth")) \
        .withColumn("magnitudo", col("magnitude"))

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df_assembled = assembler.transform(df_for_ml)
    
    df_pred = rf_model.transform(df_assembled)
    
    # 7. Format Result
    df_final = df_pred \
        .withColumn("prediction_days", round(col("prediction"), 2)) \
        .withColumn("status", 
            when(col("prediction") <= 1.0, "HIGH")
            .when(col("prediction") <= 3.0, "MEDIUM")
            .otherwise("LOW")
        )

    # 8. Output to Cassandra
    query = df_final.select(
        "grid_id", "id", "time", "place", "magnitude", "longitude", 
        "latitude", "depth", "signifikansi", "energy", "is_small_eq",
        "prediction_days", "status"
    ) \
        .writeStream \
        .outputMode("append") \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "earthquake_db") \
        .option("table", "latest_events") \
        .option("checkpointLocation", "checkpoint_dir_cassandra") \
        .start()

    print("Started Spark Streaming with MLlib Prediction. Writing to Cassandra...")
    query.awaitTermination()

if __name__ == "__main__":
    process_stream()
