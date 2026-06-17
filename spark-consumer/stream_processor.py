import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, expr, round, concat, lit, window, count, sum as _sum, pow
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

# Configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "earthquake_stream"
CASSANDRA_HOST = "localhost"
CASSANDRA_PORT = "9042"

# Define the schema of the incoming JSON data from Kafka
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

def create_spark_session():
    # Make sure to include necessary packages for Kafka and Cassandra
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 pyspark-shell'
    
    spark = SparkSession.builder \
        .appName("EarthquakeStreamProcessor") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def process_stream():
    spark = create_spark_session()
    print("Spark Session created successfully.")

    # 1. Read from Kafka
    df_kafka = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Parse JSON
    df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")

    # 3. Clean Data & Handle Missing Values
    # Fill missing mmi with 0.0, replace null warning with "none"
    df_cleaned = df_parsed \
        .fillna({"mmi": 0.0, "peringatan": "none", "potensi_tsunami": "0", "signifikansi": 0.0}) \
        .filter(col("magnitude").isNotNull() & col("latitude").isNotNull() & col("longitude").isNotNull())

    # 4. Feature Engineering: Grid Transformation & Energy Calculation
    # Grid: 1x1 degree
    # Energy Formula: E = 10^(4.8 + 1.5 * M)
    df_features = df_cleaned \
        .withColumn("grid_lat", round(col("latitude"), 0)) \
        .withColumn("grid_lon", round(col("longitude"), 0)) \
        .withColumn("grid_id", concat(col("grid_lon"), lit("_"), col("grid_lat"))) \
        .withColumn("energy", pow(10, 4.8 + 1.5 * col("magnitude"))) \
        .withColumn("is_small_eq", expr("CASE WHEN magnitude < 3.0 THEN 1 ELSE 0 END"))

    # Print schema for debugging
    df_features.printSchema()

    # Note: Full sliding window aggregation (energy_accum_7d, etc.) requires watermarking
    # For a real-time predictive ML pipeline, you usually write this stream to a feature store
    # or Cassandra, and then the API/ML model pulls the historical context + real-time feature.
    
    # Example: Simple windowed aggregation (for 7 days)
    # We use a 1-day slide for simplicity in this prototype.
    df_windowed = df_features \
        .withWatermark("time", "2 hours") \
        .groupBy(
            window(col("time"), "7 days", "1 day"),
            col("grid_id")
        ) \
        .agg(
            _sum("energy").alias("energy_accum_7d"),
            _sum("is_small_eq").alias("small_eq_freq_7d")
        )

    # 5. Output to Console (for debugging/testing)
    # In production, this would be `.format("org.apache.spark.sql.cassandra")`
    query = df_features \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    print("Started Spark Streaming. Waiting for data...")
    query.awaitTermination()

if __name__ == "__main__":
    process_stream()
