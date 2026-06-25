import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))
MODEL_PATH = os.getenv("MODEL_PATH", "ml-model/spark_rf_model")
FEATURES_PATH = os.getenv("FEATURES_PATH", "ml-model/latest_features.json")
CSV_PATH = os.getenv("CSV_PATH", "dataset_gempa_bigdata.csv")
