"""
Seed & Backfill — Satu kali jalan sebelum streaming.
1. Bulk insert CSV → Cassandra earthquake_history
2. Backfill gap (last_ts → now) dari USGS
3. Compute features → update latest_features.json
4. Load ML model → predict → upsert latest_events
"""
import sys
import os
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import numpy as np
import requests
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement, ConsistencyLevel
from cassandra import ConsistencyLevel as CL

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import config
from features import compute_features, FEATURE_COLS, assign_grid
from minio_utils import (
    get_client, ensure_bucket, version_tag,
    download_model, upload_features, get_best_version
)

USGS_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
BATCH_SIZE = 100

cassandra_session = None


def connect():
    global cassandra_session
    print(f"Connecting to Cassandra at {config.CASSANDRA_HOST}:{config.CASSANDRA_PORT}...")
    cluster = Cluster([config.CASSANDRA_HOST], port=config.CASSANDRA_PORT)
    cassandra_session = cluster.connect()
    print("Connected.")


def create_schema():
    cassandra_session.execute("""
    CREATE KEYSPACE IF NOT EXISTS earthquake_db
    WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
    """)
    cassandra_session.execute("""
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
    ) WITH CLUSTERING ORDER BY (time DESC)
    """)
    cassandra_session.execute("""
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
    print("Schema ready (earthquake_history + latest_events).")


def read_csv(path: str) -> pd.DataFrame:
    print(f"Reading CSV: {path}")
    df = pd.read_csv(path)
    print(f"  {len(df)} rows loaded.")
    df['waktu'] = pd.to_datetime(df['waktu'], errors='coerce')
    df = df[df['waktu'].notna()].copy()
    df = assign_grid(df)
    return df


def bulk_insert_history(df: pd.DataFrame):
    print("Bulk inserting into earthquake_history...")
    insert = cassandra_session.prepare("""
    INSERT INTO earthquake_db.earthquake_history
        (grid_id, time, id, place, magnitude, longitude, latitude, depth, signifikansi, energy, is_small_eq)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)
    rows = []
    for _, r in df.iterrows():
        energy = 10 ** (4.8 + 1.5 * r.get('magnitudo', 0))
        is_small = 1 if r.get('magnitudo', 0) < 4.5 else 0
        rows.append((
            r['grid_id'],
            r['waktu'].to_pydatetime(),
            str(r.get('id', '')),
            str(r.get('tempat', r.get('place', ''))),
            float(r.get('magnitudo', r.get('magnitude', 0))),
            float(r.get('longitude', 0)),
            float(r.get('latitude', 0)),
            float(r.get('kedalaman', r.get('depth', 0))),
            float(r.get('signifikansi', 0)),
            energy,
            is_small,
        ))
    total = len(rows)
    for i in range(0, total, BATCH_SIZE):
        batch = BatchStatement(consistency_level=CL.LOCAL_QUORUM)
        for row in rows[i:i + BATCH_SIZE]:
            batch.add(insert, row)
        cassandra_session.execute(batch)
        if (i + 1) % 1000 == 0 or i == 0:
            print(f"  Inserted {min(i + BATCH_SIZE, total)}/{total}")
    print(f"  Done. {total} rows inserted.")


def get_history_count() -> int:
    row = cassandra_session.execute("SELECT count(*) FROM earthquake_db.earthquake_history").one()
    return row[0] if row else 0


def get_last_timestamp() -> datetime | None:
    row = cassandra_session.execute(
        "SELECT MAX(time) as max_ts FROM earthquake_db.earthquake_history"
    ).one()
    return row.max_ts if row and row.max_ts else None


def backfill_usgs(last_ts: datetime):
    now = datetime.now(timezone.utc)
    if last_ts and last_ts.tzinfo is None:
        last_ts = last_ts.replace(tzinfo=timezone.utc)
    if last_ts and (now - last_ts).total_seconds() < 3600:
        print("  Backfill skipped — data up to date (<1h old).")
        return
    if last_ts and last_ts >= now:
        print("No gap to backfill (last_ts >= now).")
        return

    start = (last_ts if last_ts else now - timedelta(days=30)).strftime('%Y-%m-%dT%H:%M:%S')
    end = now.strftime('%Y-%m-%dT%H:%M:%S')
    print(f"Backfilling USGS gap: {start} → {end}")

    params = {
        "format": "geojson",
        "starttime": start,
        "endtime": end,
        "minmagnitude": "2.5"
    }
    try:
        resp = requests.get(USGS_URL, params=params, timeout=30)
        if resp.status_code != 200:
            print(f"  USGS API error: {resp.status_code}")
            return
        features = resp.json().get('features', [])
        print(f"  Got {len(features)} events from USGS.")
    except Exception as e:
        print(f"  USGS fetch failed: {e}")
        return

    backfill_rows = []
    for f in features:
        prop = f['properties']
        geom = f['geometry']['coordinates']
        ts = datetime.utcfromtimestamp(prop['time'] / 1000.0)
        mag = prop['mag']
        lat, lon = geom[1], geom[0]
        grid_lat = np.floor(lat / 1.0) * 1.0
        grid_lon = np.floor(lon / 1.0) * 1.0
        grid_id = f"{grid_lat:.2f}_{grid_lon:.2f}"
        energy = 10 ** (4.8 + 1.5 * mag)
        is_small = 1 if mag < 4.5 else 0
        backfill_rows.append((
            grid_id, ts, f['id'], prop.get('place', ''),
            mag, lon, lat, geom[2], prop.get('sig', 0),
            energy, is_small
        ))

    if not backfill_rows:
        print("  No new events to insert.")
        return

    insert = cassandra_session.prepare("""
    INSERT INTO earthquake_db.earthquake_history
        (grid_id, time, id, place, magnitude, longitude, latitude, depth, signifikansi, energy, is_small_eq)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)
    for i in range(0, len(backfill_rows), BATCH_SIZE):
        batch = BatchStatement(consistency_level=CL.LOCAL_QUORUM)
        for row in backfill_rows[i:i + BATCH_SIZE]:
            batch.add(insert, row)
        cassandra_session.execute(batch)
    print(f"  Inserted {len(backfill_rows)} backfill events.")


def read_all_history() -> pd.DataFrame:
    print("Reading all history from Cassandra...")
    rows = cassandra_session.execute(
        "SELECT grid_id, time, id, place, magnitude, longitude, latitude, depth, signifikansi, energy, is_small_eq "
        "FROM earthquake_db.earthquake_history"
    )
    records = []
    for r in rows:
        records.append({
            'grid_id': r.grid_id,
            'waktu': r.time,
            'id': r.id,
            'place': r.place,
            'magnitudo': r.magnitude,
            'longitude': r.longitude,
            'latitude': r.latitude,
            'kedalaman': r.depth,
            'signifikansi': r.signifikansi,
            'energy': r.energy,
            'is_small_eq': r.is_small_eq,
        })
    df = pd.DataFrame(records)
    print(f"  {len(df)} rows loaded from Cassandra.")
    return df


def save_latest_features(df_features: pd.DataFrame, minio_client=None, version=None):
    path = os.path.join(os.path.dirname(__file__), 'latest_features.json')
    latest = df_features.groupby('grid_id').tail(1).copy()
    latest.to_json(path, orient='records')
    print(f"  latest_features.json saved ({len(latest)} grids).")

    if minio_client and version:
        upload_features(minio_client, path, version)


def predict_and_update(df_features: pd.DataFrame, minio_client=None):
    from pyspark.sql import SparkSession
    from pyspark.ml.regression import RandomForestRegressionModel
    from pyspark.ml.feature import VectorAssembler

    model_dir = os.path.join(os.path.dirname(__file__), 'spark_rf_model')

    # Prioritaskan download dari MinIO (BEST) daripada model lokal
    if minio_client:
        best_ver = get_best_version(minio_client)
        if best_ver:
            print(f"  Downloading BEST model ({best_ver}) from MinIO...")
            download_model(minio_client, best_ver, model_dir)

    if not os.path.exists(model_dir):
        print("  Model not found locally or in MinIO. Skipping prediction.")
        return

    spark = SparkSession.builder \
        .appName("SeedPredict") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("Loading model...")
    rf_model = RandomForestRegressionModel.load(model_dir)

    df_features[FEATURE_COLS] = df_features[FEATURE_COLS].fillna(0.0)
    latest = df_features.groupby('grid_id').tail(1).reset_index(drop=True)

    df_spark = spark.createDataFrame(latest)
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
    df_assembled = assembler.transform(df_spark)
    df_pred = rf_model.transform(df_assembled)

    print("Upserting predictions to latest_events...")
    upsert = cassandra_session.prepare("""
    INSERT INTO earthquake_db.latest_events
        (grid_id, id, time, place, magnitude, longitude, latitude, depth,
         signifikansi, energy, is_small_eq, prediction_days, status)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    predictions = df_pred.collect()
    for row in predictions:
        pred_days = round(float(row.prediction), 2)
        if pred_days <= 1.0:
            status = "HIGH"
        elif pred_days <= 3.0:
            status = "MEDIUM"
        else:
            status = "LOW"

        cassandra_session.execute(upsert, (
            row.grid_id, row.id, row.waktu, row.place,
            float(row.magnitudo), float(row.longitude), float(row.latitude), float(row.kedalaman),
            float(row.signifikansi), float(row.energy), int(row.is_small_eq),
            pred_days, status
        ))

    spark.stop()
    print(f"  Updated {len(predictions)} grids in latest_events.")


def main():
    connect()
    create_schema()

    minio_client = None
    try:
        minio_client = get_client()
        ensure_bucket(minio_client)
        print("MinIO connected.")
    except Exception as e:
        print(f"MinIO not available ({e}), continuing without MinIO.")

    csv_path = os.path.join(os.path.dirname(__file__), '..', config.CSV_PATH)
    if os.path.exists(csv_path):
        df_csv = read_csv(csv_path)
        existing = get_history_count()
        if existing > 0 and existing >= len(df_csv) * 0.9:
            print(f"  Data already exists ({existing} rows), skipping CSV bulk insert.")
        else:
            bulk_insert_history(df_csv)
    else:
        print(f"CSV not found at {csv_path}, skipping bulk insert.")

    last_ts = get_last_timestamp()
    if last_ts:
        print(f"Last timestamp in DB: {last_ts}")
    else:
        print("No data in earthquake_history yet.")

    backfill_usgs(last_ts)

    df_history = read_all_history()
    if len(df_history) == 0:
        print("No data in history. Nothing to do.")
        return

    print("Computing features...")
    df_features = compute_features(df_history, compute_target=False)
    seed_version = version_tag()
    save_latest_features(df_features, minio_client, seed_version)

    predict_and_update(df_features, minio_client)

    print("\n=== Seed & Backfill Complete ===")
    print(f"Total events in earthquake_history: {len(df_history)}")
    print(f"Grids with predictions: {df_features['grid_id'].nunique()}")


if __name__ == "__main__":
    main()
