import os
import pandas as pd
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

CASSANDRA_HOST = "127.0.0.1"
CASSANDRA_PORT = 9042
KEYSPACE = "earthquake_db"
CSV_PATH = "dataset_gempa_bigdata.csv"

def init_db():
    print(f"Connecting to Cassandra at {CASSANDRA_HOST}:{CASSANDRA_PORT}...")
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect()
    
    print("Creating keyspace and tables...")
    session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
    WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }}
    """)
    
    session.set_keyspace(KEYSPACE)
    
    # Table for all historical and recent events
    session.execute("""
    CREATE TABLE IF NOT EXISTS all_events (
        id text PRIMARY KEY,
        time timestamp,
        magnitude double,
        place text,
        longitude double,
        latitude double,
        depth double,
        type text,
        potensi_tsunami text,
        peringatan text,
        signifikansi double,
        mmi double
    )
    """)
    
    # Table for sync metadata
    session.execute("""
    CREATE TABLE IF NOT EXISTS sync_metadata (
        id text PRIMARY KEY,
        last_synced_time timestamp
    )
    """)
    
    # Check if sync_metadata has usgs_sync record
    row = session.execute("SELECT last_synced_time FROM sync_metadata WHERE id='usgs_sync'").one()
    
    if row and row.last_synced_time:
        print(f"Database already initialized. Last synced time: {row.last_synced_time}")
        cluster.shutdown()
        return

    print("Metadata empty. Starting initialization from CSV...")
    
    if not os.path.exists(CSV_PATH):
        print(f"Error: {CSV_PATH} not found!")
        cluster.shutdown()
        return
        
    print(f"Reading {CSV_PATH}...")
    df = pd.read_csv(CSV_PATH)
    df['waktu'] = pd.to_datetime(df['waktu'])
    
    # Clean NaNs for Cassandra
    df = df.where(pd.notnull(df), None)
    
    print(f"Inserting {len(df)} rows into Cassandra. This might take a minute...")
    
    insert_query = session.prepare("""
        INSERT INTO all_events (
            id, time, magnitude, place, longitude, latitude, depth, 
            type, potensi_tsunami, peringatan, signifikansi, mmi
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)
    
    # Prepare arguments for concurrent execution
    args = []
    for _, r in df.iterrows():
        args.append((
            str(r['id']), 
            r['waktu'], 
            float(r['magnitudo']) if r['magnitudo'] is not None else 0.0, 
            str(r['tempat']) if r['tempat'] is not None else None, 
            float(r['longitude']) if r['longitude'] is not None else 0.0, 
            float(r['latitude']) if r['latitude'] is not None else 0.0, 
            float(r['kedalaman']) if r['kedalaman'] is not None else 0.0, 
            str(r['tipe']) if r['tipe'] is not None else None, 
            str(r['potensi_tsunami']) if r['potensi_tsunami'] is not None else None, 
            str(r['peringatan']) if r['peringatan'] is not None else None, 
            float(r['signifikansi']) if r['signifikansi'] is not None else 0.0, 
            float(r['mmi']) if r['mmi'] is not None else 0.0
        ))
    
    execute_concurrent_with_args(session, insert_query, args, concurrency=100)
    
    max_time = df['waktu'].max()
    print(f"Finished inserting data. Max time in CSV: {max_time}")
    
    print("Updating sync_metadata...")
    
    # Convert Pandas timestamp to standard ISO string to avoid Cassandra parsing error
    max_time_str = max_time.isoformat()
    
    session.execute(
        f"INSERT INTO sync_metadata (id, last_synced_time) VALUES ('usgs_sync', '{max_time_str}')"
    )
    
    print("Initialization complete!")
    cluster.shutdown()

if __name__ == "__main__":
    init_db()
