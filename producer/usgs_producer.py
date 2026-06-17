import json
import time
import requests
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'earthquake_stream'
USGS_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
POLL_INTERVAL_SEC = 60

print(f"Connecting to Kafka at {KAFKA_BROKER}...")
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    print("Successfully connected to Kafka!")
except Exception as e:
    print(f"Failed to connect to Kafka: {e}")
    exit(1)

# Keep track of sent earthquake IDs to avoid duplicates
sent_ids = set()

def fetch_and_send():
    # Fetch data from the last 2 hours to ensure we don't miss anything due to API delays
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=2)
    
    params = {
        "format": "geojson",
        "starttime": start_time.isoformat(),
        "endtime": end_time.isoformat(),
        "minmagnitude": "0.0"
    }
    
    try:
        response = requests.get(USGS_URL, params=params)
        if response.status_code == 200:
            data = response.json()
            features = data.get('features', [])
            
            new_events_count = 0
            for feature in reversed(features): # Reverse to process oldest first
                eq_id = feature['id']
                if eq_id not in sent_ids:
                    prop = feature['properties']
                    geom = feature['geometry']['coordinates']
                    
                    # Convert ms timestamp to ISO format string
                    eq_time = datetime.utcfromtimestamp(prop['time'] / 1000.0).isoformat()
                    
                    payload = {
                        "id": eq_id,
                        "time": eq_time,
                        "magnitude": prop['mag'],
                        "place": prop['place'],
                        "longitude": geom[0],
                        "latitude": geom[1],
                        "depth": geom[2],
                        "type": prop['type'],
                        "potensi_tsunami": prop.get('tsunami'),
                        "peringatan": prop.get('alert'),
                        "signifikansi": prop.get('sig'),
                        "mmi": prop.get('mmi')
                    }
                    
                    # Send to Kafka
                    producer.send(KAFKA_TOPIC, value=payload)
                    sent_ids.add(eq_id)
                    new_events_count += 1
                    
                    print(f"Sent to Kafka: {payload['id']} - Mag: {payload['magnitude']} - {payload['place']}")
            
            producer.flush()
            if new_events_count == 0:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] No new earthquakes found.")
            else:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Successfully sent {new_events_count} new earthquakes.")
        else:
            print(f"Failed to fetch data from USGS. Status code: {response.status_code}")
            
    except Exception as e:
        print(f"Error fetching/sending data: {e}")

if __name__ == "__main__":
    print(f"Starting USGS Earthquake Producer...")
    print(f"Polling USGS API every {POLL_INTERVAL_SEC} seconds...")
    while True:
        fetch_and_send()
        time.sleep(POLL_INTERVAL_SEC)
