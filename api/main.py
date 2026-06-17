from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
import os

app = FastAPI(title="Sistem Peringatan Dini Gempa API")

# Add CORS Middleware to allow React frontend to fetch data
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

class PredictionResponse(BaseModel):
    grid_id: str
    prediction: float
    status: str
    place: str = "Unknown Location"
    event_time: str = ""
    magnitude: float = 0.0

class EventResponse(BaseModel):
    grid_id: str
    prediction: float
    status: str
    place: str = "Unknown Location"
    event_time: str = ""
    magnitude: float = 0.0
    latitude: float = 0.0
    longitude: float = 0.0

cassandra_session = None

try:
    from cassandra.cluster import Cluster
    cluster = Cluster(['localhost'], port=9042)
    cassandra_session = cluster.connect()
    print("Berhasil terhubung ke Cassandra!")
except Exception as e:
    print(f"Gagal terhubung ke Cassandra: {e}")

@app.get("/api/prediction", response_model=PredictionResponse)
def get_prediction(grid_id: str):
    """
    Mendapatkan prediksi waktu (dalam hari) menuju gempa susulan 
    berdasarkan data prediksi terbaru yang sudah dihitung oleh Spark Streaming
    di dalam database Cassandra.
    """
    if not grid_id:
        raise HTTPException(status_code=400, detail="grid_id is required")
        
    if not cassandra_session:
        raise HTTPException(status_code=503, detail="Database Cassandra tidak tersedia")
        
    try:
        row = cassandra_session.execute(
            "SELECT prediction_days, status, place, time, magnitude FROM earthquake_db.latest_events WHERE grid_id = %s",
            (grid_id,)
        ).one()
        
        if row and row.prediction_days is not None and row.status is not None:
            return PredictionResponse(
                grid_id=grid_id,
                prediction=row.prediction_days,
                status=row.status,
                place=row.place if row.place else "Unknown Location",
                event_time=row.time.isoformat() if row.time else "",
                magnitude=row.magnitude if row.magnitude else 0.0
            )
        else:
            return PredictionResponse(
                grid_id=grid_id,
                prediction=99.99,
                status="UNKNOWN",
                place="Unknown Location",
                event_time="",
                magnitude=0.0
            )
            
    except Exception as e:
        print(f"Error querying Cassandra for {grid_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/api/events", response_model=List[EventResponse])
def get_all_events():
    """
    Mendapatkan daftar semua event/grid yang aktif untuk ditampilkan di Dashboard.
    """
    if not cassandra_session:
        raise HTTPException(status_code=503, detail="Database Cassandra tidak tersedia")
        
    try:
        rows = cassandra_session.execute(
            "SELECT grid_id, prediction_days, status, place, time, magnitude, latitude, longitude FROM earthquake_db.latest_events"
        )
        
        events = []
        for row in rows:
            if row.prediction_days is not None and row.status is not None:
                events.append(EventResponse(
                    grid_id=row.grid_id,
                    prediction=row.prediction_days,
                    status=row.status,
                    place=row.place if row.place else "Unknown Location",
                    event_time=row.time.isoformat() if row.time else "",
                    magnitude=row.magnitude if row.magnitude else 0.0,
                    latitude=row.latitude if row.latitude else 0.0,
                    longitude=row.longitude if row.longitude else 0.0
                ))
        return events
            
    except Exception as e:
        print(f"Error querying Cassandra for events: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

if __name__ == "__main__":
    import uvicorn
    print("Starting FastAPI Server (Read-Only Cassandra Mode)...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
