import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "ml-model"))

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
from datetime import datetime, timedelta, timezone

import config

app = FastAPI(title="Sistem Peringatan Dini Gempa API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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
    cluster = Cluster([config.CASSANDRA_HOST], port=config.CASSANDRA_PORT)
    cassandra_session = cluster.connect()
    print("Berhasil terhubung ke Cassandra!")
except Exception as e:
    print(f"Gagal terhubung ke Cassandra: {e}")


def is_verifiable(grid_id: str, event_time: datetime) -> bool:
    """Cek apakah grid punya event lain dalam ≤3 hari dari event_time."""
    try:
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)
        hist = cassandra_session.execute(
            "SELECT time FROM earthquake_db.earthquake_history "
            "WHERE grid_id = %s LIMIT 200",
            (grid_id,)
        )
        for h in hist:
            h_time = h.time
            if h_time.tzinfo is None:
                h_time = h_time.replace(tzinfo=timezone.utc)
            if abs((h_time - event_time).total_seconds()) < 60:
                continue
            if abs((h_time - event_time).total_seconds() / 86400) <= 3:
                return True
        return False
    except:
        return False


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
def get_all_events(days: int = 0):
    """
    Mendapatkan daftar event/grid yang aktif untuk ditampilkan di Dashboard.
    Parameter `days` opsional: filter hanya event dari N hari terakhir.
    """
    if not cassandra_session:
        raise HTTPException(status_code=503, detail="Database Cassandra tidak tersedia")
        
    try:
        if days > 0:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
            rows = cassandra_session.execute(
                "SELECT grid_id, prediction_days, status, place, time, magnitude, latitude, longitude "
                "FROM earthquake_db.latest_events WHERE time > %s ALLOW FILTERING",
                (cutoff,)
            )
        else:
            rows = cassandra_session.execute(
                "SELECT grid_id, prediction_days, status, place, time, magnitude, latitude, longitude FROM earthquake_db.latest_events"
            )
        
        events = []
        for row in rows:
            if row.prediction_days is not None and row.status is not None:
                if not is_verifiable(row.grid_id, row.time):
                    continue
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

@app.get("/api/recent")
def get_recent(limit: int = 50, hours: int = 24):
    """
    Mengambil event gempa terbaru dari N jam terakhir.
    """
    if not cassandra_session:
        raise HTTPException(status_code=503, detail="Cassandra tidak tersedia")

    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

    try:
        rows = list(cassandra_session.execute(
            "SELECT grid_id, time, id, place, magnitude, latitude, longitude, depth "
            "FROM earthquake_db.earthquake_history "
            "WHERE time > %s ALLOW FILTERING",
            (cutoff,)
        ))
    except Exception as e:
        print(f"Recent query error: {e}")
        return []

    rows.sort(key=lambda r: r.time, reverse=True)

    results = []
    for r in rows[:limit]:
        results.append({
            "grid_id": r.grid_id,
            "id": r.id,
            "time": r.time.isoformat() if r.time else None,
            "place": r.place or "Unknown",
            "magnitude": r.magnitude or 0.0,
            "latitude": r.latitude or 0.0,
            "longitude": r.longitude or 0.0,
            "depth": r.depth or 0.0,
        })
    return results


@app.get("/api/model-info")
def get_model_info():
    """Info model ML terbaik (BEST) dari MinIO."""
    try:
        from minio_utils import get_client, get_best_version, read_metrics
        client = get_client()
        version = get_best_version(client)
        if not version:
            return {"version": None, "mae": None, "rmse": None, "r2": None, "trained_at": None}

        metrics = read_metrics(client, version)
        trained_at = version.replace("v_", "")[:15] if version else None

        return {
            "version": version,
            "mae": metrics.get("mae") if metrics else None,
            "rmse": metrics.get("rmse") if metrics else None,
            "r2": metrics.get("r2") if metrics else None,
            "trained_at": trained_at,
        }
    except Exception as e:
        print(f"Model info error: {e}")
        return {"version": None, "mae": None, "rmse": None, "r2": None, "trained_at": None}


if __name__ == "__main__":
    import uvicorn
    print("Starting FastAPI Server (Read-Only Cassandra Mode)...")
    uvicorn.run(app, host=config.API_HOST, port=config.API_PORT)
