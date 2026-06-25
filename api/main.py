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

class VerificationItem(BaseModel):
    grid_id: str
    place: str
    predicted_days: float
    predicted_magnitude: float
    actual_found: bool
    actual_time: str = ""
    actual_days_after_main: float = 0.0
    delta_days: float = 0.0
    actual_magnitude: float = 0.0
    matched: bool = False
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


@app.get("/api/accuracy")
def get_accuracy():
    """
    Statistik akurasi prediksi: berapa banyak prediksi yang tepat dalam ±1 hari.
    """
    if not cassandra_session:
        raise HTTPException(status_code=503, detail="Cassandra tidak tersedia")

    now = datetime.now(timezone.utc)

    rows = cassandra_session.execute(
        "SELECT grid_id, id, time, prediction_days, status, magnitude "
        "FROM earthquake_db.latest_events"
    )

    total = 0
    expired = 0
    active = 0
    high = medium = low = 0
    verified = 0
    matched = 0

    for row in rows:
        if not is_verifiable(row.grid_id, row.time):
            continue

        total += 1
        s = row.status
        if s == "HIGH": high += 1
        elif s == "MEDIUM": medium += 1
        else: low += 1

        event_time = row.time
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)

        deadline = event_time + timedelta(days=row.prediction_days)
        if now < deadline:
            active += 1
            continue

        expired += 1

        if verified >= 200:
            continue

        hist = cassandra_session.execute(
            "SELECT time, magnitude FROM earthquake_db.earthquake_history "
            "WHERE grid_id = %s LIMIT 200",
            (row.grid_id,)
        )

        for h in hist:
            h_time = h.time
            if h_time.tzinfo is None:
                h_time = h_time.replace(tzinfo=timezone.utc)
            if h.magnitude is not None and h.magnitude >= 2.5:
                if abs((h_time - event_time).total_seconds()) < 60:
                    continue
                if abs((h_time - event_time).total_seconds() / 86400) > 3:
                    continue
                verified += 1
                diff = abs((h_time - deadline).total_seconds() / 86400)
                if diff <= 1:
                    matched += 1
                break

    accuracy_pct = round(matched / verified * 100, 1) if verified > 0 else 0

    return {
        "total_predictions": total,
        "active_predictions": active,
        "expired_predictions": expired,
        "checked_for_accuracy": verified,
        "predicted_within_1day": matched,
        "predicted_within_2days": matched,
        "accuracy_pct_1day": accuracy_pct,
        "status_breakdown": {"HIGH": high, "MEDIUM": medium, "LOW": low},
    }


@app.get("/api/verification", response_model=List[VerificationItem])
def get_verification(limit: int = 200):
    """
    Verifikasi per-grid: bandingkan prediksi (prediction_days) dengan
    actual gempa susulan (M≥4.0) di earthquake_history.
    Hanya untuk prediksi yang sudah expired.
    """
    if not cassandra_session:
        raise HTTPException(status_code=503, detail="Cassandra tidak tersedia")

    now = datetime.now(timezone.utc)
    results = []

    rows = cassandra_session.execute(
        "SELECT grid_id, id, time, prediction_days, status, magnitude, place, latitude, longitude "
        "FROM earthquake_db.latest_events"
    )

    for row in rows:
        if len(results) >= limit:
            break

        # Ambil event_time asli dari earthquake_history berdasarkan id
        main_event = cassandra_session.execute(
            "SELECT time FROM earthquake_db.earthquake_history "
            "WHERE grid_id = %s AND time = %s AND id = %s LIMIT 1",
            (row.grid_id, row.time, row.id)
        )
        main_row = main_event.one()
        if not main_row:
            continue

        event_time = main_row.time
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)

        deadline = event_time + timedelta(days=row.prediction_days)
        if now < deadline:
            continue

        hist = cassandra_session.execute(
            "SELECT time, magnitude FROM earthquake_db.earthquake_history "
            "WHERE grid_id = %s LIMIT 200",
            (row.grid_id,)
        )

        found = None
        best_diff = None
        for h in hist:
            h_time = h.time
            if h_time.tzinfo is None:
                h_time = h_time.replace(tzinfo=timezone.utc)
            if h.magnitude is not None and h.magnitude >= 2.5:
                if abs((h_time - event_time).total_seconds()) < 60:
                    continue
                if abs((h_time - event_time).total_seconds() / 86400) > 3:
                    continue
                diff = abs((h_time - deadline).total_seconds() / 86400)
                if best_diff is None or diff < best_diff:
                    best_diff = diff
                    found = (h_time, h.magnitude)

        lat = row.latitude or 0.0
        lon = row.longitude or 0.0

        if found:
            actual_time, actual_mag = found
            actual_days = (actual_time - event_time).total_seconds() / 86400
            if abs(actual_days) <= 3:
                delta = abs(actual_days - row.prediction_days)
                results.append(VerificationItem(
                    grid_id=row.grid_id,
                    place=row.place or "Unknown",
                    predicted_days=round(row.prediction_days, 2),
                    predicted_magnitude=round(row.magnitude or 0, 2),
                    actual_found=True,
                    actual_time=actual_time.isoformat(),
                    actual_days_after_main=round(actual_days, 3),
                    delta_days=round(delta, 3),
                    actual_magnitude=round(actual_mag, 2),
                    matched=best_diff <= 1.0,
                    latitude=lat,
                    longitude=lon,
                ))

    results.sort(key=lambda r: r.delta_days if r.actual_found else 999, reverse=True)
    return results


if __name__ == "__main__":
    import uvicorn
    print("Starting FastAPI Server (Read-Only Cassandra Mode)...")
    uvicorn.run(app, host=config.API_HOST, port=config.API_PORT)
