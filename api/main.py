from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import random

app = FastAPI(title="Sistem Peringatan Dini Gempa API")

class PredictionResponse(BaseModel):
    grid_id: str
    prediction: float
    status: str

# In a real application, this would connect to Cassandra using the cassandra-driver
# and fetch the latest prediction for the grid_id.

@app.get("/api/prediction", response_model=PredictionResponse)
def get_prediction(grid_id: str):
    """
    Mendapatkan prediksi waktu (dalam hari) menuju gempa susulan 
    berdasarkan data historis dan stream real-time terbaru.
    """
    # Mocking Cassandra Query for Prototype
    # "SELECT prediction_days, risk_level FROM earthquake_prediction WHERE grid_id = {grid_id} LIMIT 1"
    
    if not grid_id:
        raise HTTPException(status_code=400, detail="grid_id is required")
        
    # Simulate some ML output
    mock_prediction_days = round(random.uniform(0.1, 10.0), 2)
    
    if mock_prediction_days <= 1.0:
        status = "HIGH"
    elif mock_prediction_days <= 3.0:
        status = "MEDIUM"
    else:
        status = "LOW"
        
    return PredictionResponse(
        grid_id=grid_id,
        prediction=mock_prediction_days,
        status=status
    )

if __name__ == "__main__":
    import uvicorn
    # Jalankan dengan: python main.py atau uvicorn main:app --reload
    print("Starting FastAPI Server...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
