import os
import time
import requests

# Konfigurasi Telegram
# Dapatkan TELEGRAM_BOT_TOKEN dari BotFather di Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8924410551:AAGeCnOpBJH4IAuuqoJiNPXR-N-bmh7vZqk")
# Dapatkan TELEGRAM_CHAT_ID dari bot @userinfobot
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "-5189248843")

# Endpoint FastAPI
API_URL = "http://localhost:8000/api/prediction"

def send_telegram_alert(grid_id, prediction_days, status):
    """
    Mengirim pesan peringatan ke Telegram.
    """
    message = f"""⚠️ *SIAGA GEMPA* ⚠️

*Grid* : {str(grid_id).replace('_', '\\_')}
*Prediksi* : {prediction_days} hari menuju gempa susulan
*Status* : {status} ALERT
    """
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    
    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            print(f"[{time.strftime('%X')}] Alert sent to Telegram for Grid {grid_id}")
        else:
            print(f"Failed to send Telegram alert: {response.text}")
    except Exception as e:
        print(f"Error sending Telegram alert: {e}")

def monitor_grid(grid_id="106_-6"):
    """
    Melakukan polling ke FastAPI untuk mengecek status grid secara berkala.
    Dalam skala produksi, bot ini bisa di-trigger langsung dari Kafka (Consumer)
    jika menemukan data dengan status HIGH.
    """
    try:
        response = requests.get(API_URL, params={"grid_id": grid_id})
        if response.status_code == 200:
            data = response.json()
            
            # Cek Kondisi Bahaya (Prediksi <= 1 hari)
            if data["prediction"] <= 1.0 or data["status"] == "HIGH":
                send_telegram_alert(data["grid_id"], data["prediction"], data["status"])
            else:
                print(f"[{time.strftime('%X')}] Grid {grid_id} is safe. Prediction: {data['prediction']} days.")
        else:
            print(f"Failed to check API. Status Code: {response.status_code}")
            
    except requests.exceptions.ConnectionError:
        print("API is not reachable. Is FastAPI running?")
    except Exception as e:
        print(f"Error monitoring API: {e}")

if __name__ == "__main__":
    print("Memulai Monitoring Bot Telegram...")
    # Contoh grid_id pulau Jawa (sekitar Jakarta/Bandung)
    grid_to_monitor = "106.0_-6.0" 
    
    if TELEGRAM_BOT_TOKEN == "ISI_TOKEN_BOT_DISINI":
        print("PERINGATAN: TELEGRAM_BOT_TOKEN belum diatur!")
        
    while True:
        monitor_grid(grid_to_monitor)
        # Polling setiap 1 jam (3600 detik) atau sesuaikan kebutuhan
        time.sleep(60)
