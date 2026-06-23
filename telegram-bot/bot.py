import os
import time
from datetime import datetime, timedelta, timezone
import requests
from cassandra.cluster import Cluster

# Konfigurasi Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8924410551:AAGeCnOpBJH4IAuuqoJiNPXR-N-bmh7vZqk")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "-5189248843")

# Endpoint FastAPI
API_URL = "http://localhost:8000/api/prediction"

# Setup Cassandra Connection
try:
    cluster = Cluster(['localhost'], port=9042)
    cassandra_session = cluster.connect()
    print("Bot berhasil terhubung ke Cassandra!")
except Exception as e:
    print(f"Gagal terhubung ke Cassandra: {e}")
    cassandra_session = None

def get_active_grids():
    """Mengambil semua grid_id yang ada di tabel Cassandra terbaru."""
    if not cassandra_session:
        return ["-8.0_106.0"] # Fallback
    try:
        rows = cassandra_session.execute("SELECT grid_id FROM earthquake_db.latest_events")
        return [row.grid_id for row in rows]
    except Exception as e:
        print(f"Gagal mengambil grid dari Cassandra: {e}")
        return ["-8.0_106.0"]

# Menyimpan riwayat waktu alert terakhir untuk setiap grid agar tidak spamming
alert_history = {}
ALERT_COOLDOWN_SECONDS = 3600 # 1 jam cooldown untuk grid yang sama

def format_countdown(event_time_str, prediction_days):
    if not event_time_str:
        return "Tidak diketahui"
        
    try:
        # parse ISO format string
        # Cassandra usually stores UTC. We append UTC info if it lacks one
        event_time = datetime.fromisoformat(event_time_str)
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)
            
        target_time = event_time + timedelta(days=prediction_days)
        now = datetime.now(timezone.utc)
        
        remaining = target_time - now
        total_seconds = int(remaining.total_seconds())
        
        if total_seconds <= 0:
            return "🚨 WAKTU TERLEWATI! WASPADA! 🚨"
            
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        
        if hours > 0:
            return f"{hours} Jam {minutes} Menit lagi"
        else:
            return f"{minutes} Menit lagi"
    except Exception as e:
        print(f"Error parsing date: {e}")
        return f"{prediction_days} hari lagi"

def monitor_all_grids():
    """
    Melakukan polling ke FastAPI untuk mengecek status semua grid dari Cassandra secara berkala.
    """
    grids = get_active_grids()
    print(f"[{time.strftime('%X')}] Memonitor {len(grids)} area (grid)...")
    
    current_time = time.time()
    all_high_alerts = []
    has_new_alert = False
    
    for grid_id in grids:
        try:
            response = requests.get(API_URL, params={"grid_id": grid_id})
            if response.status_code == 200:
                data = response.json()
                
                # Cek Kondisi Bahaya (Prediksi <= 1 hari atau HIGH)
                if data["prediction"] <= 1.0 or data["status"] == "HIGH":
                    countdown_str = format_countdown(data.get("event_time", ""), data["prediction"])
                    place = data.get("place", "Lokasi tidak diketahui")
                    magnitude = data.get("magnitude", 0.0)
                    
                    escaped_grid_id = str(grid_id).replace('_', '\\_')
                    alert_text = f"\U0001f4cd *Lokasi*: {place} (Grid: {escaped_grid_id} | *Telah terjadi gempa dengan Magnitudo {magnitude:.1f}*)\n"
                    alert_text += f"⏳ *Countdown Susulan*: {countdown_str}"
                    
                    # Kumpulkan semua alert beserta nilai prediksinya untuk disorting nanti
                    all_high_alerts.append((grid_id, alert_text, data["prediction"]))
                    
                    # Cek apakah ini alert baru (waktu cooldown sudah habis)
                    last_alert = alert_history.get(grid_id, 0)
                    if current_time - last_alert > ALERT_COOLDOWN_SECONDS:
                        has_new_alert = True
                else:
                    pass
            else:
                pass
                
        except requests.exceptions.ConnectionError:
            print("API is not reachable. Is FastAPI running?")
            break
        except Exception as e:
            print(f"Error monitoring API for {grid_id}: {e}")

    # Kirim Grup Notifikasi Jika Ada Minimal 1 Alert Baru
    if has_new_alert and all_high_alerts:
        # Urutkan berdasarkan nilai prediksi terkecil (waktu paling cepat/mendesak)
        all_high_alerts.sort(key=lambda x: x[2])
        
        combined_message = "⚠️ *STATUS SIAGA TINGGI GEMPA SUSULAN* ⚠️\n\n"
        alerts_text_list = [item[1] for item in all_high_alerts]
        combined_message += "\n\n".join(alerts_text_list)
        
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": combined_message,
            "parse_mode": "Markdown"
        }
        
        try:
            resp = requests.post(url, json=payload)
            if resp.status_code == 200:
                print(f"[{time.strftime('%X')}] Grouped alert sent to Telegram for {len(all_high_alerts)} grids!")
                # Reset waktu cooldown untuk SEMUA alert yang baru saja dikirim
                for grid_id, _, _ in all_high_alerts:
                    alert_history[grid_id] = current_time 
            else:
                print(f"Failed to send Telegram grouped alert: {resp.text}")
        except Exception as e:
            print(f"Error sending Telegram grouped alert: {e}")

if __name__ == "__main__":
    print("Memulai Monitoring Bot Telegram (Grouped & Countdown)...")
    
    if TELEGRAM_BOT_TOKEN == "ISI_TOKEN_BOT_DISINI":
        print("PERINGATAN: TELEGRAM_BOT_TOKEN belum diatur!")
        
    while True:
        monitor_all_grids()
        # Polling setiap 1 menit (60 detik)
        time.sleep(60)
