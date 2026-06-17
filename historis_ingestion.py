import requests
import time
import pandas as pd
from datetime import datetime, timedelta

# 1. Konfigurasi Endpoint dan Parameter
URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
MIN_MAGNITUDE = "2.5"

# Tentukan rentang waktu projek (10 tahun: 2016 sampai 2026)
start_date = datetime(2016, 6, 11)
end_date = datetime(2026, 6, 11)

semua_gempa = []

# 2. Fungsi untuk membagi rentang waktu per 90 hari (sekitar 3 bulan)
def harian_chunks(start, end, delta):
    current = start
    while current < end:
        yield current, min(current + delta, end)
        current += delta

# Jeda 30 hari terbukti aman agar jumlah gempa tidak menembus 20.000 data
interval_hari = timedelta(days=30)

print("=== MEMULAI INGESTI DATA BIG DATA USGS ===")

# 3. Proses Looping Pengambilan Data
for chunk_start, chunk_end in harian_chunks(start_date, end_date, interval_hari):
    str_start = chunk_start.strftime('%Y-%m-%d')
    str_end = chunk_end.strftime('%Y-%m-%d')
    
    print(f"Mengambil data dari {str_start} sampai {str_end}...")
    
    params = {
        "format": "geojson",
        "starttime": str_start,
        "endtime": str_end,
        "minmagnitude": MIN_MAGNITUDE
    }
    
    try:
        response = requests.get(URL, params=params)
        
        if response.status_code == 200:
            data = response.json()
            jumlah_data = data['metadata']['count']
            print(f"-> Berhasil! Mendapatkan {jumlah_data} data gempa.")
            
            # Ekstrak data JSON ke struktur tabel
            for feature in data['features']:
                prop = feature['properties']
                geom = feature['geometry']['coordinates']
                
                gempa_item = {
                    "id": feature['id'],
                    "waktu": pd.to_datetime(prop['time'], unit='ms'), # Mengubah timestamp ke format tanggal
                    "magnitudo": prop['mag'],
                    "tempat": prop['place'],
                    "longitude": geom[0],
                    "latitude": geom[1],
                    "kedalaman": geom[2],
                    "tipe": prop['type'],
                    "potensi_tsunami": prop.get('tsunami'),
                    "peringatan": prop.get('alert'),
                    "signifikansi": prop.get('sig'),
                    "mmi": prop.get('mmi')
                }
                semua_gempa.append(gempa_item)
                
        else:
            print(f"-> Gagal di rentang ini. Status Code: {response.status_code}")
            
    except Exception as e:
        print(f"-> Terjadi error koneksi: {e}")
    
    # Etika API: Beri jeda 1 detik agar tidak dianggap spam/DDoS oleh server USGS
    time.sleep(1)

# 4. Simpan Hasil Akhir ke CSV
if semua_gempa:
    df = pd.DataFrame(semua_gempa)
    nama_file = "dataset_gempa_bigdata.csv"
    df.to_csv(nama_file, index=False)
    print("\n=== PROSES SELESAI ===")
    print(f"Total seluruh data yang berhasil dikumpulkan: {len(df)} baris.")
    print(f"File berhasil disimpan dengan nama: {nama_file}")
else:
    print("\nTidak ada data yang berhasil diambil.")