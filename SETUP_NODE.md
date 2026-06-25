# Setup Per Node

> Sistem distributed EWS gempa: VPS (Kafka + Producer) → Laptop 1 (Spark Streaming) → Laptop 2 (Cassandra + MinIO + API + Dashboard + Retrain + Bot)

---

## 1. VPS (Kafka + Producer)

### Prasyarat
- Docker & Docker Compose terinstall
- Port **9092** terbuka di firewall
- Repo sudah di-clone

### Langkah

**a. Setup `.env`**
```env
KAFKA_BROKER=localhost:9092
CASSANDRA_HOST=100.68.78.82
MINIO_ENDPOINT=100.68.78.82:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=ml-models
MODEL_PATH=ml-model/spark_rf_model
FEATURES_PATH=ml-model/latest_features.json
CSV_PATH=dataset_gempa_bigdata.csv
```

**b. Jalankan Kafka + Zookeeper**
```bash
cd docker
docker compose -f docker-compose-vps.yml up -d
```

**c. Verifikasi Kafka**
```bash
docker compose -f docker-compose-vps.yml ps
# Harusnya ada: zookeeper-1, kafka-1  (keduanya Up)
```

**d. Jalankan USGS Producer**
```bash
screen -S producer
uv run producer/usgs_producer.py
```
Detach screen: `Ctrl+A D`

### Health Check
```bash
nc -zv 168.144.97.105 9092
# Harus: Connection succeeded
docker logs kafka-1 --tail 20
```

---

## 2. Laptop 2 (Cassandra + MinIO + API + Dashboard + Retrain + Bot)

### Prasyarat
- Docker & Docker Compose terinstall
- Node.js (v18+) & npm terinstall
- Python 3.10+ dengan `uv`
- Tailscale join (IP: 100.68.78.82)
- Repo sudah di-clone

### Langkah

**a. Setup `.env`**
```env
KAFKA_BROKER=168.144.97.105:9092
CASSANDRA_HOST=localhost
CASSANDRA_PORT=9042
API_HOST=0.0.0.0
API_PORT=8000
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=ml-models
MODEL_PATH=ml-model/spark_rf_model
FEATURES_PATH=ml-model/latest_features.json
CSV_PATH=dataset_gempa_bigdata.csv
```

**b. Jalankan Cassandra + MinIO**
```bash
cd docker
docker compose -f docker-compose-l2.yml up -d
```

Tunggu ~30 detik agar Cassandra siap.

**c. Train model awal & upload ke MinIO**
```bash
uv run ml-model/train.py
uv run ml-model/retrain_flow.py
```

Verifikasi MinIO: buka `http://localhost:9001` (login: `minioadmin`/`minioadmin`), cek bucket `ml-models` ada, ada folder `v_*/` dengan model + features.

**d. Seed data & backfill (download model dari MinIO, predict, insert)**
```bash
uv run ml-model/seed_and_backfill.py
```

**e. Jalankan FastAPI**
```bash
screen -S api
uvicorn api.main:app --host 0.0.0.0 --port 8000
```
Detach: `Ctrl+A D`

Cek: `curl http://localhost:8000/health`

**f. Jalankan Dashboard**
```bash
cd dashboard
npm install
npm run dev
```
Dashboard akan berjalan di `http://localhost:5173`.

**g. Jalankan Telegram Bot**
```bash
screen -S bot
uv run telegram-bot/bot.py
```
Detach: `Ctrl+A D`

**h. (Opsional) Jadwalkan retrain otomatis via cron**
```bash
crontab -e
```
Tambahkan (retrain tiap 6 jam):
```
0 */6 * * * cd /home/portolas/kuliah/Datmin/project/Projek_ROSBD_Kelompok3 && uv run ml-model/retrain_flow.py >> retrain_cron.log 2>&1
```

### Health Check
```bash
# Cassandra
docker exec -it cassandra cqlsh -e "SELECT count(*) FROM earthquake_db.latest_events;"

# MinIO Console
curl http://localhost:9001

# API
curl http://localhost:8000/health

# MinIO objects
uv run python -c "
from minio_utils import get_client
mc = get_client()
print('Buckets:', mc.list_buckets())
objects = list(mc.list_objects('ml-models', recursive=True))
for o in objects:
    print(f'  {o.object_name}  ({o.size} bytes)')
"
```

---

## 3. Laptop 1 (Spark Streaming Consumer)

### Prasyarat
- Python 3.10+ dengan `uv`
- Repo sudah di-clone
- Tailscale join (IP: 100.105.213.75)
- Bisa reach VPS (port 9092) dan Laptop 2 (port 9042, 9000)

### Langkah

**a. Setup `.env`**
```env
KAFKA_BROKER=168.144.97.105:9092
CASSANDRA_HOST=100.68.78.82
CASSANDRA_PORT=9042
MINIO_ENDPOINT=100.68.78.82:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=ml-models
MODEL_PATH=ml-model/spark_rf_model
FEATURES_PATH=ml-model/latest_features.json
CSV_PATH=dataset_gempa_bigdata.csv
```

**b. Test koneksi ke semua service**
```bash
nc -zv 168.144.97.105 9092   # Kafka VPS
nc -zv 100.68.78.82 9042     # Cassandra L2
nc -zv 100.68.78.82 9000     # MinIO L2
```
Semua harus `Connection succeeded`.

**c. Hapus checkpoint lama** (kalau fresh start)
```bash
rm -rf checkpoint_dir_*
```

**d. Jalankan Spark Streaming**
```bash
screen -S stream
uv run spark-consumer/stream_processor.py
```
Detach: `Ctrl+A D`

### Health Check
```bash
# Cek log
tail -F spark_stream_output.log
# Harusnya ada: "writeBatch to Cassandra: 1 rows" tiap menit

# Verifikasi data masuk ke Cassandra (dari L2)
docker exec -it cassandra cqlsh -e "SELECT count(*) FROM earthquake_db.latest_events;"
```

---

## Urutan Start yang Benar

```
 1️⃣ VPS       docker compose -f docker-compose-vps.yml up -d
              ↓ (tunggu 15 detik)
              usgs_producer.py

  2️⃣ Laptop 2  docker compose -f docker-compose-l2.yml up -d
              ↓ (tunggu 30 detik sampai Cassandra ready)
              train.py + retrain_flow.py   (upload model ke MinIO)
              ↓
              seed_and_backfill.py          (download model dari MinIO)
              ↓
              FastAPI (api/main.py)
              ↓
              Dashboard (npm run dev)
              ↓
              Telegram Bot (bot.py)

 3️⃣ Laptop 1  stream_processor.py
```

> **PENTING:** Laptop 2 harus selesai start (Cassandra + MinIO + API) sebelum Laptop 1 jalan, karena L1 nulis prediksi ke Cassandra & polling model dari MinIO.

---

## Troubleshooting

### Kafka
| Gejala | Solusi |
|---|---|
| `Connection refused` ke Kafka | Cek firewall VPS port 9092, cek `docker ps` di VPS |
| Producer error `NodeExists` | `docker compose -f docker-compose-vps.yml down -v && docker compose -f docker-compose-vps.yml up -d` |
| Consumer `failOnDataLoss` | Hapus `checkpoint_dir_*` di L1, restart stream |

### Cassandra (L2)
| Gejala | Solusi |
|---|---|
| `Connection refused` | Cek `docker ps`, cassandra butuh ~30 detik untuk ready |
| `cqlsh` error auth | Default cassandra: no password (kosong) |
| Query timeout | Tunggu seeding selesai |

### MinIO (L2)
| Gejala | Solusi |
|---|---|
| `AccessDenied` bucket | `retrain_flow.py` atau `seed_and_backfill.py` harus jalan duluan (via `ensure_bucket()`) |
| L1 can't reach MinIO | Cek Tailscale: `ping 100.68.78.82` dari L1 |

### Streaming (L1)
| Gejala | Solusi |
|---|---|
| `No module named 'pyspark'` | `uv sync` dulu, pastikan pyspark di pyproject.toml |
| Cassandra write slow | Cek network latency L1 → L2 via Tailscale |
| Model not found | `retrain_flow.py` atau `train.py` harus jalan minimal sekali di L2 (upload model ke MinIO) |
| Features not found | `retrain_flow.py` atau `seed_and_backfill.py` harus jalan minimal sekali (upload features ke MinIO) |
