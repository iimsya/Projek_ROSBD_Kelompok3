# Setup & Inisialisasi

## Prerequisites

- Docker & Docker Compose
- Python 3.14+, `uv`
- Node.js 18+, npm
- Java 17 (untuk Spark)
- Minimal 8GB RAM, 20GB free disk

## 1. Clone & Environment

```bash
git clone <repo-url>
cd Projek_ROSBD_Kelompok3

# Buat virtual env & install dependencies
uv venv
source .venv/bin/activate
uv sync

# Install dashboard dependencies
cd dashboard && npm install && cd ..
```

## 2. Start Infrastructure (Docker)

```bash
docker compose -f docker/docker-compose.yml up -d
```

Menjalankan: **Zookeeper** (2181), **Kafka** (9092), **Cassandra** (9042).

Cek status:

```bash
docker ps
# Semua container harus "healthy" atau "Up"
```

## 3. Start Spark Streaming (Inisialisasi Cassandra + ML)

Spark streaming otomatis membuat keyspace & tabel di Cassandra, lalu mulai consume dari Kafka.

```bash
uv run spark-consumer/stream_processor.py &
```

Tunggu hingga log muncul: `"Cassandra siap!"` dan `"Started Spark Streaming"`.

> **Catatan**: Jika restart, hapus checkpoint yang corrupt:
> ```bash
> pkill -f stream_processor
> rm -rf checkpoint_dir_cassandra checkpoint_dir_history
> uv run spark-consumer/stream_processor.py &
> ```

## 4. Start USGS Producer

Mengirim data gempa real-time dari USGS ke Kafka.

```bash
uv run producer/usgs_producer.py &
```

Cek log: `"Successfully sent N new earthquakes."`

## 5. Start FastAPI API

```bash
uv run api/main.py &
```

Server API berjalan di **http://localhost:8000**.

Cek:

```bash
curl http://localhost:8000/api/recent?limit=5
curl http://localhost:8000/api/accuracy
```

## 6. Start Dashboard

```bash
cd dashboard && npm run dev &
```

Vite dev server di **http://localhost:5173**.

## 7. Verifikasi

| Cek | Command / Cara |
|---|---|
| Data gempa masuk | `curl http://localhost:8000/api/recent?limit=5` |
| Prediksi ada | `curl http://localhost:8000/api/accuracy` |
| Dashboard muncul | Buka `http://localhost:5173` di browser |
| Streaming berjalan | `tail -f spark-consumer/*.log` |

Dashboard polling otomatis ke API tiap beberapa detik. Data akan muncul dalam 1-2 menit.

## Troubleshooting

| Masalah | Solusi |
|---|---|
| Spark crash / checkpoint error | `pkill -f stream_processor; rm -rf checkpoint_dir_*` lalu restart step 3 |
| Cassandra timeout | `docker logs cassandra` — pastikan healthy (`docker ps`) |
| Kafka tidak connect | `docker logs kafka` — cek error |
| API error saat startup | Pastikan Cassandra sudah up dan keyspace sudah dibuat (step 3 sudah jalan minimal sekali) |
| Port sudah dipakai | Ubah di `.env` atau `docker/docker-compose.yml` |
| Data tidak muncul di dashboard | Cek API langsung (`curl ...`). Jika API OK, refresh dashboard (F5) |
| Producer "No new earthquakes" | Normal — USGS rilis data tiap 1-5 menit |

## Port Summary

| Port | Service |
|---|---|
| 2181 | Zookeeper |
| 9092 | Kafka |
| 9042 | Cassandra |
| 8000 | FastAPI |
| 5173 | Dashboard (Vite) |
