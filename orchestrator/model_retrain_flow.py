import os
import subprocess
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(retries=1, retry_delay_seconds=10, name="Periksa Koneksi Cassandra")
def check_cassandra():
    """Tugas untuk memeriksa apakah Cassandra aktif sebelum memulai retraining"""
    print("Memeriksa status container Cassandra...")
    # Menggunakan ping/cqlsh dummy sebagai simulasi pengecekan
    try:
        result = subprocess.run(
            ["docker", "exec", "cassandra", "cqlsh", "-e", "DESCRIBE KEYSPACES;"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            print("Cassandra siap!")
            return True
        else:
            print("Gagal menghubungi Cassandra.")
            return False
    except Exception as e:
        print(f"Error: {e}")
        return False

@task(name="Retrain Model Gempa")
def retrain_model():
    """Tugas untuk mengeksekusi skrip ml-model/train.py"""
    print("Memulai pelatihan ulang model Machine Learning...")
    # Dapatkan path absolut menuju train.py
    current_dir = os.path.dirname(os.path.abspath(__file__))
    train_script_path = os.path.join(current_dir, "..", "ml-model", "train.py")
    
    ml_model_dir = os.path.dirname(train_script_path)
    result = subprocess.run(
        ["python", train_script_path],
        cwd=ml_model_dir,
        capture_output=True, text=True
    )
    
    print("Output Pelatihan:")
    print(result.stdout)
    
    if result.returncode != 0:
        print("Error pada saat pelatihan:")
        print(result.stderr)
        raise RuntimeError("Pelatihan model gagal (mungkin masalah winutils/pyspark di Windows).")
    
    return "Model berhasil diperbarui!"

@flow(name="Earthquake Model Retraining Pipeline", log_prints=True)
def earthquake_retraining_flow():
    """
    Pipeline Utama yang menskrip aliran kerja (workflow).
    Bisa dijadwalkan jalan setiap minggu (cron) lewat Prefect UI.
    """
    print("--- Memulai Pipeline Earthquake Early Warning ---")
    
    is_db_ready = check_cassandra()
    
    if is_db_ready:
        print("Database tersedia. Melanjutkan ke proses Machine Learning...")
        status = retrain_model()
        print(f"Status Akhir: {status}")
    else:
        print("Database belum siap. Menghentikan pipeline.")

if __name__ == "__main__":
    # Mengeksekusi flow secara lokal
    earthquake_retraining_flow()
