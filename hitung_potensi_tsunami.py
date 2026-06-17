import pandas as pd

# 1. Load dataset CSV yang telah digenerate sebelumnya
dataset_gempa_bigdata = pd.read_csv("dataset_gempa_bigdata.csv")

# 2. Menghitung jumlah gempa yang berpotensi tsunami (dimana nilainya = 1)
jumlah_potensi = (dataset_gempa_bigdata['potensi_tsunami'] == 1).sum()

print(f"Jumlah gempa berpotensi tsunami: {jumlah_potensi}")
