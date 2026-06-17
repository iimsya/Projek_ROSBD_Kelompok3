import os
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
import joblib

DATA_PATH = os.path.join(os.path.dirname(__file__), "../dataset_gempa_bigdata.csv")
MODEL_PATH = "random_forest_model.pkl"

def filter_dense_grids(df: pd.DataFrame, min_events: int = 10, window_days: int = 365) -> pd.DataFrame:
    valid_grids = []
    for grid_id, group in df.groupby('grid_id'):
        times = group['waktu'].sort_values().values
        if len(times) < min_events:
            continue
        start = 0
        for end in range(len(times)):
            while times[end] - times[start] > np.timedelta64(window_days, 'D'):
                start += 1
            if (end - start + 1) >= min_events:
                valid_grids.append(grid_id)
                break
    return df[df['grid_id'].isin(valid_grids)].copy()

def create_features_and_target(df):
    """
    Simulates the Spark Feature Engineering on historical data
    and creates the target label `days_until_next_earthquake`.
    """
    print("Preprocessing data and engineering features...")
    # Filter for earthquakes
    if 'tipe' in df.columns:
        df = df[df['tipe'] == 'earthquake'].copy()
        
    df['waktu'] = pd.to_datetime(df['waktu'], errors='coerce')
    df = df.dropna(subset=['waktu', 'latitude', 'longitude', 'kedalaman', 'magnitudo']).reset_index(drop=True)
    
    # Calculate Grid ID (1x1 degree resolution)
    df['grid_lat'] = np.floor(df['latitude'] / 1.0) * 1.0
    df['grid_lon'] = np.floor(df['longitude'] / 1.0) * 1.0
    df['grid_id'] = df['grid_lat'].round(2).astype(str) + "_" + df['grid_lon'].round(2).astype(str)
    
    # Filter dense grids
    df = filter_dense_grids(df, min_events=10, window_days=365)
    
    # Calculate Energy
    df['energy'] = 10 ** (4.8 + 1.5 * df['magnitudo'])
    df['is_small_eq'] = (df['magnitudo'] < 4.5).astype(float)
    
    df = df.sort_values(by=['grid_id', 'waktu']).reset_index(drop=True)
    df = df.set_index('waktu')
    
    features_list = []
    sig_mag_threshold = 4.0
    
    for grid, group in df.groupby('grid_id'):
        group = group.sort_index()
        
        # Accumulate energy (closed='left' to prevent data leakage)
        group['energy_accum_7d'] = group['energy'].rolling('7D', closed='left').sum()
        group['energy_accum_30d'] = group['energy'].rolling('30D', closed='left').sum()
        group['energy_accum_90d'] = group['energy'].rolling('90D', closed='left').sum()
        
        # Frequency
        group['small_eq_freq_30d'] = group['is_small_eq'].rolling('30D', closed='left').sum()
        
        group = group.reset_index()
        
        # Calculate target: days until *next significant* earthquake
        sig_times = group.loc[group['magnitudo'] >= sig_mag_threshold, 'waktu'].values
        if len(sig_times) == 0:
            group['days_until_next_earthquake'] = np.nan
        else:
            current_times = group['waktu'].values
            next_indices = np.searchsorted(sig_times, current_times, side='right')
            next_sig_times = [sig_times[idx] if idx < len(sig_times) else np.datetime64('NaT') for idx in next_indices]
            group['days_until_next_earthquake'] = (np.array(next_sig_times) - current_times) / np.timedelta64(1, 'D')
            
        features_list.append(group)
        
    df_final = pd.concat(features_list).reset_index(drop=True)
    
    # Filter aftershock only
    MAX_HARI_SUSULAN = 3.0
    df_final = df_final[df_final['days_until_next_earthquake'] <= MAX_HARI_SUSULAN].copy()
    
    # Drop rows without target (the last earthquake in each grid)
    df_final = df_final.dropna(subset=['days_until_next_earthquake'])
    
    # Filter only relevant columns
    cols = ['grid_id', 'latitude', 'longitude', 'kedalaman', 'magnitudo', 
            'energy_accum_7d', 'energy_accum_30d', 'energy_accum_90d', 'small_eq_freq_30d', 
            'days_until_next_earthquake', 'potensi_tsunami', 'peringatan', 'signifikansi', 'mmi']
    
    return df_final[cols].copy()

def train_and_evaluate():
    if not os.path.exists(DATA_PATH):
        print(f"Dataset not found at {DATA_PATH}. Please run historis_ingestion.py first.")
        return
        
    df_raw = pd.read_csv(DATA_PATH)
    
    # Fill missing columns if they don't exist yet in the CSV (to avoid breaking)
    for col in ['potensi_tsunami', 'peringatan', 'signifikansi', 'mmi']:
        if col not in df_raw.columns:
            df_raw[col] = 0.0

    df = create_features_and_target(df_raw)
    
    # Features (X) and Target (y)
    # We exclude string/categorical columns for this simple prototype or encode them
    feature_cols = ['latitude', 'longitude', 'kedalaman', 'magnitudo', 
                    'energy_accum_7d', 'energy_accum_30d', 'energy_accum_90d', 
                    'small_eq_freq_30d', 'signifikansi']
    
    # Fill any remaining NaNs in features
    X = df[feature_cols].fillna(0)
    y = df['days_until_next_earthquake']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    print("\n--- Training Model (Random Forest) ---")
    
    rf = RandomForestRegressor(n_estimators=50, random_state=42, n_jobs=-1)
    rf.fit(X_train, y_train)
    rf_pred = rf.predict(X_test)
    
    def evaluate(name, y_true, y_pred):
        mae = mean_absolute_error(y_true, y_pred)
        rmse = np.sqrt(mean_squared_error(y_true, y_pred))
        r2 = r2_score(y_true, y_pred)
        print(f"{name:18} | MAE: {mae:.2f} | RMSE: {rmse:.2f} | R²: {r2:.2f}")

    print(f"\n{'Model':18} | MAE  | RMSE | R²")
    print("-" * 45)
    evaluate("Random Forest", y_test, rf_pred)
    
    print("\n--- Saving Model (Random Forest) ---")
    joblib.dump(rf, MODEL_PATH)
    print(f"Model saved to {MODEL_PATH}")
    
    print("\n--- Feature Importance (Random Forest) ---")
    importance = rf.feature_importances_
    fi_df = pd.DataFrame({'Feature': feature_cols, 'Importance': importance})
    fi_df = fi_df.sort_values(by='Importance', ascending=False)
    for _, row in fi_df.iterrows():
        print(f"{row['Feature']:20}: {row['Importance']:.4f}")

if __name__ == "__main__":
    train_and_evaluate()
