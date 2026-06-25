import pandas as pd
import numpy as np

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


def assign_grid(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['grid_lat'] = np.floor(df['latitude'] / 1.0) * 1.0
    df['grid_lon'] = np.floor(df['longitude'] / 1.0) * 1.0
    df['grid_id'] = df['grid_lat'].round(2).astype(str) + "_" + df['grid_lon'].round(2).astype(str)
    return df


def compute_features(df: pd.DataFrame, compute_target: bool = True,
                     max_hari_susulan: float = 3.0,
                     sig_mag_threshold: float = 4.0) -> pd.DataFrame:
    if 'tipe' in df.columns:
        df = df[df['tipe'] == 'earthquake'].copy()

    df['waktu'] = pd.to_datetime(df['waktu'], errors='coerce')
    df = df.dropna(subset=['waktu', 'latitude', 'longitude', 'kedalaman', 'magnitudo']).reset_index(drop=True)

    df = assign_grid(df)
    df = filter_dense_grids(df, min_events=10, window_days=365)

    df['energy'] = 10 ** (4.8 + 1.5 * df['magnitudo'])
    df['is_small_eq'] = (df['magnitudo'] < 4.5).astype(float)

    df = df.sort_values(by=['grid_id', 'waktu']).reset_index(drop=True)
    df = df.set_index('waktu')

    features_list = []
    for grid, group in df.groupby('grid_id'):
        group = group.sort_index()
        group['energy_accum_7d'] = group['energy'].rolling('7D', closed='left').sum()
        group['energy_accum_30d'] = group['energy'].rolling('30D', closed='left').sum()
        group['energy_accum_90d'] = group['energy'].rolling('90D', closed='left').sum()
        group['small_eq_freq_30d'] = group['is_small_eq'].rolling('30D', closed='left').sum()
        group = group.reset_index()

        if compute_target:
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

    if compute_target:
        df_final = df_final[df_final['days_until_next_earthquake'] <= max_hari_susulan].copy()
        df_final = df_final.dropna(subset=['days_until_next_earthquake'])

    return df_final


FEATURE_COLS = [
    'latitude', 'longitude', 'kedalaman', 'magnitudo',
    'energy_accum_7d', 'energy_accum_30d', 'energy_accum_90d',
    'small_eq_freq_30d', 'signifikansi'
]
