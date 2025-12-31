
import time
import math
import sys
from datetime import datetime, timezone, timedelta
from typing import List, Tuple
import os
import ccxt
import pandas as pd
import numpy as np
from io import StringIO
from google.cloud import storage

TIMEFRAME = '1h'
LOOKBACK_DAYS = 30
VOL_ROLL_HOURS = 240
VOL_MIN_PERIODS = 120
TOP_N_WEEKDAY = 40
TOP_N_SUNMON = 30
UNIVERSE_MAX = 180
SLEEP_BETWEEN_REQ = 0.3
SCRIPT_NAME = 'Final_1Script_Bybit_YN10k_GetList'

# Google Cloud Storage configuration
# Set this via environment variable GCS_BUCKET_NAME or update the default below
GCS_BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME', 'bybitcronjobsdata')  # Update with your bucket name


def boot_exchange():
    print("[boot_exchange] Initializing Bybit exchange...")
    ex = ccxt.bybit({
        "enableRateLimit": True,
        "options": {"defaultType": "future"}
    })
    print("[boot_exchange] Exchange initialized successfully")
    return ex

def get_script_directory():
    """
    Get the directory where this script is located.
    Works for both regular Python scripts and Jupyter notebooks.
    """
    try:
        # For regular Python scripts, __file__ is available
        script_path = os.path.abspath(__file__)
        return os.path.dirname(script_path)
    except NameError:
        # For Jupyter notebooks or interactive Python, __file__ is not available
        # Use current working directory as fallback
        return os.getcwd()


def write_csv_to_gcs(df: pd.DataFrame, gcs_blob_name: str, bucket_name: str = GCS_BUCKET_NAME) -> str:
    """
    Write a pandas DataFrame to Google Cloud Storage as CSV.
    Overwrites existing file if it exists.
    
    Args:
        df: DataFrame to write
        gcs_blob_name: Name/path for the file in GCS bucket
        bucket_name: Name of the GCS bucket
        
    Returns:
        Full GCS path (gs://bucket_name/blob_name)
    """
    try:
        print(f"[write_csv_to_gcs] Writing DataFrame to gs://{bucket_name}/{gcs_blob_name}")
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(gcs_blob_name)
        
        # Convert DataFrame to CSV string
        csv_string = df.to_csv(index=False)
        
        # Upload to GCS
        blob.upload_from_string(csv_string, content_type='text/csv')
        
        gcs_path = f"gs://{bucket_name}/{gcs_blob_name}"
        print(f"[write_csv_to_gcs] Successfully wrote {len(df)} rows to {gcs_path}")
        return gcs_path
    except Exception as e:
        print(f"[write_csv_to_gcs] ERROR writing to GCS: {type(e).__name__}: {e}", file=sys.stderr)
        raise


 
def load_universe(exchange, max_symbols: int = UNIVERSE_MAX) -> List[str]:
    print(f"[load_universe] Loading markets (max_symbols={max_symbols})...")
    markets = exchange.load_markets()
    print(f"[load_universe] Loaded {len(markets)} total markets")
    symbols_meta: List[Tuple[str, dict]] = []
    for symbol, meta in markets.items():
        if meta.get('contract') and meta.get('linear') and meta.get('active', True):
            if meta.get('quote') == 'USDT':
                symbols_meta.append((symbol, meta))
    print(f"[load_universe] Found {len(symbols_meta)} USDT linear futures contracts")
    try:
        print("[load_universe] Fetching tickers to sort by volume...")
        tickers = exchange.fetch_tickers()
        vol_map = {k: v.get('quoteVolume', 0) or v.get('baseVolume', 0) for k, v in tickers.items()}
        symbols_meta.sort(key=lambda kv: vol_map.get(kv[0], 0), reverse=True)
        print(f"[load_universe] Sorted by volume. Top 5: {[s[0] for s in symbols_meta[:5]]}")
    except Exception as e:
        print(f"[load_universe] Warning: Could not fetch tickers ({type(e).__name__}), sorting alphabetically")
        symbols_meta.sort(key=lambda kv: kv[0])
    result = [s for s, _ in symbols_meta[:max_symbols]]
    print(f"[load_universe] Returning {len(result)} symbols")
    return result


 
ex = boot_exchange()


universe = load_universe(ex)
len(universe), universe[:10]
 
def fetch_ohlcv_symbol(exchange, symbol: str, timeframe: str = TIMEFRAME, days: int = LOOKBACK_DAYS,
                        sleep: float = SLEEP_BETWEEN_REQ) -> pd.DataFrame:
    print(f"[fetch_ohlcv_symbol] Fetching {symbol} ({timeframe}, {days} days)...")
    since_ms = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000)
    since_dt = datetime.fromtimestamp(since_ms / 1000, tz=timezone.utc)
    print(f"[fetch_ohlcv_symbol] {symbol}: Starting from {since_dt.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    all_rows = []
    limit = 1500
    cursor = since_ms
    batch_num = 0
    while True:
        batch_num += 1
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=cursor, limit=limit)
            
        except ccxt.RateLimitExceeded:
            print(f"[fetch_ohlcv_symbol] {symbol}: Rate limit exceeded, waiting...")
            time.sleep(max(sleep * 2, 1.0))
            continue
        except Exception as e:
            print(f"[fetch_ohlcv_symbol] {symbol}: Error fetching data ({type(e).__name__}): {e}")
            break
        if not ohlcv:
            print(f"[fetch_ohlcv_symbol] {symbol}: No more data available")
            break
        all_rows.extend(ohlcv)
        last_t = ohlcv[-1][0]
        last_dt = datetime.fromtimestamp(last_t / 1000, tz=timezone.utc)
        print(f"[fetch_ohlcv_symbol] {symbol}: Batch {batch_num}: got {len(ohlcv)} candles, last: {last_dt.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        next_t = last_t + exchange.parse_timeframe(timeframe) * 1000
        if next_t <= cursor or len(ohlcv) < limit:
            break
        cursor = next_t
        time.sleep(sleep)
    if not all_rows:
        print(f"[fetch_ohlcv_symbol] {symbol}: No data retrieved, returning empty DataFrame")
        return pd.DataFrame(columns=['ts', 'open', 'high', 'low', 'close', 'volume', 'symbol'])
    df = pd.DataFrame(all_rows, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
    df['ts'] = pd.to_datetime(df['ts'], unit='ms', utc=True)
    df['symbol'] = symbol
    print(f"[fetch_ohlcv_symbol] {symbol}: Successfully fetched {len(df)} candles (from {df['ts'].min()} to {df['ts'].max()})")
    return df
 

def fetch_ohlcv_universe(exchange, symbols: List[str]) -> pd.DataFrame:
    print(f"[fetch_ohlcv_universe] Starting to fetch OHLCV for {len(symbols)} symbols...")
    frames = []
    for i, sym in enumerate(symbols, 1):
        print(f"[fetch_ohlcv_universe] Progress: {i}/{len(symbols)} - Processing {sym}")
        df = fetch_ohlcv_symbol(exchange, sym)
        if not df.empty:
            frames.append(df)
            print(f"[fetch_ohlcv_universe] {sym}: Added {len(df)} rows (total frames: {len(frames)})")
        else:
            print(f"[fetch_ohlcv_universe] {sym}: Skipped (empty DataFrame)")
    print(f"[fetch_ohlcv_universe] Concatenating {len(frames)} dataframes...")
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if not out.empty:
        print(f"[fetch_ohlcv_universe] Before deduplication: {len(out)} rows, {out['symbol'].nunique()} unique symbols")
        out['ts'] = out['ts'].dt.floor('h')
        out = out.drop_duplicates(subset=['symbol', 'ts']).sort_values(['symbol', 'ts'])
        print(f"[fetch_ohlcv_universe] After deduplication: {len(out)} rows, {out['symbol'].nunique()} unique symbols")
        print(f"[fetch_ohlcv_universe] Time range: {out['ts'].min()} to {out['ts'].max()}")
    else:
        print("[fetch_ohlcv_universe] Warning: No data fetched for any symbol!")
    return out


def add_time_fields(df: pd.DataFrame) -> pd.DataFrame:
    print(f"[add_time_fields] Adding time fields to {len(df)} rows...")
    d = df.copy()
    d['date'] = d['ts'].dt.floor('D')
    d['weekday'] = d['ts'].dt.weekday
    d['hour'] = d['ts'].dt.hour
    iso = d['ts'].dt.isocalendar()
    d['iso_year'] = iso['year'].astype(int)
    d['iso_week'] = iso['week'].astype(int)
    print(f"[add_time_fields] Added time fields. Date range: {d['date'].min()} to {d['date'].max()}")
    print(f"[add_time_fields] Weekdays present: {sorted(d['weekday'].unique())}, Hours present: {sorted(d['hour'].unique())}")
    return d


def compute_sigma_daily(df: pd.DataFrame, vol_roll: int = VOL_ROLL_HOURS, minp: int = VOL_MIN_PERIODS) -> pd.DataFrame:
    print(f"[compute_sigma_daily] Computing daily volatility (roll={vol_roll}h, min_periods={minp})...")
    d = df.copy()
    d['log_close'] = np.log(d['close'].astype(float))
    d['ret1h'] = d.groupby('symbol')['log_close'].diff()
    sigma_h = d.groupby('symbol')['ret1h'].transform(lambda s: s.rolling(vol_roll, min_periods=minp).std()).shift(1)
    d['sigma_daily'] = sigma_h * np.sqrt(24)
    valid_sigma = d['sigma_daily'].notna().sum()
    print(f"[compute_sigma_daily] Computed sigma_daily: {valid_sigma}/{len(d)} valid values")
    if valid_sigma > 0:
        print(f"[compute_sigma_daily] Sigma stats: min={d['sigma_daily'].min():.6f}, max={d['sigma_daily'].max():.6f}, median={d['sigma_daily'].median():.6f}")
    return d


def latest_complete_weekday_date(df: pd.DataFrame) -> pd.Timestamp:
    print("[latest_complete_weekday_date] Finding latest complete weekday...")
    d = df[(df['weekday'] < 5)]
    print(f"[latest_complete_weekday_date] Filtered to {len(d)} weekday rows")
    have23 = d[d['hour'] == 23]['date'].max()
    if pd.isna(have23):
        print("[latest_complete_weekday_date] Warning: No complete weekday found (no hour 23 data)")
    else:
        print(f"[latest_complete_weekday_date] Latest complete weekday: {have23}")
    return have23


def friday_of_last_week(df: pd.DataFrame) -> pd.Timestamp:
    print("[friday_of_last_week] Finding Friday of last complete week...")
    fridays = df[df['weekday'] == 4]
    print(f"[friday_of_last_week] Found {len(fridays)} Friday rows")
    friday_date = fridays['date'].max()
    if pd.isna(friday_date):
        print("[friday_of_last_week] Warning: No Friday found in data")
    else:
        print(f"[friday_of_last_week] Last Friday: {friday_date}")
    return friday_date


def weekday_top_list(df: pd.DataFrame, date_ref: pd.Timestamp, top_n: int = TOP_N_WEEKDAY) -> pd.DataFrame:
    print(f"[weekday_top_list] Computing top {top_n} for weekday {date_ref}...")
    day = df[df['date'] == date_ref].copy()
    print(f"[weekday_top_list] Found {len(day)} rows for date {date_ref}, {day['symbol'].nunique()} unique symbols")
    op = day[day['hour'] == 0].sort_values('ts').groupby('symbol', as_index=False).first()[['symbol', 'ts', 'close']].rename(columns={'ts': 'ts_open', 'close': 'px_open'})
    print(f"[weekday_top_list] Opening prices: {len(op)} symbols with hour 0 data")
    cl = day[day['hour'] == 23].sort_values('ts').groupby('symbol', as_index=False).last()[['symbol', 'ts', 'close', 'sigma_daily']].rename(columns={'ts': 'ts_23', 'close': 'px_23', 'sigma_daily': 'sigma_entry'})
    print(f"[weekday_top_list] Closing prices: {len(cl)} symbols with hour 23 data")
    tab = cl.merge(op, on='symbol', how='inner')
    if tab.empty:
        print("[weekday_top_list] Warning: No symbols with both opening and closing prices")
        return tab
    print(f"[weekday_top_list] Merged table: {len(tab)} symbols with both open and close")
    tab['ret_day'] = tab['px_23'] / tab['px_open'] - 1.0
    pos = tab[tab['ret_day'] > 0].copy()
    if pos.empty:
        print("[weekday_top_list] Warning: No positive returns found")
        return pos
    print(f"[weekday_top_list] Positive returns: {len(pos)} symbols")
    med = pos['ret_day'].median()
    print(f"[weekday_top_list] Median positive return: {med:.6f}")
    pos['ret_excess'] = pos['ret_day'] - med
    pos['sigma_entry'] = pos['sigma_entry'].replace([0, np.inf, -np.inf], np.nan)
    pos['sigma_entry'] = pos['sigma_entry'].fillna(pos['sigma_entry'].median())
    eps = 1e-12
    pos['score'] = pos['ret_excess'] / np.maximum(pos['sigma_entry'], eps)
    pos = pos.sort_values('score', ascending=False)
    result = pos.head(top_n)[['symbol', 'score', 'ret_day', 'ret_excess', 'sigma_entry', 'ts_23']].reset_index(drop=True)
    print(f"[weekday_top_list] Returning top {len(result)} symbols")
    if len(result) > 0:
        print(f"[weekday_top_list] Top 5: {result.head(5)[['symbol', 'score']].to_dict('records')}")
    return result


def sunmon_top_list(df: pd.DataFrame, friday_date: pd.Timestamp, top_n: int = TOP_N_SUNMON) -> pd.DataFrame:
    print(f"[sunmon_top_list] Computing top {top_n} for Thu-Fri week ending {friday_date}...")
    week_start = friday_date - pd.Timedelta(days=6)
    wk = df[df['date'].between(week_start, friday_date)].copy()
    if wk.empty:
        print(f"[sunmon_top_list] Warning: No data for week {week_start} to {friday_date}")
        return wk
    print(f"[sunmon_top_list] Week data: {len(wk)} rows, {wk['symbol'].nunique()} unique symbols")
    thu = wk[wk['weekday'] == 3].sort_values('ts').groupby('symbol', as_index=False).last()[['symbol', 'ts', 'close']].rename(columns={'ts': 'thu_ts', 'close': 'thu_px'})
    print(f"[sunmon_top_list] Thursday prices: {len(thu)} symbols")
    fri = wk[wk['weekday'] == 4].sort_values('ts').groupby('symbol', as_index=False).last()[['symbol', 'ts', 'close', 'sigma_daily']].rename(columns={'ts': 'fri_ts', 'close': 'fri_px', 'sigma_daily': 'sigma_fri'})
    print(f"[sunmon_top_list] Friday prices: {len(fri)} symbols")
    tab = fri.merge(thu, on='symbol', how='inner')
    if tab.empty:
        print("[sunmon_top_list] Warning: No symbols with both Thursday and Friday prices")
        return tab
    print(f"[sunmon_top_list] Merged table: {len(tab)} symbols with both Thu and Fri")
    tab['fri_ret'] = tab['fri_px'] / tab['thu_px'] - 1.0
    pos = tab[tab['fri_ret'] > 0].copy()
    if pos.empty:
        print("[sunmon_top_list] Warning: No positive Friday returns found")
        return pos
    print(f"[sunmon_top_list] Positive Friday returns: {len(pos)} symbols")
    med = pos['fri_ret'].median()
    print(f"[sunmon_top_list] Median positive Friday return: {med:.6f}")
    pos['fri_excess'] = pos['fri_ret'] - med
    pos['sigma_fri'] = pos['sigma_fri'].replace([0, np.inf, -np.inf], np.nan).fillna(pos['sigma_fri'].median())
    eps = 1e-12
    pos['score'] = pos['fri_excess'] / np.maximum(pos['sigma_fri'], eps)
    pos = pos.sort_values('score', ascending=False)
    result = pos.head(top_n)[['symbol', 'score', 'fri_ret', 'fri_excess', 'sigma_fri', 'fri_ts']].reset_index(drop=True)
    print(f"[sunmon_top_list] Returning top {len(result)} symbols")
    if len(result) > 0:
        print(f"[sunmon_top_list] Top 5: {result.head(5)[['symbol', 'score']].to_dict('records')}")
    return result


def run_screener_bybit(universe_max: int = UNIVERSE_MAX, top_n_weekday: int = TOP_N_WEEKDAY, top_n_sunmon: int = TOP_N_SUNMON, save_to_gcs: bool = True, bucket_name: str = GCS_BUCKET_NAME):
    print("=" * 80)
    print(f"[run_screener_bybit] Starting screener (universe_max={universe_max}, top_n_weekday={top_n_weekday}, top_n_sunmon={top_n_sunmon})")
    print("=" * 80)
    
    ex = boot_exchange()
    universe = load_universe(ex, max_symbols=universe_max)
    print(f"[run_screener_bybit] Universe loaded: {len(universe)} symbols")
    
    raw = fetch_ohlcv_universe(ex, universe)
    if raw.empty:
        print("[run_screener_bybit] ERROR: No OHLCV data retrieved!")
        return {
            'weekday': pd.DataFrame(),
            'sunmon': pd.DataFrame(),
            'ref_day': None,
            'friday': None
        }
    print(f"[run_screener_bybit] OHLCV data: {len(raw)} rows")
    
    print("\n[run_screener_bybit] Processing data...")
    df = add_time_fields(raw)
    df = compute_sigma_daily(df)
    print(f"[run_screener_bybit] Processed data: {len(df)} rows, {df['symbol'].nunique()} symbols")

    print("\n[run_screener_bybit] Computing weekday top list...")
    ref_day = latest_complete_weekday_date(df)
    if ref_day is None:
        print("[run_screener_bybit] WARNING: No reference day found, skipping weekday top list")
        weekday_tab = pd.DataFrame()
    else:
        weekday_tab = weekday_top_list(df, ref_day, top_n=top_n_weekday)
        print(f"[run_screener_bybit] Weekday top list: {len(weekday_tab)} results")

    print("\n[run_screener_bybit] Computing Sun/Mon top list...")
    weekday_now = datetime.now(timezone.utc).weekday()
    print(f"[run_screener_bybit] Current weekday: {weekday_now} (0=Monday, 6=Sunday)")
    friday_date = friday_of_last_week(df)
    if friday_date is None:
        print("[run_screener_bybit] WARNING: No Friday found, skipping Sun/Mon top list")
        sunmon_tab = pd.DataFrame()
    else:
        sunmon_tab = sunmon_top_list(df, friday_date, top_n=top_n_sunmon)
        print(f"[run_screener_bybit] Sun/Mon top list: {len(sunmon_tab)} results")

    print("\n" + "=" * 80)
    print("[run_screener_bybit] SUMMARY:")
    print(f"  - Reference day: {ref_day}")
    print(f"  - Last Friday: {friday_date}")
    print(f"  - Weekday results: {len(weekday_tab)} symbols")
    print(f"  - Sun/Mon results: {len(sunmon_tab)} symbols")
    print("=" * 80 + "\n")

    # Save weekday results to GCS if requested and data exists
    if save_to_gcs and not weekday_tab.empty:
        try:
            print(f"\n[run_screener_bybit] Saving weekday results to GCS...")
            # Save dated version
            csv_blob_date = f'weekday_bybit_{datetime.now().strftime("%Y-%m-%d")}.csv'
            gcs_path_date = write_csv_to_gcs(weekday_tab, csv_blob_date, bucket_name)
            print(f"[run_screener_bybit] Saved dated version: {gcs_path_date}")
            
            # Save latest version (this is what the trade execution script reads)
            csv_blob_latest = 'weekday_bybit_latest.csv'
            gcs_path_latest = write_csv_to_gcs(weekday_tab, csv_blob_latest, bucket_name)
            print(f"[run_screener_bybit] Saved latest version: {gcs_path_latest}")
            print(f"[run_screener_bybit] Total rows saved: {len(weekday_tab)}")
        except Exception as e:
            print(f"[run_screener_bybit] WARNING: Could not save to GCS: {type(e).__name__}: {e}")
            print(f"[run_screener_bybit] Results are still returned, but CSV was not saved to GCS")

    return {
        'weekday': weekday_tab,
        'sunmon': sunmon_tab,
        'ref_day': ref_day,
        'friday': friday_date,
    }

# --- AQUÍ EMPIEZA EL CAMBIO (EL FINAL DEL ARCHIVO) ---

if __name__ == "__main__":
    print(f"[MAIN] Iniciando Job Script 1...")
    print(f"[MAIN] Bucket configurado: {GCS_BUCKET_NAME}")
    
    try:
        # Ejecutamos la función. Ella misma se encarga de guardar el CSV en el bucket.
        results = run_screener_bybit(
            bucket_name=GCS_BUCKET_NAME,
            save_to_gcs=True
        )
        
        print("[MAIN] Job Script 1 finalizado exitosamente.")
        
        # Opcional: Imprimir un resumen en los logs de Cloud Run
        if not results['weekday'].empty:
            print(f"[MAIN] Top 3 Weekday encontrados:")
            print(results['weekday'].head(3)[['symbol', 'score']])
            
    except Exception as e:
        print(f"[MAIN] Error crítico ejecutando el Job: {e}")
        sys.exit(1) # Salir con error para que Cloud Run reporte fallo