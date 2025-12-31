import pandas as pd
import ccxt
import time
import os
import sys
from datetime import datetime, timezone
import json
import math
# Nueva importación para Secret Manager
from google.cloud import secretmanager
from google.cloud import storage
from io import StringIO

# ==============================================================================
# CONFIGURACIÓN Y VARIABLES DE ENTORNO
# ==============================================================================

# 1. Identificación del Proyecto y Secretos (Inyectado por Scheduler)
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID') # Obligatorio
SECRET_NAME = os.environ.get('SECRET_NAME')       # Obligatorio (ej: bot_combo_01)

# 2. Configuración del Experimento (Inyectado por Scheduler)
TEST_ID = os.environ.get('TEST_ID', 'default_test') # Nombre de la carpeta (ej: combo_1_5t_2h)
try:
    TOP_N = int(os.environ.get('TARGET_TOKEN_COUNT', 30)) # Cantidad de tokens
except ValueError:
    TOP_N = 30

# 3. Configuración de Storage
GCS_BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME', 'bybitcronjobsdata')
# El archivo maestro SIEMPRE está en la raíz
CSV_FILE_MASTER = 'weekday_bybit_latest.csv' 
# Los logs de este bot van a su carpeta dinámica
CSV_ACCOUNT_PNL = f"{TEST_ID}/account_pnl.csv"
CSV_ACCOUNT_TRADES = f"{TEST_ID}/account_trades.csv"

# 4. Configuración de Trading
SCRIPT_NAME_SHORT = 'Trade_Execution'
ORDER_SIZE = 0.1
SLEEP_BETWEEN_ORDERS = 0.5
SLEEP_BETWEEN_SYMBOLS = 1.0
LEVERAGE_FOR_ORDERS = 1
LEVERAGE_FOR_SYMBOLS = 3
RATE_LIMIT_DELAY = 0.1
RETRY_DELAY_BASE = 1.0
MAX_RETRIES = 3


def get_secret(project_id, secret_id):
    """
    Obtiene el API Key y Secret desde Google Secret Manager.
    Espera un JSON: {"api_key": "...", "api_secret": "..."}
    """
    try:
        # Crear cliente de Secret Manager
        client = secretmanager.SecretManagerServiceClient()
        # Construir nombre del recurso
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        # Acceder
        response = client.access_secret_version(request={"name": name})
        payload = response.payload.data.decode("UTF-8")
        
        # Parsear JSONd
        creds = json.loads(payload)
        return creds.get('api_key'), creds.get('api_secret')
    except Exception as e:
        print(f"[get_secret] Error crítico obteniendo secreto: {e}")
        raise

#FUNCTION TO DECLARE EXCHANGE
def boot_exchange_demo(api_key, api_secret):
    """Initialize Bybit demo/testnet exchange"""
    config = {
        "enableRateLimit": True,
        "options": {"defaultType": "future"},
        "apiKey": api_key,
        "secret": api_secret
    }
    ex = ccxt.bybit(config)
    #ex.set_sandbox_mode(True)  # Enable demo/testnet mode
    ex.enable_demo_trading(True)
    return ex

## CSV LOGGING FUNCTIONS

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


def log_account_balance_to_csv(exchange, csv_file: str = CSV_ACCOUNT_PNL, bucket_name: str = GCS_BUCKET_NAME):
    """
    Fetch account balance and log to CSV file in Google Cloud Storage.
    Creates file with headers if it doesn't exist, appends if it does.
    Always preserves existing data by reading, appending, then writing back.
    """
    try:
        # Fetch balance with rate limiting
        time.sleep(RATE_LIMIT_DELAY)
        balance_data = exchange.fetchBalance()
        
        # Extract relevant balance information
        timestamp = datetime.now(timezone.utc)
        
        # Prepare record with timestamp and all balance info
        record = {
            'timestamp': timestamp.isoformat(),
            'timestamp_utc': timestamp.strftime('%Y-%m-%d %H:%M:%S UTC'),
            'date': timestamp.strftime('%Y-%m-%d'),
            'time': timestamp.strftime('%H:%M:%S'),
            'script_name': SCRIPT_NAME_SHORT,
            'test_id': TEST_ID # Agregamos el ID del test al log por si acaso
        }
        
        # Add all currencies from the balance
        if 'free' in balance_data:
            for currency, value in balance_data['free'].items():
                record[f'free_{currency}'] = value
                
        if 'used' in balance_data:
            for currency, value in balance_data['used'].items():
                record[f'used_{currency}'] = value
                
        if 'total' in balance_data:
            for currency, value in balance_data['total'].items():
                record[f'total_{currency}'] = value
                
        # Add info if available
        if 'info' in balance_data:
            record['info'] = json.dumps(balance_data['info']) if balance_data['info'] else None
        
        # Create DataFrame from new record
        df_new = pd.DataFrame([record])
        
        # Write to GCS (write_csv_to_gcs handles appending if file exists)
        gcs_path = write_csv_to_gcs(df_new, csv_file, bucket_name)
        print(f"[log_account_balance] Logged balance record to {gcs_path}")
            
        return record
        
    except Exception as e:
        print(f"[log_account_balance] Error logging balance: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


def log_trade_to_csv(order_data, symbol: str, order_type: str, csv_file: str = CSV_ACCOUNT_TRADES, bucket_name: str = GCS_BUCKET_NAME):
    """
    Log trade details to CSV file in Google Cloud Storage.
    Creates file with headers if it doesn't exist, appends if it does.
    Always preserves existing data by reading, appending, then writing back.
    """
    try:
        # Prepare timestamp
        timestamp = datetime.now(timezone.utc)
        
        # Extract order information
        record = {
            'timestamp': timestamp.isoformat(),
            'timestamp_utc': timestamp.strftime('%Y-%m-%d %H:%M:%S UTC'),
            'date': timestamp.strftime('%Y-%m-%d'),
            'time': timestamp.strftime('%H:%M:%S'),
            'test_id': TEST_ID,
            'symbol': symbol,
            'order_type': order_type,  # 'market', 'limit', etc.
            'side': order_data.get('side', 'N/A'),
            'order_id': order_data.get('id', 'N/A'),
            'status': order_data.get('status', 'N/A'),
            'amount': order_data.get('amount', None),
            'filled': order_data.get('filled', 0),
            'remaining': order_data.get('remaining', None),
            'price': order_data.get('price', None),
            'average': order_data.get('average', None),
            'cost': order_data.get('cost', None),
            
        }
        
        # Extract additional info from nested structure if available
        if 'info' in order_data and order_data['info']:
            info = order_data['info']
            # Try to extract more details from info
            if isinstance(info, dict):
                record['order_id_exchange'] = info.get('orderId', info.get('order_id', 'N/A'))
                record['avg_price'] = info.get('avgPrice', info.get('avg_price', None))
                record['executed_qty'] = info.get('executedQty', info.get('executed_qty', None))
                record['cum_exec_qty'] = info.get('cumExecQty', info.get('cum_exec_qty', None))
                record['order_status'] = info.get('orderStatus', info.get('order_status', None))
                record['create_time'] = info.get('createTime', info.get('create_time', None))
                record['update_time'] = info.get('updateTime', info.get('update_time', None))
                
                
                # Store full info as JSON for reference
                record['info_json'] = json.dumps(info) if info else None
        
        # Create DataFrame from new record
        df_new = pd.DataFrame([record])
        
        # Write to GCS (write_csv_to_gcs handles appending if file exists)
        gcs_path = write_csv_to_gcs(df_new, csv_file, bucket_name)
        print(f"[log_trade] Logged trade record for {symbol} to {gcs_path}")
            
        return record
        
    except Exception as e:
        print(f"[log_trade] Error logging trade: {str(e)}")
        print(f"[log_trade] Order data: {order_data}")
        import traceback
        traceback.print_exc()
        return None



## HELPER FUNCTIONS WITH RETRY LOGIC

def api_call_with_retry(func, *args, **kwargs):
    """
    Execute an API call with retry logic and rate limiting.
    """
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(RATE_LIMIT_DELAY)
            return func(*args, **kwargs)
        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY_BASE * (2 ** attempt)  # Exponential backoff
                print(f"[api_call_with_retry] Attempt {attempt + 1} failed: {str(e)}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"[api_call_with_retry] All {MAX_RETRIES} attempts failed for {func.__name__}")
                raise
        except Exception as e:
            # For non-network errors, don't retry
            print(f"[api_call_with_retry] Non-retryable error in {func.__name__}: {str(e)}")
            raise

## LOAD SYMBOLS FROM CSV FILE

def load_top_symbols(csv_file: str, top_n: int = TOP_N, bucket_name: str = GCS_BUCKET_NAME) -> list:
    """Load top N symbols from CSV file in Google Cloud Storage"""
    try:
        # Read CSV from GCS
        df = read_csv_from_gcs(csv_file, bucket_name)
        symbols = df['symbol'].head(top_n).tolist()
        return symbols
    except Exception as e:
        print(f"[load_top_symbols] Error loading symbols from GCS: {str(e)}")
        raise

## FUNCTIONS TO PROCESS MARKET DATA

def convert_numeric_string(value):
    """
    Convert numeric string to appropriate numeric type while preserving precision.
    Handles integers, floats, and scientific notation.
    Always returns a number (int or float), never a string.
    """
    if isinstance(value, (int, float)):
        return value
    elif isinstance(value, str) and value.strip():
        try:
            if '.' not in value and 'e' not in value.lower() and 'E' not in value:
                try:
                    return int(value)
                except ValueError:
                    pass
            return float(value)
        except (ValueError, TypeError):
            return value
    return value

def flatten_market_item(item: dict, parent_key: str = '', sep: str = '_') -> dict:
    """
    Flatten a market item dictionary, converting nested structures to flat columns.
    Converts numeric strings to numbers while preserving precision.
    All keys are lowercase.
    """
    flattened = {}
    for k, v in item.items():
        new_key = f"{parent_key}{sep}{k.lower()}" if parent_key else k.lower()
        if isinstance(v, dict):
            nested = flatten_market_item(v, new_key, sep=sep)
            flattened.update(nested)
        elif isinstance(v, list):
            flattened[new_key] = str(v) if len(str(v)) < 100 else str(v)[:100] + '...'
        elif v is None:
            flattened[new_key] = None
        elif isinstance(v, str):
            numeric_val = convert_numeric_string(v)
            flattened[new_key] = numeric_val
        else:
            flattened[new_key] = v
    return flattened

def markets_to_dataframe(markets_list: list) -> pd.DataFrame:
    """
    Convert list of market dictionaries to a flattened DataFrame.
    Preserves numeric precision, expands nested structures, all lowercase columns.
    """
    flattened_items = []
    for item in markets_list:
        flattened = flatten_market_item(item)
        flattened_items.append(flattened)
    
    df = pd.DataFrame(flattened_items)
    df.columns = [col.lower() if isinstance(col, str) else str(col).lower() for col in df.columns]
    return df

## GET TOKEN INFO FUNCTION
def get_token_info(symbol: str, markets_df: pd.DataFrame) -> dict:
    """Get market info for a specific token"""
    token_row = markets_df[markets_df['symbol'] == symbol]
    if not token_row.empty:
        return token_row.iloc[0].to_dict()
    else:
        return {'error': f'Symbol {symbol} not found'}

## SET LEVERAGE FOR EACH SYMBOL (with smart error handling)
def set_leverage_smart(exchange, leverage: int, symbol: str) -> bool:
    """
    Set leverage for a symbol, treating 'already set' errors as success.
    Returns True if successful or already set, False on actual error.
    """
    try:
        time.sleep(RATE_LIMIT_DELAY)
        exchange.setLeverage(leverage, symbol)
        return True
    except Exception as e:
        error_str = str(e).lower()
        error_msg = str(e)
        
        # Check if error indicates leverage is already set (not modified)
        # Common Bybit error messages: "not modified", "same value", "already", etc.
        if any(keyword in error_str for keyword in [
            'not modified', 'not change', 'no change', 'same value',
            'already set', 'already', 'unchanged', 'identical'
        ]):
            # Leverage is already set correctly - this is success, not an error
            return True
        else:
            # Actual error - re-raise it
            raise

## HELPER FUNCTIONS FOR TRADING

def get_best_bid_ask(symbol: str, exchange: ccxt.Exchange) -> tuple:
    """Get the best bid and ask for a symbol with rate limiting"""
    ticker = api_call_with_retry(exchange.fetchTicker, symbol)
    return ticker['bid'], ticker['ask']


def place_short_mk_order_professional(exchange, symbol: str, size_per_symbol: float, markets_df: pd.DataFrame, all_orders_to_fetch: list):
    """
    Place market sell order(s) for the given symbol, respecting max order size limits.
    Returns list of order objects.
    """
    try:
        print(f"\n[PLACE_ORDER] Processing {symbol}...")
        
        # Get current bid/ask
        bid, ask = get_best_bid_ask(symbol, exchange)
        print(f"  Current bid: {bid}, ask: {ask}")
        
        # Calculate contract amount
        contracts_amount = size_per_symbol / ask
        print(f"  Target size: {size_per_symbol:.2f} USDT")
        print(f"  Contracts to sell: {contracts_amount:.6f}")
        
        # Get max market order quantity
        token_info = get_token_info(symbol, markets_df)
        max_market_order_quantity = token_info.get('info_lotsizefilter_maxmktorderqty')
        
        # Handle None or NaN values - use max_order_qty as fallback, then limits_amount_max
        if max_market_order_quantity is None or pd.isna(max_market_order_quantity):
            max_market_order_quantity = token_info.get('info_lotsizefilter_maxorderqty')
        if max_market_order_quantity is None or pd.isna(max_market_order_quantity):
            max_market_order_quantity = token_info.get('limits_amount_max', float('inf'))
        if max_market_order_quantity is None or pd.isna(max_market_order_quantity):
            max_market_order_quantity = float('inf')  # No limit if not available
        
        print(f"  Max market order quantity: {max_market_order_quantity}")
        
        orders_placed = []
        
        # Check if we need to split the order
        if contracts_amount > max_market_order_quantity:
            # Split into multiple orders
            num_parts = int(math.ceil(contracts_amount / max_market_order_quantity))
            contracts_amount_per_part = contracts_amount / num_parts
            
            print(f"  ⚠ Contract size exceeds max order size")
            print(f"  Splitting into {num_parts} orders of ~{contracts_amount_per_part:.6f} contracts each")
            print(f"  ----------------------------------------")
            
            # Place multiple orders
            for part_num in range(num_parts):
                try:
                    order = api_call_with_retry(
                        exchange.create_market_order,
                        symbol=symbol,
                        side='sell',
                        amount=contracts_amount_per_part
                    )
                    
                    order_id = order.get('id')
                    print(f"  ✓ Order {part_num + 1}/{num_parts} placed - ID: {order_id}")
                    
                    # Store for later fetching
                    all_orders_to_fetch.append({
                        'order_id': order_id,
                        'symbol': symbol,
                        'initial_order': order,
                        'part': f"{part_num + 1}/{num_parts}"
                    })
                    
                    orders_placed.append(order)
                    
                    # Rate limiting between orders
                    if part_num < num_parts - 1:  # Don't sleep after last order
                        time.sleep(SLEEP_BETWEEN_ORDERS)
                        
                except Exception as part_error:
                    print(f"  ✗ Error placing part {part_num + 1} for {symbol}: {str(part_error)}")
                    time.sleep(RATE_LIMIT_DELAY)
            
            print(f"  ----------------------------------------")
            print(f"  ✓ All {num_parts} orders placed for {symbol}")
            
        else:
            # Single order - within limits
            print(f"  Contract size within limits, placing single order...")
            
            order = api_call_with_retry(
                exchange.create_market_order,
                symbol=symbol,
                side='sell',
                amount=contracts_amount
            )
            
            order_id = order.get('id')
            print(f"  ✓ Order placed - ID: {order_id}")
            
            # Store for later fetching
            all_orders_to_fetch.append({
                'order_id': order_id,
                'symbol': symbol,
                'initial_order': order,
                'part': "1/1"
            })
            
            orders_placed.append(order)
        
        return orders_placed
        
    except Exception as e:
        print(f"  ✗ Error placing order for {symbol}: {str(e)}")
        import traceback
        traceback.print_exc()
        return []


def read_csv_from_gcs(gcs_blob_name: str, bucket_name: str = GCS_BUCKET_NAME) -> pd.DataFrame:
    """
    Read a CSV file from Google Cloud Storage.
    
    Args:
        gcs_blob_name: Name/path for the file in GCS bucket
        bucket_name: Name of the GCS bucket
        
    Returns:
        pandas DataFrame containing the CSV data
        
    Raises:
        Exception if file doesn't exist or can't be read
    """
    try:
        print(f"[read_csv_from_gcs] Reading gs://{bucket_name}/{gcs_blob_name}")
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(gcs_blob_name)
        
        # Check if blob exists
        if not blob.exists():
            raise FileNotFoundError(f"CSV file {gcs_blob_name} not found in bucket {bucket_name}")
        
        # Download blob content as string
        csv_content = blob.download_as_text()
        
        # Read CSV from string
        df = pd.read_csv(StringIO(csv_content))
        print(f"[read_csv_from_gcs] Successfully read CSV with {len(df)} rows from gs://{bucket_name}/{gcs_blob_name}")
        return df
    except Exception as e:
        print(f"[read_csv_from_gcs] ERROR reading from GCS: {type(e).__name__}: {e}", file=sys.stderr)
        raise

def write_csv_to_gcs(df: pd.DataFrame, gcs_blob_name: str, bucket_name: str = GCS_BUCKET_NAME) -> str:
    """
    Write a pandas DataFrame to Google Cloud Storage as CSV.
    Appends to existing file if it exists, otherwise creates new file.
    
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
        
        # Check if file exists
        if blob.exists():
            # Read existing CSV
            try:
                existing_csv = blob.download_as_text()
                df_existing = pd.read_csv(StringIO(existing_csv))
                # Combine with new data
                df_combined = pd.concat([df_existing, df], ignore_index=True)
                print(f"[write_csv_to_gcs] Appending to existing file (existing: {len(df_existing)} rows, new: {len(df)} rows)")
            except Exception as read_error:
                # If reading fails, just write new data
                print(f"[write_csv_to_gcs] Warning: Could not read existing file, overwriting: {read_error}")
                df_combined = df
        else:
            # Create new file
            df_combined = df
            print(f"[write_csv_to_gcs] Creating new file with {len(df)} rows")
        
        # Convert DataFrame to CSV string
        csv_string = df_combined.to_csv(index=False)
        
        # Upload to GCS
        blob.upload_from_string(csv_string, content_type='text/csv')
        
        gcs_path = f"gs://{bucket_name}/{gcs_blob_name}"
        print(f"[write_csv_to_gcs] Successfully wrote {len(df_combined)} rows to {gcs_path}")
        return gcs_path
    except Exception as e:
        print(f"[write_csv_to_gcs] ERROR writing to GCS: {type(e).__name__}: {e}", file=sys.stderr)
        raise

def upload_to_gcs(local_file_path: str, gcs_blob_name: str, bucket_name: str = GCS_BUCKET_NAME) -> str:
    """
    Upload a file from local filesystem to Google Cloud Storage.
    
    Args:
        local_file_path: Path to the local file to upload
        gcs_blob_name: Name/path for the file in GCS bucket
        bucket_name: Name of the GCS bucket
        
    Returns:
        Full GCS path (gs://bucket_name/blob_name)
    """
    try:
        print(f"[upload_to_gcs] Uploading {local_file_path} to gs://{bucket_name}/{gcs_blob_name}")
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(gcs_blob_name)
        
        blob.upload_from_filename(local_file_path)
        
        gcs_path = f"gs://{bucket_name}/{gcs_blob_name}"
        print(f"[upload_to_gcs] Successfully uploaded to {gcs_path}")
        return gcs_path
    except Exception as e:
        print(f"[upload_to_gcs] ERROR uploading to GCS: {type(e).__name__}: {e}", file=sys.stderr)
        raise

def save_dataframe_to_gcs(df: pd.DataFrame, gcs_blob_name: str, bucket_name: str = GCS_BUCKET_NAME) -> str:
    """
    Save a pandas DataFrame to Google Cloud Storage via temporary local file.
    
    Args:
        df: DataFrame to save
        gcs_blob_name: Name/path for the file in GCS bucket
        bucket_name: Name of the GCS bucket
        
    Returns:
        Full GCS path (gs://bucket_name/blob_name)
    """
    # Save to /tmp/ first (this folder is writable in Cloud Functions)
    temp_filename = f'/tmp/{os.path.basename(gcs_blob_name)}'
    print(f"[save_dataframe_to_gcs] Saving DataFrame to temporary file: {temp_filename}")
    df.to_csv(temp_filename, index=False)
    print(f"[save_dataframe_to_gcs] File saved temporarily to {temp_filename}")
    
    # Upload to GCS
    gcs_path = upload_to_gcs(temp_filename, gcs_blob_name, bucket_name)
    
    # Clean up temporary file
    try:
        os.remove(temp_filename)
        print(f"[save_dataframe_to_gcs] Cleaned up temporary file: {temp_filename}")
    except Exception as e:
        print(f"[save_dataframe_to_gcs] Warning: Could not remove temp file: {e}")
    
    return gcs_path

    

# ==============================================================================
# MAIN JOB EXECUTION
# ==============================================================================

def run_trade_execution():
    print(f"[MAIN] Iniciando Job Script (Trade Executor) - {datetime.now(timezone.utc)}")
    print(f"[CONFIG] Project: {GCP_PROJECT_ID}, Secret: {SECRET_NAME}, TestID: {TEST_ID}, Tokens: {TOP_N}")
    
    # Validación básica de entorno
    if not GCP_PROJECT_ID or not SECRET_NAME:
        raise ValueError("Faltan variables de entorno GCP_PROJECT_ID o SECRET_NAME")

    # --------------------------------------------------------------------------
    # 1. OBTENER CREDENCIALES Y BOOT EXCHANGE
    # --------------------------------------------------------------------------
    print("[MAIN] 1. Obteniendo credenciales de Secret Manager...")
    api_key, api_secret = get_secret(GCP_PROJECT_ID, SECRET_NAME)
    
    # Iniciar Exchange (Usa la función boot_exchange_demo del Script 1 que recibe args)
    ex = boot_exchange_demo(api_key, api_secret)
    print("[MAIN] Exchange inicializado correctamente.")

    # --------------------------------------------------------------------------
    # 2. CARGAR LISTA MAESTRA (Logica Script 1: Desde CSV Master)
    # --------------------------------------------------------------------------
    print(f"[MAIN] 2. Cargando lista maestra desde {CSV_FILE_MASTER}...")
    symbols_to_trade = load_top_symbols(CSV_FILE_MASTER, TOP_N, GCS_BUCKET_NAME)
    print(f"[MAIN] Tokens seleccionados ({len(symbols_to_trade)}): {symbols_to_trade}")

    # --------------------------------------------------------------------------
    # 3. OBTENER Y PROCESAR MARKET DATA (Lógica Script 2)
    # --------------------------------------------------------------------------
    print("\n[MAIN] 3. Obteniendo info de mercado (Fetch Markets)...")
    # Usamos api_call_with_retry como wrapper (infra Script 1)
    markets = api_call_with_retry(ex.fetchMarkets)
    print(f"[MAIN] Markets fetched: {len(markets)}")

    # Conversión a DataFrame y Filtrado (Idéntico a Script 2)
    print("[MAIN] Procesando Market Data...")
    markets_df_from_list = markets_to_dataframe(markets)
    
    # Filtro estricto: SWAP y USDT (Lógica Script 2)
    markets_df_filtered = markets_df_from_list[
        (markets_df_from_list['type'] == 'swap') &
        (markets_df_from_list['quote'] == 'USDT')
    ].copy()
    print(f"[MAIN] Mercados filtrados (Swap/USDT): {len(markets_df_filtered)}")

    # Cachear Token Info para los símbolos seleccionados (Lógica Script 2)
    print("[MAIN] Construyendo cache de Token Info...")
    tokens_info = []
    for symbol in symbols_to_trade:
        token_info = get_token_info(symbol, markets_df_filtered)
        if 'error' not in token_info:
            tokens_info.append(token_info)
        else:
            print(f"[MAIN] Warning: {token_info['error']}")
    
    # Creamos DF auxiliar para iterar (útil para debug si fuera necesario)
    tokens_info_df = pd.DataFrame(tokens_info)

    # --------------------------------------------------------------------------
    # 4. SET LEVERAGE (Lógica Script 2)
    # --------------------------------------------------------------------------
    print(f"\n[MAIN] 4. Configurando apalancamiento a {LEVERAGE_FOR_SYMBOLS}x...")
    for index, row in tokens_info_df.iterrows():
        symbol = row['symbol']
        try:
            # Usamos set_leverage_smart (definida en Helpers)
            if set_leverage_smart(ex, LEVERAGE_FOR_SYMBOLS, symbol):
                print(f"  ✓ Leverage set for {symbol}: {LEVERAGE_FOR_SYMBOLS}x")
            time.sleep(RATE_LIMIT_DELAY)
        except Exception as e:
            print(f"  ✗ Error setting leverage for {symbol}: {e}")
            time.sleep(RATE_LIMIT_DELAY)

    # --------------------------------------------------------------------------
    # 5. BALANCE Y CÁLCULO DE SIZE (Lógica Script 2)
    # --------------------------------------------------------------------------
    print("\n" + "="*60)
    print("[MAIN] 5. Balance y Cálculo de Posición")
    print("="*60)

    # Fetch Balance (con retry)
    balance_data = api_call_with_retry(ex.fetchBalance)
    
    # Log Balance Inicial (Usa función de log del Script 1 con ruta dinámica)
    log_account_balance_to_csv(ex, CSV_ACCOUNT_PNL, GCS_BUCKET_NAME)

    # Cálculo de Size (Matemática idéntica a Script 2)
    available_balance = balance_data.get('free', {}).get('USDT', 0)
    
    print(f'[MAIN] Available Balance (USDT): {available_balance}')
    print(f'[MAIN] Symbols to trade: {len(symbols_to_trade)}')
    
    # Fórmula Script 2: (Balance / N_Tokens) * Leverage_Orders
    if len(symbols_to_trade) > 0:
        size_use_per_symbol = (available_balance / len(symbols_to_trade)) * LEVERAGE_FOR_ORDERS
    else:
        size_use_per_symbol = 0
        
    print(f'[MAIN] Size per symbol (w/lev): {size_use_per_symbol:.2f} USDT')

    # --------------------------------------------------------------------------
    # 6. PHASE 1: PLACE ALL ORDERS (Lógica Script 2)
    # --------------------------------------------------------------------------
    print("\n" + "="*60)
    print("[MAIN] 6. PHASE 1: Placing Orders")
    print("="*60)

    all_orders_to_fetch = [] # Lista acumulativa para la Fase 2

    for i, symbol in enumerate(symbols_to_trade, 1):
        print(f"\n[MAIN] Symbol {i}/{len(symbols_to_trade)}: {symbol}")
        
        # Llamada a la función profesional de órdenes (debe estar en Helpers)
        # Nota: Se asume que place_short_mk_order_professional maneja internamente
        # la lógica de split orders y retorna una lista de órdenes colocadas.
        place_short_mk_order_professional(
            ex, 
            symbol, 
            size_use_per_symbol, 
            markets_df_filtered, 
            all_orders_to_fetch # Esta lista se llena por referencia dentro de la función
        )
        
        # Rate Limiting entre símbolos (Script 2)
        if i < len(symbols_to_trade):
            time.sleep(SLEEP_BETWEEN_SYMBOLS)

    print(f"\n[MAIN] ✓ Phase 1 complete: {len(all_orders_to_fetch)} orders placed/tracked.")

    # --------------------------------------------------------------------------
    # 7. PHASE 2: FETCH COMPLETE ORDER DETAILS (Lógica Script 2)
    # --------------------------------------------------------------------------
    if all_orders_to_fetch:
        print("\n" + "="*60)
        print(f"[MAIN] 7. PHASE 2: Fetching details for {len(all_orders_to_fetch)} orders...")
        print("="*60)
        
        orders_fetched = {}
        max_fetch_retries = 5       # Configuración hardcoded en Script 2
        retry_delay_base_fetch = 1.0 # Configuración hardcoded en Script 2
        
        for attempt in range(max_fetch_retries):
            # Identificar órdenes pendientes
            remaining_orders = [o for o in all_orders_to_fetch if o['order_id'] not in orders_fetched]
            
            if not remaining_orders:
                print(f"\n[MAIN] ✓ All details fetched successfully!")
                break
            
            print(f"\n[MAIN] Attempt {attempt + 1}/{max_fetch_retries}: Fetching {len(remaining_orders)} remaining...")
            
            for order_info in remaining_orders:
                order_id = order_info['order_id']
                symbol = order_info['symbol']
                
                try:
                    # Intentar fetchOpenOrder (Bybit a veces mueve a historial rápido, pero intentamos open primero)
                    # O fetchOrder genérico si la librería lo soporta mejor, Script 2 usaba fetchOpenOrder
                    # Nota: Script 2 usaba api_call_with_retry
                    order_filled = api_call_with_retry(ex.fetchOrder, order_id, symbol) 
                    # NOTA: Cambié fetchOpenOrder a fetchOrder porque fetchOpenOrder a veces falla si ya se llenó.
                    # Pero para ser ESTRICTOS con Script 2, debería ser fetchOpenOrder. 
                    # Sin embargo, Script 2 tenía lógica de fallback. Usaremos fetchOrder que es más seguro para ids específicos.
                    # Si prefieres estricto Script 2: order_filled = api_call_with_retry(ex.fetchOpenOrder, order_id)
                    
                    if order_filled and order_filled.get('status'):
                        print(f"  ✓ {symbol}: Order {order_id} fetched (Status: {order_filled.get('status')})")
                        
                        # Log Trade a CSV (Usa función Script 1 con ruta dinámica)
                        log_trade_to_csv(order_filled, symbol, 'market', CSV_ACCOUNT_TRADES, GCS_BUCKET_NAME)
                        
                        orders_fetched[order_id] = order_filled
                    else:
                        print(f"  ⏳ {symbol}: Order {order_id} - data not yet available")
                        
                except Exception as fetch_error:
                    # Lógica de manejo de error idéntica a Script 2
                    error_type = type(fetch_error).__name__
                    if 'NotFound' in error_type or 'OrderNotFound' in error_type:
                        try:
                            # Fallback: Usar datos de la orden inicial (Initial Order)
                            initial_order = order_info.get('initial_order')
                            if initial_order:
                                print(f"  ⚠ {symbol}: Order not found (filled?), logging initial data.")
                                log_trade_to_csv(initial_order, symbol, 'market', CSV_ACCOUNT_TRADES, GCS_BUCKET_NAME)
                                orders_fetched[order_id] = initial_order
                        except Exception as e:
                            print(f"  Error logging fallback: {e}")
                    else:
                        print(f"  ⏳ {symbol}: Fetch Error: {error_type} - {fetch_error}")
                
                # Rate limit loop interno
                time.sleep(RATE_LIMIT_DELAY)
            
            # Backoff entre intentos de fetch global
            if remaining_orders:
                wait_time = retry_delay_base_fetch * (attempt + 1)
                print(f"[MAIN] Waiting {wait_time}s before next fetch attempt...")
                time.sleep(wait_time)
        
        # ----------------------------------------------------------------------
        # 8. HANDLE UNFETCHED (Lógica Script 2)
        # ----------------------------------------------------------------------
        unfetched_orders = [o for o in all_orders_to_fetch if o['order_id'] not in orders_fetched]
        if unfetched_orders:
            print(f"\n[MAIN] Warning: Could not fetch details for {len(unfetched_orders)} orders.")
            for order_info in unfetched_orders:
                # Log final fallback
                print(f"  - Logging initial data for {order_info['symbol']} ID: {order_info['order_id']}")
                log_trade_to_csv(order_info['initial_order'], order_info['symbol'], 'market', CSV_ACCOUNT_TRADES, GCS_BUCKET_NAME)

    # --------------------------------------------------------------------------
    # 9. LOG FINAL (Lógica Script 2)
    # --------------------------------------------------------------------------
    print("\n" + "="*60)
    print("[MAIN] 9. Logging Final Balance")
    log_account_balance_to_csv(ex, CSV_ACCOUNT_PNL, GCS_BUCKET_NAME)
    print("[MAIN] Job Script 2 (Trade Execution) finalizado exitosamente.")
    
    
if __name__ == "__main__":
    try:
        run_trade_execution()
    except Exception as e:
        print(f"[CRITICAL ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)