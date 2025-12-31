import pandas as pd
import ccxt
import time
import os
import sys
from datetime import datetime, timezone
import json
import math
from google.cloud import secretmanager
from google.cloud import storage
from io import StringIO
from google.cloud import secretmanager

# ==============================================================================
# CONFIGURACIÓN Y VARIABLES DE ENTORNO
# ==============================================================================

# 1. Identificación del Proyecto y Secretos
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
SECRET_NAME = os.environ.get('SECRET_NAME')

# 2. Configuración del Experimento
TEST_ID = os.environ.get('TEST_ID', 'default_test')

# 3. Configuración de Storage (Rutas Dinámicas)
GCS_BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME', 'bybitcronjobsdata')
CSV_ACCOUNT_PNL = f"{TEST_ID}/account_pnl.csv"
CSV_ACCOUNT_TRADES = f"{TEST_ID}/account_trades.csv"
CSV_ACCOUNT_POSITIONS = f"{TEST_ID}/account_positions_final.csv" # Opcional si lo usas

# 4. Configuración
SCRIPT_NAME_SHORT = 'Close_Trades'
SLEEP_BETWEEN_ORDERS = 0.5
MAX_ITERATIONS = 100

def get_secret(project_id, secret_id):
    """Obtiene API Key y Secret desde Secret Manager (JSON)"""
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        payload = response.payload.data.decode("UTF-8")
        creds = json.loads(payload)
        return creds.get('api_key'), creds.get('api_secret')
    except Exception as e:
        print(f"[get_secret] Error crítico: {e}")
        raise

#FUNCTION TO DECLARE EXCHANGE
def boot_exchange_demo(api_key, api_secret):
    """Initialize Bybit demo/testnet exchange con credenciales dinámicas"""
    config = {
        "enableRateLimit": True,
        "options": {"defaultType": "future"},
        "apiKey": api_key,
        "secret": api_secret
    }
    ex = ccxt.bybit(config)
    ex.enable_demo_trading(True)
    return ex

## CSV LOGGING FUNCTIONS

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

def log_account_balance_to_csv(exchange, csv_file: str = CSV_ACCOUNT_PNL, bucket_name: str = GCS_BUCKET_NAME):
    """
    Fetch account balance and log to CSV file in Google Cloud Storage.
    Creates file with headers if it doesn't exist, appends if it does.
    Always preserves existing data by reading, appending, then writing back.
    """
    try:
        # Fetch balance
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
            'test_id': TEST_ID
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
        print(f"[log_trade] Logged trade record to {gcs_path} for {symbol}")
            
        return record
        
    except Exception as e:
        print(f"[log_trade] Error logging trade: {str(e)}")
        print(f"[log_trade] Order data: {order_data}")
        import traceback
        traceback.print_exc()
        return None



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



def run_close_trades():
    print(f"[MAIN] Iniciando Job Script 3 (Close Trades) - {datetime.now(timezone.utc)}")
    print(f"[CONFIG] Project: {GCP_PROJECT_ID}, Secret: {SECRET_NAME}, TestID: {TEST_ID}")

    if not GCP_PROJECT_ID or not SECRET_NAME:
        raise ValueError("Faltan variables de entorno GCP_PROJECT_ID o SECRET_NAME")

    # 1. Obtener Credenciales
    print("[MAIN] Obteniendo credenciales...")
    api_key, api_secret = get_secret(GCP_PROJECT_ID, SECRET_NAME)

    # 2. Iniciar Exchange con credenciales
    ex = boot_exchange_demo(api_key, api_secret)

    markets = ex.fetchMarkets()

    ## CREATE MARKETS DATAFRAME
    markets_df_from_list = markets_to_dataframe(markets)
    markets_df_filtered = markets_df_from_list[
        (markets_df_from_list['type'] == 'swap') &
        (markets_df_from_list['quote'] == 'USDT')
    ].copy()

    print(f"[CLOSE_TRADES] Filtered markets: {markets_df_filtered.shape} (swap + USDT)")

    ## Log account balance at the beginning
    print("\n" + "="*80)
    print("[CLOSE_TRADES] Starting professional position closing process...")
    print("="*80)
    print(f"\n[CLOSE_TRADES] Logging initial account balance...")
    log_account_balance_to_csv(ex, CSV_ACCOUNT_PNL)

    ## Get all current open positions
    print(f"\n[CLOSE_TRADES] Fetching open positions...")
    open_positions = ex.fetchPositions()

    # CONVERT open_positions INTO DATAFRAME
    open_positions_df = pd.DataFrame(open_positions)

    # Filter only positions with non-zero contracts
    if open_positions_df.empty or len(open_positions_df.columns) == 0:
        print("\n[CLOSE_TRADES] No open positions found (empty DataFrame). Nothing to close.")
        open_positions_df = pd.DataFrame()
    else:
        # Check which column name is used for contracts
        contract_column = None
        for col_name in ['contracts', 'contract', 'size', 'amount', 'positionSize']:
            if col_name in open_positions_df.columns:
                contract_column = col_name
                break
        
        if contract_column is None:
            print(f"\n[CLOSE_TRADES] Warning: Could not find contracts column. Available columns: {list(open_positions_df.columns)}")
            print("[CLOSE_TRADES] No positions to close.")
            open_positions_df = pd.DataFrame()
        else:
            open_positions_df = open_positions_df[open_positions_df[contract_column] != 0].copy()
            
            if open_positions_df.empty:
                print("\n[CLOSE_TRADES] No open positions found. Nothing to close.")
            else:
                print(f"\n[CLOSE_TRADES] Found {len(open_positions_df)} open positions:")
                # Show available columns
                available_cols = ['symbol', contract_column]
                for col in ['side', 'unrealizedPnl', 'positionSide']:
                    if col in open_positions_df.columns:
                        available_cols.append(col)
                print(open_positions_df[available_cols].to_string())

    ### CLOSE ALL POSITIONS PROFESSIONALLY - Loop until all positions are closed

    if not open_positions_df.empty:
        print("\n" + "="*80)
        print("[CLOSE_TRADES] Starting to close positions...")
        print("="*80)
        
        # Store ALL order IDs and symbols to fetch details AFTER all positions are closed
        all_orders_to_fetch = []
        
        iteration = 0
        all_closed = False
        
        while not all_closed and iteration < MAX_ITERATIONS:
            iteration += 1
            print(f"\n[CLOSE_TRADES] Iteration {iteration} - Checking for open positions...")
            
            # Fetch current positions
            try:
                current_positions = ex.fetchPositions()
                current_positions_df = pd.DataFrame(current_positions)
                
                # Check if DataFrame is empty or has no columns
                if current_positions_df.empty or len(current_positions_df.columns) == 0:
                    print("[CLOSE_TRADES] No positions found (empty DataFrame).")
                    all_closed = True
                    break
                
                # Check which column name is used for contracts
                contract_column = None
                for col_name in ['contracts', 'contract', 'size', 'amount', 'positionSize']:
                    if col_name in current_positions_df.columns:
                        contract_column = col_name
                        break
                
                if contract_column is None:
                    print(f"[CLOSE_TRADES] Warning: Could not find contracts column. Available columns: {list(current_positions_df.columns)}")
                    print("[CLOSE_TRADES] Assuming no positions to close.")
                    all_closed = True
                    break
                
                # Filter only positions with non-zero contracts
                current_positions_df = current_positions_df[current_positions_df[contract_column] != 0].copy()
                
                if current_positions_df.empty:
                    print("[CLOSE_TRADES] All positions have been closed!")
                    all_closed = True
                    break
                    
            except Exception as fetch_error:
                print(f"[CLOSE_TRADES] Error fetching positions: {fetch_error}")
                print("[CLOSE_TRADES] Retrying in next iteration...")
                time.sleep(SLEEP_BETWEEN_ORDERS * 2)
                continue
            
            print(f"[CLOSE_TRADES] Found {len(current_positions_df)} positions still open")
            
            # PHASE 1: Close all positions professionally (respecting max order size)
            print(f"\n[CLOSE_TRADES] PHASE 1: Closing all positions (respecting max order size)...")
            
            for index, row in current_positions_df.iterrows():
                # Get symbol
                symbol = None
                for col_name in ['symbol', 'info']:
                    if col_name in row.index:
                        if col_name == 'info' and isinstance(row[col_name], dict):
                            symbol = row[col_name].get('symbol')
                        else:
                            symbol = row[col_name]
                        break
                
                if symbol is None:
                    print(f"[CLOSE_TRADES] Warning: Could not find symbol for row {index}. Skipping.")
                    continue
                
                # Get contracts amount
                contracts = abs(float(row[contract_column]))
                
                # Get position side
                position_side = None
                for col_name in ['side', 'positionSide', 'position']:
                    if col_name in row.index:
                        position_side = str(row[col_name]).lower()
                        break
                
                if position_side is None:
                    position_side = 'short'
                    print(f"[CLOSE_TRADES] Warning: Could not determine position side for {symbol}, defaulting to 'short'")
                
                # Determine closing side
                if position_side == 'short':
                    close_side = 'buy'
                elif position_side == 'long':
                    close_side = 'sell'
                else:
                    close_side = 'buy'
                    print(f"[CLOSE_TRADES] Warning: Unknown position side '{position_side}' for {symbol}, using 'buy'")
                
                print(f"\n[CLOSE_TRADES] Closing {position_side} position: {symbol}, {contracts} contracts (side: {close_side})")
                
                try:
                    # Get token info to check max market order quantity
                    token_info = get_token_info(symbol, markets_df_filtered)
                    max_market_order_quantity = token_info.get('info_lotsizefilter_maxmktorderqty')
                    
                    # Handle None or NaN values - use max_order_qty as fallback, then limits_amount_max
                    if max_market_order_quantity is None or pd.isna(max_market_order_quantity):
                        max_market_order_quantity = token_info.get('info_lotsizefilter_maxorderqty')
                    if max_market_order_quantity is None or pd.isna(max_market_order_quantity):
                        max_market_order_quantity = token_info.get('limits_amount_max', float('inf'))
                    if max_market_order_quantity is None or pd.isna(max_market_order_quantity):
                        max_market_order_quantity = float('inf')  # No limit if not available
                    
                    print(f"  Max market order quantity: {max_market_order_quantity}")
                    
                    # Check if we need to split the order
                    if contracts > max_market_order_quantity:
                        # Split into multiple orders
                        num_parts = int(math.ceil(contracts / max_market_order_quantity))
                        contracts_per_part = contracts / num_parts
                        
                        print(f"  Contract size ({contracts}) exceeds max order size ({max_market_order_quantity})")
                        print(f"  Splitting into {num_parts} orders of ~{contracts_per_part:.2f} contracts each")
                        print(f"  ----------------------------------------")
                        
                        # Place multiple orders
                        for part_num in range(num_parts):
                            try:
                                order = ex.create_market_order(
                                    symbol=symbol,
                                    side=close_side,
                                    amount=contracts_per_part,
                                    params={'reduceOnly': True}
                                )
                                
                                order_id = order.get('id')
                                print(f"  ✓ Order {part_num + 1}/{num_parts} executed for {symbol} - Order ID: {order_id}")
                                
                                # Store order ID for later fetching
                                all_orders_to_fetch.append({
                                    'order_id': order_id,
                                    'symbol': symbol,
                                    'initial_order': order
                                })
                                
                                # Minimal delay for rate limiting
                                time.sleep(SLEEP_BETWEEN_ORDERS * 0.1)
                                
                            except Exception as part_error:
                                print(f"  ✗ Error placing part {part_num + 1} for {symbol}: {str(part_error)}")
                                time.sleep(SLEEP_BETWEEN_ORDERS * 0.1)
                        
                        print(f"  ----------------------------------------")
                        
                    else:
                        # Single order - contract size is within limits
                        print(f"  Contract size within limits, placing single order")
                        
                        order = ex.create_market_order(
                            symbol=symbol,
                            side=close_side,
                            amount=contracts,
                            params={'reduceOnly': True}
                        )
                        
                        order_id = order.get('id')
                        print(f"  ✓ Order executed for {symbol} - Order ID: {order_id}")
                        
                        # Store order ID for later fetching
                        all_orders_to_fetch.append({
                            'order_id': order_id,
                            'symbol': symbol,
                            'initial_order': order
                        })
                        
                        # Minimal delay for rate limiting
                        time.sleep(SLEEP_BETWEEN_ORDERS * 0.1)
                    
                except Exception as e:
                    print(f"[CLOSE_TRADES] ✗ Error closing {symbol}: {str(e)}")
                    time.sleep(SLEEP_BETWEEN_ORDERS * 0.1)
            
            if not all_closed:
                print(f"\n[CLOSE_TRADES] Waiting before next iteration...")
                time.sleep(SLEEP_BETWEEN_ORDERS * 2)
        
        # PHASE 2: Fetch order details for ALL closed orders (after all positions are closed)
        if all_orders_to_fetch:
            print(f"\n" + "="*80)
            print(f"[CLOSE_TRADES] PHASE 2: Fetching order details for {len(all_orders_to_fetch)} orders...")
            print(f"[CLOSE_TRADES] All positions have been closed. Now fetching order information.")
            print("="*80)
            
            # Keep track of which orders we've successfully fetched
            orders_fetched = {}
            max_retries = 10
            retry_delay_base = 2  # Base delay in seconds
            
            for attempt in range(max_retries):
                remaining_orders = [o for o in all_orders_to_fetch if o['order_id'] not in orders_fetched]
                
                if not remaining_orders:
                    print(f"\n[CLOSE_TRADES] ✓ Successfully fetched all {len(orders_fetched)} order details!")
                    break
                
                print(f"\n[CLOSE_TRADES] Attempt {attempt + 1}/{max_retries}: Fetching {len(remaining_orders)} remaining orders...")
                
                for order_info in remaining_orders:
                    order_id = order_info['order_id']
                    symbol = order_info['symbol']
                    
                    try:
                        order_filled = ex.fetchClosedOrder(order_id, symbol)
                        
                        # Verify we got valid data
                        if order_filled and order_filled.get('status'):
                            print(f"  ✓ {symbol}: Order {order_id} fetched successfully")
                            
                            # Display order details
                            print(f"    Status: {order_filled.get('status', 'N/A')}")
                            print(f"    Filled: {order_filled.get('filled', 0)}")
                            print(f"    Average Price: {order_filled.get('average', 'N/A')}")
                            print(f"    Cost: {order_filled.get('cost', 'N/A')}")
                            
                            # Extract additional info if available
                            if 'info' in order_filled:
                                info = order_filled['info']
                                if isinstance(info, dict):
                                    print(f"    Order ID (exchange): {info.get('orderId', 'N/A')}")
                                    print(f"    Order Status: {info.get('orderStatus', 'N/A')}")
                                    print(f"    Cum Exec Qty: {info.get('cumExecQty', 'N/A')}")
                                    print(f"    Cum Exec Value: {info.get('cumExecValue', 'N/A')}")
                            
                            # Log trade to CSV with complete order information
                            log_trade_to_csv(order_filled, symbol, 'market')
                            
                            # Mark as successfully fetched
                            orders_fetched[order_id] = order_filled
                        else:
                            print(f"  ⏳ {symbol}: Order {order_id} - data not yet available (empty response)")
                            
                    except Exception as fetch_error:
                        print(f"  ⏳ {symbol}: Order {order_id} - waiting for data: {type(fetch_error).__name__}")
                    
                    # Small delay between fetches
                    time.sleep(SLEEP_BETWEEN_ORDERS)
                
                # Wait longer between retry attempts (exponential backoff)
                if remaining_orders:
                    wait_time = retry_delay_base * (attempt + 1)
                    print(f"\n[CLOSE_TRADES] Waiting {wait_time} seconds before next attempt...")
                    time.sleep(wait_time)
            
            # Handle any orders that couldn't be fetched
            unfetched_orders = [o for o in all_orders_to_fetch if o['order_id'] not in orders_fetched]
            if unfetched_orders:
                print(f"\n[CLOSE_TRADES] Warning: Could not fetch details for {len(unfetched_orders)} orders after {max_retries} attempts")
                for order_info in unfetched_orders:
                    print(f"  - {order_info['symbol']}: Order ID {order_info['order_id']}")
                    # Log initial order data as fallback
                    print(f"    Logging initial order data (complete details unavailable)")
                    log_trade_to_csv(order_info['initial_order'], order_info['symbol'], 'market')
        
        if iteration >= MAX_ITERATIONS:
            print(f"\n[CLOSE_TRADES] Warning: Reached maximum iterations ({MAX_ITERATIONS}). Some positions may still be open.")
            
            # Final check
            try:
                final_positions = ex.fetchPositions()
                final_positions_df = pd.DataFrame(final_positions)
                
                if not final_positions_df.empty and len(final_positions_df.columns) > 0:
                    # Find contract column
                    contract_column = None
                    for col_name in ['contracts', 'contract', 'size', 'amount', 'positionSize']:
                        if col_name in final_positions_df.columns:
                            contract_column = col_name
                            break
                    
                    if contract_column:
                        final_positions_df = final_positions_df[final_positions_df[contract_column] != 0].copy()
                        
                        if not final_positions_df.empty:
                            print(f"[CLOSE_TRADES] Remaining open positions:")
                            available_cols = ['symbol', contract_column]
                            for col in ['side', 'unrealizedPnl', 'positionSide']:
                                if col in final_positions_df.columns:
                                    available_cols.append(col)
                            print(final_positions_df[available_cols].to_string())
                        else:
                            print("[CLOSE_TRADES] All positions successfully closed!")
                    else:
                        print(f"[CLOSE_TRADES] Could not determine contract column. Available columns: {list(final_positions_df.columns)}")
                else:
                    print("[CLOSE_TRADES] All positions successfully closed!")
            except Exception as final_check_error:
                print(f"[CLOSE_TRADES] Error in final position check: {final_check_error}")

        ## Log final account balance
        print("\n" + "="*80)
        print("[CLOSE_TRADES] Logging final account balance...")
        
        print("\n[CLOSE_TRADES] Professional position closing process completed!")
        print("="*80)
        #Wait for 1 second
        time.sleep(10)
        log_account_balance_to_csv(ex, CSV_ACCOUNT_PNL, GCS_BUCKET_NAME)
        print(f"[CLOSE_TRADES] Final account balance logged to {GCS_BUCKET_NAME}/{CSV_ACCOUNT_PNL}")



if __name__ == "__main__":
    try:
        run_close_trades()
    except Exception as e:
        print(f"[CRITICAL ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)