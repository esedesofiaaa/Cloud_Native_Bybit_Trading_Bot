import pandas as pd
from google.cloud import storage
import io
import os

# CONFIGURACIÓN
# Leemos el nombre del bucket de una variable de entorno para flexibilidad
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'bybitcronjobsdata')

def process_blobs():
    # Inicializamos el cliente de GCP
    # (No hacen falta credenciales explícitas si se ejecuta en Cloud Run con la Service Account correcta)
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    print(f"🔍 Escaneando bucket: {BUCKET_NAME}...")

    all_trades = []
    all_pnl = []

    # Listamos TODOS los archivos (blobs) del bucket
    blobs = bucket.list_blobs()

    for blob in blobs:
        # 1. Filtro básico: Solo nos interesan los CSV
        if not blob.name.endswith('.csv'):
            continue

        # 2. Filtro de ubicación: ¿Está en una carpeta?
        # En GCS, las "carpetas" son solo parte del nombre separadas por '/'.
        # Si no hay '/', el archivo está en la raíz y debemos ignorarlo (ej: los del Script 1).
        if '/' not in blob.name:
            print(f"⚠️ Saltando archivo en raíz: {blob.name}")
            continue

        # 3. Extracción de Metadatos
        # blob.name se ve así: "TEST1-10TKN/account_trades.csv"
        parts = blob.name.split('/')
        
        # La primera parte es el nombre de la carpeta (Tu ID de Proyecto)
        project_id = parts[0] 
        # La última parte es el nombre del archivo real
        filename = parts[-1]

        # Evitamos procesar los archivos unificados anteriores si se guardan en carpetas (por seguridad)
        if 'master_' in filename:
            continue

        try:
            # 4. Lectura inteligente (Streaming a Memoria)
            print(f"📥 Leyendo: {project_id} -> {filename}")
            content = blob.download_as_bytes()
            
            # Usamos io.BytesIO para que Pandas crea que es un archivo normal
            df = pd.read_csv(io.BytesIO(content))

            # ---------------------------------------------------------
            # LA MAGIA: Añadimos la columna identificadora
            # ---------------------------------------------------------
            df['project_id'] = project_id

            # 5. Clasificación
            if 'account_trades' in filename or 'trades' in filename:
                all_trades.append(df)
            elif 'account_pnl' in filename or 'pnl' in filename:
                all_pnl.append(df)

        except Exception as e:
            print(f"❌ Error leyendo {blob.name}: {e}")

    # 6. Unificación y Guardado
    save_unified_csv(bucket, all_trades, 'master_trades_unificado.csv')
    save_unified_csv(bucket, all_pnl, 'master_pnl_unificado.csv')

def save_unified_csv(bucket, df_list, output_filename):
    if not df_list:
        print(f"⚠️ No se encontraron datos para {output_filename}")
        return

    # Concatenar todos los dataframes
    master_df = pd.concat(df_list, ignore_index=True)
    
    # Convertir a CSV (string buffer)
    output_buffer = master_df.to_csv(index=False)
    
    # Subir a la RAÍZ del bucket
    new_blob = bucket.blob(output_filename)
    new_blob.upload_from_string(output_buffer, content_type='text/csv')
    
    print(f"✅ GENERADO EXITOSAMENTE: {output_filename}")
    print(f"   📊 Total filas: {len(master_df)}")
    print(f"   ☁️  Ubicación: gs://{BUCKET_NAME}/{output_filename}")

if __name__ == "__main__":
    process_blobs()