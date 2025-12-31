import pandas as pd
import os
from pathlib import Path

# CONFIGURACIÓN
# Carpeta donde tienes tus subcarpetas de proyectos
root_folder = 'gcp_data' 

all_trades = []
all_pnl = []

# Recorremos todas las carpetas y archivos
path = Path(root_folder)
print(f"Buscando archivos en: {path.absolute()}")

for file_path in path.rglob('*.csv'):
    # El nombre de la carpeta padre será nuestro ID de proyecto (ej: 'proyecto_usa')
    project_name = file_path.parent.name
    filename = file_path.name
    
    try:
        df = pd.read_csv(file_path)
        # ---------------------------------------------------------
        # AQUÍ ESTÁ LA MAGIA: Añadimos la columna identificadora
        # ---------------------------------------------------------
        df['project_id'] = project_name
        
        if 'trades' in filename:
            all_trades.append(df)
            print(f"✅ Cargado TRADES de: {project_name}")
        elif 'pnl' in filename:
            all_pnl.append(df)
            print(f"✅ Cargado PNL    de: {project_name}")
            
    except Exception as e:
        print(f"❌ Error leyendo {file_path}: {e}")

# Unificamos todo en dos grandes DataFrames
if all_trades:
    master_trades = pd.concat(all_trades, ignore_index=True)
    master_trades.to_csv('master_trades_unificado.csv', index=False)
    print(f"\nGenerado 'master_trades_unificado.csv' con {len(master_trades)} filas.")
else:
    print("No se encontraron archivos de trades.")

if all_pnl:
    master_pnl = pd.concat(all_pnl, ignore_index=True)
    master_pnl.to_csv('master_pnl_unificado.csv', index=False)
    print(f"Generado 'master_pnl_unificado.csv' con {len(master_pnl)} filas.")

# Ejemplo de análisis rápido
if not master_pnl.empty:
    print("\n--- Resumen de PNL por Proyecto (Último registro) ---")
    # Agrupamos por proyecto y tomamos el último valor registrado
    summary = master_pnl.groupby('project_id').last()[['total_USDT', 'total_USDC']]
    print(summary)