import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# CONFIGURACIÓN DE ESTILO
sns.set_theme(style="whitegrid")
plt.rcParams['figure.figsize'] = [12, 6]

try:
    # 1. CARGAR DATOS
    print("📂 Leyendo archivos maestros...")
    df_pnl = pd.read_csv('master_pnl_unificado.csv')
    df_trades = pd.read_csv('master_trades_unificado.csv')

    # 2. PREPARAR DATOS DE PNL (BALANCE)
    # Convertir fecha
    df_pnl['timestamp'] = pd.to_datetime(df_pnl['timestamp'])
    
    # Manejar columnas de monedas (rellenar NaN con 0 para sumar)
    cols_to_fix = ['total_USDT', 'total_USDC']
    for col in cols_to_fix:
        if col not in df_pnl.columns:
            df_pnl[col] = 0
        else:
            df_pnl[col] = df_pnl[col].fillna(0)

    # Crear una columna de "Valor Total USD" (USDT + USDC)
    df_pnl['total_balance'] = df_pnl['total_USDT'] + df_pnl['total_USDC']

    # Ordenar por fecha para los gráficos de línea
    df_pnl = df_pnl.sort_values('timestamp')

    # 3. GENERAR GRÁFICOS

    # --- GRÁFICO 1: Evolución del Balance en el Tiempo ---
    print("📊 Generando Gráfico de Evolución...")
    plt.figure()
    sns.lineplot(data=df_pnl, x='timestamp', y='total_balance', hue='project_id', marker="o")
    plt.title('Evolución del Balance Total (USDT + USDC) por Proyecto')
    plt.xlabel('Fecha/Hora')
    plt.ylabel('Balance Total (USD)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('grafico_1_evolucion_balance.png')
    print("   -> Guardado: grafico_1_evolucion_balance.png")

    # --- GRÁFICO 2: Comparativa Final de Ganancia/Pérdida ---
    # Tomamos el último registro de cada proyecto
    last_records = df_pnl.sort_values('timestamp').groupby('project_id').tail(1)
    
    # Calcular ROI asumiendo base de 10k o 50k (Opcional: solo mostramos balance final por ahora)
    print("📊 Generando Gráfico de Barras Final...")
    plt.figure()
    barplot = sns.barplot(data=last_records, x='project_id', y='total_balance', hue='project_id', palette='viridis', legend=False)
    plt.title('Balance Final Actual por Credencial/Región')
    plt.ylabel('Total USD')
    plt.xticks(rotation=45)
    
    # Añadir etiquetas con el valor encima de las barras
    for p in barplot.patches:
        barplot.annotate(f'${p.get_height():,.0f}', 
                        (p.get_x() + p.get_width() / 2., p.get_height()), 
                        ha = 'center', va = 'center', 
                        xytext = (0, 9), 
                        textcoords = 'offset points')
    
    plt.tight_layout()
    plt.savefig('grafico_2_balance_final.png')
    print("   -> Guardado: grafico_2_balance_final.png")

    # --- GRÁFICO 3: Volumen de Actividad (Trades) ---
    print("📊 Generando Gráfico de Actividad de Trading...")
    if not df_trades.empty:
        # Contar cuántos trades hizo cada proyecto
        trades_count = df_trades['project_id'].value_counts().reset_index()
        trades_count.columns = ['project_id', 'num_trades']

        plt.figure()
        sns.barplot(data=trades_count, x='project_id', y='num_trades', hue='project_id', palette='rocket', legend=False)
        plt.title('Número Total de Operaciones (Trades) Ejecutadas')
        plt.ylabel('Cantidad de Trades')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig('grafico_3_actividad_trades.png')
        print("   -> Guardado: grafico_3_actividad_trades.png")
    else:
        print("   ⚠️ No hay datos de trades para graficar.")

    print("\n✅ ¡Listo! Revisa las 3 imágenes PNG generadas en tu carpeta.")

except Exception as e:
    print(f"\n❌ Ocurrió un error: {e}")
    print("Asegúrate de tener instaladas las librerías: pip install matplotlib seaborn")