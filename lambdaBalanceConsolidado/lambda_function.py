import boto3
import pandas as pd
from io import StringIO
from datetime import datetime

s3_client = boto3.client('s3')

def get_latest_file(bucket, prefix):
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = response.get('Contents', [])
    if not files:
        raise Exception("No files found in the specified prefix.")
    latest_file = max(files, key=lambda x: x['LastModified'])
    return latest_file['Key']

def lambda_handler(event, context):
    bucket = 'myenergybalnce0978'
    
    try:
        # Obtener el archivo BalanceConsolidado más reciente
        consolidado_prefix = 'BalanceConsolidado/'
        consolidado_key = get_latest_file(bucket, consolidado_prefix)
        consolidado_obj = s3_client.get_object(Bucket=bucket, Key=consolidado_key)
        consolidado_data = consolidado_obj['Body'].read().decode('utf-8')
        
        # Leer el archivo CSV de BalanceConsolidado
        df_consolidado = pd.read_csv(StringIO(consolidado_data))
        
        # Simulando los datos de la tabla Precios Bolsa (puedes reemplazar con una extracción real)
        precios_bolsa_data = {
            'dia': [1, 2, 3],   # Ejemplo de fechas
            'mes': [9, 9, 9],
            'anio': [2024, 2024, 2024],
            'valor': [150, 200, 180]  # Valores de la bolsa
        }
        df_precios_bolsa = pd.DataFrame(precios_bolsa_data)
        
        # Realizar la unión de las tablas BalanceConsolidado y Precios Bolsa
        df_merged = pd.merge(df_consolidado, df_precios_bolsa, on=['anio', 'mes', 'dia'], how='inner')
        
        # Calcular el valor de la energía en miles de millones de pesos
        df_merged['Compromisos_MCOP'] = (df_merged['consolidado_planta'] * df_merged['valor']) / 1000
        
        # Obtener la fecha y hora actual
        now = datetime.now()
        timestamp = now.strftime('%Y%m%d_%H%M%S')
        
        # Guardar el archivo CSV en el prefijo BalanceCompraVentaEnergia/ con la fecha y hora en el nombre
        output_csv = df_merged.to_csv(index=False)
        output_key = f'BalanceCompraVentaEnergia/BalancesComprasVentasEnergia_{timestamp}.csv'
        s3_client.put_object(Body=output_csv, Bucket=bucket, Key=output_key)
        
        return {
            'statusCode': 200,
            'body': f'File saved to {output_key}'
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error processing files: {e}'
        }
