import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

# Configuración del cliente de S3
BUCKET_NAME = "myenergybalnce0978"
SOURCE_PREFIX = "EnergyDispatch/"
DEST_PREFIX = "FilteredEnergyDispatch/"
s3 = boto3.client('s3')

def get_latest_file(s3_client, bucket, prefix):
    """
    Obtiene el archivo más reciente en el prefijo dado.
    """
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if 'Contents' not in response:
        raise RuntimeError(f"No files found in the prefix {prefix}")
    
    files = response['Contents']
    latest_file = max(files, key=lambda x: x['LastModified'])
    return latest_file['Key']

def lambda_handler(event, context):
    """
    Función Lambda para leer el archivo JSON más reciente desde S3, filtrar los datos y guardar el resultado en S3 como un archivo CSV.
    """
    try:
        # Obtener el archivo más reciente en el prefijo EnergyDispatch/
        latest_file_key = get_latest_file(s3, BUCKET_NAME, SOURCE_PREFIX)
        
        # Leer el archivo JSON desde S3
        response = s3.get_object(Bucket=BUCKET_NAME, Key=latest_file_key)
        data = json.loads(response['Body'].read().decode('utf-8'))
        
        # Convertir los datos a un DataFrame de Pandas
        records = data['result']['records']
        df = pd.DataFrame(records)
        
        # Filtrar los datos
        filtered_df = df[
            df['CodigoPlanta'].isin(['ZPA2', 'ZPA3', 'ZPA4', 'ZPA5', 'GVIO', 'QUI1', 'CHVR'])
        ].sort_values(by=['Valor', 'FechaHora'], ascending=[True, False])
        
        # Crear un archivo CSV a partir del DataFrame filtrado
        csv_buffer = StringIO()
        filtered_df.to_csv(csv_buffer, index=False)
        
        # Guardar el archivo CSV en S3
        file_name = f"filtered-data-{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
        s3_key = f"{DEST_PREFIX}{file_name}"
        
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps(f"Data successfully filtered and saved to {BUCKET_NAME}/{s3_key}")
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error processing file: {str(e)}")
        }
