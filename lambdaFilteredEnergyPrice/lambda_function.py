import json
import boto3
import pandas as pd
from io import StringIO
import numpy as np

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Obtener información del evento de S3
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # Descargar el archivo JSON desde S3
    response = s3.get_object(Bucket=bucket, Key=key)
    data = json.loads(response['Body'].read())
    
    # Cargar los datos en un DataFrame de pandas
    df = pd.json_normalize(data['result']['records'])
    
    # Filtrar y seleccionar columnas
    filtered_df = df[['CodigoDuracion', 'CodigoPlanta', 'FechaHora', 'Valor']]
    
    # Convertir el DataFrame filtrado a CSV
    csv_buffer = StringIO()
    filtered_df.to_csv(csv_buffer, index=False)
    
    # Opcional: Guardar el CSV filtrado en S3
    new_key = key.replace('EnergyPrice/', 'FilteredEnergyPrice/').replace('.json', '.csv')
    s3.put_object(Bucket=bucket, Key=new_key, Body=csv_buffer.getvalue(), ContentType='text/csv')
    
    return {
        'statusCode': 200,
        'body': json.dumps('Data processed and saved as CSV successfully!')
    }
