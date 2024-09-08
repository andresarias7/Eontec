import json
import requests
import boto3
from datetime import datetime, timedelta

# Configurar las fechas de inicio y fin automáticamente al día anterior a la fecha actual del sistema
fecha_inicio = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
fecha_fin = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

# Nombre del bucket de S3 y cliente de S3
BUCKET_NAME = "myenergybalnce0978"
s3 = boto3.client('s3')

def fetch_data_from_api(api_url):
    """
    Hace una solicitud GET a la API y devuelve los datos en formato JSON.
    """
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Lanza una excepción para códigos de estado HTTP de error
        return response.json()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Error fetching data from API: {str(e)}")

def save_data_to_s3(data, prefix):
    """
    Guarda los datos en un archivo JSON en un bucket de S3 bajo el prefijo proporcionado.
    """
    file_name = f"data-{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"
    s3_key = f"{prefix}{file_name}"
    
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(data),
            ContentType='application/json'
        )
        return s3_key
    except Exception as e:
        raise RuntimeError(f"Error uploading data to S3: {str(e)}")

def lambda_handler(event, context):
    """
    Función Lambda para obtener la información de despacho de energía y guardarla en S3.
    """
    api_url = f"https://www.simem.co/backend-files/api/PublicData?startDate={fecha_inicio}&endDate={fecha_fin}&datasetId=ff027b"
    prefix = "EnergyDispatch/"

    try:
        data = fetch_data_from_api(api_url)
        s3_key = save_data_to_s3(data, prefix)
        return {
            'statusCode': 200,
            'body': json.dumps(f"Data successfully saved to {BUCKET_NAME}/{s3_key}")
        }
    except RuntimeError as e:
        return {
            'statusCode': 500,
            'body': json.dumps(str(e))
        }
