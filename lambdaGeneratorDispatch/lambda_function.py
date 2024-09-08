import boto3
import requests
from datetime import datetime
import io

# Nombre del bucket de S3 y cliente de S3
BUCKET_NAME = "myenergybalnce0978"
s3 = boto3.client('s3')

def download_file_from_google_drive(file_id):
    """
    Descarga un archivo desde Google Drive dado un ID de archivo y lo devuelve en formato binario.
    """
    url = f"https://docs.google.com/uc?export=download&id={file_id}"
    response = requests.get(url, stream=True)
    
    if response.status_code == 200:
        return io.BytesIO(response.content)
    else:
        raise RuntimeError(f"Error downloading file from Google Drive: HTTP {response.status_code}")

def save_data_to_s3(file_stream, prefix):
    """
    Guarda el archivo en un bucket de S3 bajo el prefijo proporcionado.
    """
    file_name = f"DespachoGeneradoraX_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.xlsx"
    s3_key = f"{prefix}{file_name}"
    
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=file_stream,
            ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        return s3_key
    except Exception as e:
        raise RuntimeError(f"Error uploading file to S3: {str(e)}")

def lambda_handler(event, context):
    """
    Función Lambda para descargar el archivo desde Google Drive y guardarlo en S3.
    """
    file_id = "1KNdluJaAWzDQFjqprd-qEChnmho5zyno"  # ID del archivo de Google Drive
    prefix = "GeneratorDispatch/"

    try:
        file_stream = download_file_from_google_drive(file_id)
        s3_key = save_data_to_s3(file_stream, prefix)
        return {
            'statusCode': 200,
            'body': f"File successfully saved to {BUCKET_NAME}/{s3_key}"
        }
    except RuntimeError as e:
        return {
            'statusCode': 500,
            'body': str(e)
        }
