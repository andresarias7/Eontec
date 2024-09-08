import boto3
import pandas as pd
from io import BytesIO

# Inicializa el cliente de S3
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Nombre del bucket y prefijo
    bucket_name = 'myenergybalance0978'
    prefix = 'GeneratorDispatch/'

    # Listar objetos en el prefijo dado
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    # Verificar si hay archivos en el prefijo
    if 'Contents' not in response:
        return {'statusCode': 404, 'body': 'No hay archivos en el prefijo.'}
    
    # Filtrar archivos y ordenarlos por la fecha de última modificación (descendente)
    files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
    
    # Obtener el archivo más reciente
    latest_file = files[0]['Key']
    print(f"Archivo más reciente encontrado: {latest_file}")

    # Descargar el archivo más reciente desde S3
    file_obj = s3_client.get_object(Bucket=bucket_name, Key=latest_file)
    file_content = file_obj['Body'].read()
    
    # Leer el archivo Excel en un DataFrame de Pandas
    df = pd.read_excel(BytesIO(file_content), engine='openpyxl')

    # Filtrar las filas que no son vacías o nulas, y donde 'GENERADOR' no está vacío o no es "NaN"
    df_filtered = df[df['GENERADOR'].notna() & df['GENERADOR'].ne("NaN") & df['GENERADOR'].ne("GENERADOR")]

    # Extraer el día, el mes, el año y la hora con expresiones regulares
    df_filtered['AÑO'] = df_filtered['FECHA'].str.extract(r'(\d{4})')
    df_filtered['MES'] = df_filtered['FECHA'].str.extract(r'-(\d{2})-')
    df_filtered['DÍA'] = df_filtered['FECHA'].str.extract(r'-(\d{2}) ')
    df_filtered['HORA'] = df_filtered['FECHA'].str.extract(r' (\d{2}):')
    
    # Seleccionar columnas necesarias
    df_result = df_filtered[['AÑO', 'MES', 'DÍA', 'HORA', 'CODIGO', 'CAPACIDAD (Kwh)']]

    # Guardar el DataFrame procesado en S3
    output_buffer = BytesIO()
    df_result.to_excel(output_buffer, index=False)
    output_buffer.seek(0)
    
    # Nombre del archivo de salida
    output_key = f"FilteredGeneratorDispatch/ArchivoCapacidadLimpiado_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    # Subir el archivo a S3
    s3_client.put_object(Bucket=bucket_name, Key=output_key, Body=output_buffer.getvalue())
    
    return {
        'statusCode': 200,
        'body': f"Archivo procesado y guardado como {output_key} en {bucket_name}."
    }
