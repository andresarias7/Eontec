import boto3
import pandas as pd
from io import StringIO

s3 = boto3.client('s3')
bucket = 'myenergybalnce0978'
prefix = 'FilteredEnergyDispatch/'

def lambda_handler(event, context):
    # Listar los archivos en el bucket y prefijo especificados
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    # Extraer los archivos disponibles
    files = response.get('Contents', [])
    if not files:
        return {'statusCode': 404, 'body': 'No files found in the S3 bucket.'}

    # Encontrar el archivo más reciente
    latest_file = max(files, key=lambda x: x['LastModified'])['Key']

    # Descargar el archivo más reciente
    obj = s3.get_object(Bucket=bucket, Key=latest_file)
    data = obj['Body'].read().decode('utf-8')

    # Cargar los datos en un DataFrame de Pandas
    df = pd.read_csv(StringIO(data))

    # Realizar las transformaciones necesarias
    df_transformed = transform_data(df)

    # Guardar el DataFrame transformado en otro bucket o carpeta
    save_transformed_data(df_transformed)

    return {'statusCode': 200, 'body': f'Successfully processed and saved file: {latest_file}'}

def transform_data(df):
    # Extraer los atributos día, mes, año, hora y valor junto al código de planta
    df['FechaHora'] = pd.to_datetime(df['FechaHora'])  # Asegurarse de que sea datetime
    df_transformed = pd.DataFrame({
        'dia_despacho': df['FechaHora'].dt.day,
        'mes_despacho': df['FechaHora'].dt.month,
        'anio_despacho': df['FechaHora'].dt.year,
        'hora_despacho': df['FechaHora'].dt.hour,
        'capacidad': df['Valor'],
        'codigo': df['CodigoPlanta']
    })
    return df_transformed

def save_transformed_data(df_transformed):
    # Guardar el DataFrame como un archivo CSV en otro prefix
    output_prefix = 'TransformerGeneratorDispatch/DespachosAcmeTransformado.csv'
    
    # Convertir el DataFrame a CSV en memoria
    csv_buffer = StringIO()
    df_transformed.to_csv(csv_buffer, index=False)

    # Subir el CSV transformado a S3
    s3.put_object(Bucket=bucket, Key=output_prefix, Body=csv_buffer.getvalue())
