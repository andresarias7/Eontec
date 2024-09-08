# Eontec

Integración y Automatización de Reportes Energéticos
Introducción
Este proyecto está diseñado para automatizar el proceso de recopilación, verificación, consolidación y revisión de datos para el balance energético diario, abordando la problemática de Juan, un analista de mercado energético. Mediante una serie de Lambdas desplegadas en AWS, se facilita la conexión con la API de XM para extraer datos, procesarlos, y generar reportes automáticamente, minimizando el riesgo de errores y optimizando el flujo de trabajo.

La solución mejora la eficiencia, reduce la carga manual y asegura que los reportes de balance energético sean entregados de manera oportuna y precisa.

Características
Extracción de datos desde XM: Conexión a la API de XM mediante AWS Lambda.
Procesamiento de datos: Consolidación y transformación de los datos utilizando Pandas en Lambda.
Almacenamiento seguro: Los datos extraídos y procesados se almacenan en Amazon S3.
Automatización y monitoreo: Las funciones Lambda están orquestadas por AWS Step Functions y monitoreadas con Amazon CloudWatch. Las notificaciones de estado se gestionan mediante Amazon SNS.
Seguridad: Uso de AWS IAM para control de acceso, encriptación de datos, y buenas prácticas de seguridad.
Requisitos
AWS Lambda
Python 3.x
Boto3
Pandas
Step Functions
SNS
S3
Estructura del Proyecto
Cada Lambda está ubicada en una carpeta separada y contiene su propio archivo ZIP y requirements.txt.


Monitoreo y Notificaciones
Las funciones Lambda son monitoreadas mediante Amazon CloudWatch, donde se pueden revisar logs y métricas.
Las alertas y notificaciones se envían a través de Amazon SNS, que notifica sobre el estado de las ejecuciones (éxito o error).
Seguridad
IAM Roles: Las Lambdas utilizan roles de IAM con permisos mínimos necesarios para acceder a los recursos de AWS.
Encriptación: Los datos transferidos y almacenados están protegidos mediante encriptación (en tránsito y en reposo).
Esto te proporciona una estructura detallada y completa para tu README. ¿Hay algo que te gustaría ajustar o agregar?
