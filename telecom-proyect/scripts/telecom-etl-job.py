import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
from datetime import datetime
#importo todas las dependencias


#inicio dynamoDB
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('pipeline-config')

#registro los logs en dynamodb
def log_pipeline_event(event_message):
    table.put_item(
        Item={
            'id_pipeline': str(datetime.utcnow()),
            'event_message': event_message,
            'timestamp': str(datetime.utcnow())
        }
    )
    print(f"Log registrado: {event_message}")

#cargo los parametros e inicio el job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#1er log del job
log_pipeline_event("Iniciando job ETL...")

#cargar datos desde el datalake (glue catalog)
try:
    datasource0 = glueContext.create_dynamic_frame.from_catalog(
        database="telecom_db",
        table_name="data",
        transformation_ctx="datasource0"
    )
    log_pipeline_event("Datos cargados desde Glue Catalog exitosamente.")
except Exception as e:
    log_pipeline_event(f"Error al cargar datos desde Glue Catalog: {str(e)}")
    raise

#se transforman los datos
try:
    applymapping1 = ApplyMapping.apply(frame=datasource0, 
        mappings=[
            ("id_cliente", "long", "id_cliente", "long"),
            ("nombre_cliente", "string", "nombre_cliente", "string"),
            ("plan", "string", "plan", "string"),
            ("fecha_inicio", "string", "fecha_inicio", "date"),
            ("fecha_fin", "string", "fecha_fin", "date"),
            ("monto", "long", "monto", "long")
        ],
        transformation_ctx="applymapping1"
    )
    log_pipeline_event("Se transformaron los datos correctamente.")
except Exception as e:
    log_pipeline_event(f"Error al transformar los datos: {str(e)}")
    raise

#cargo los datos procesados a s3
try:
    datasink2 = glueContext.write_dynamic_frame.from_options(
        frame=applymapping1,
        connection_type="s3",
        connection_options={"path": "s3://prueba-bucket-telecom/processed/"},
        format="parquet",
        transformation_ctx="datasink2"
    )
    log_pipeline_event("Datos cargados a S3 exitosamente.")
except Exception as e:
    log_pipeline_event(f"Error al cargar datos a S3: {str(e)}")
    raise

# termina el job 
job.commit()
log_pipeline_event("Job ETL completado correctamente.")