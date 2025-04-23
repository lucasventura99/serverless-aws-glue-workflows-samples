import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date, year, month, dayofweek, hour, when

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source', 'target', 'database', 'table'])

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read source data
source_path = args['source']
df = spark.read.parquet(source_path)

# Transform data
transformed_df = df.withColumn('date', to_date(col('transaction_date'))) \
    .withColumn('year', year(col('date'))) \
    .withColumn('month', month(col('date'))) \
    .withColumn('day_of_week', dayofweek(col('date'))) \
    .withColumn('hour', hour(col('transaction_time'))) \
    .withColumn('is_weekend', when(col('day_of_week').isin(1, 7), True).otherwise(False)) \
    .withColumn('is_business_hour', when(col('hour').between(9, 17), True).otherwise(False)) \
    .withColumn('sales_category', 
        when(col('amount') < 100, 'small') \
        .when(col('amount') < 500, 'medium') \
        .otherwise('large')) \
    .drop('transaction_date', 'transaction_time') \
    .orderBy('date', 'hour')

# Write transformed data
target_path = args['target']
transformed_df.write \
    .mode('overwrite') \
    .parquet(target_path)

# Update Glue catalog
glue_client = boto3.client('glue')
database_name = args['database']
table_name = args['table']

# Create table if it doesn't exist
try:
    glue_client.get_table(DatabaseName=database_name, Name=table_name)
except glue_client.exceptions.EntityNotFoundException:
    # Create table with the transformed schema
    table_input = {
        'Name': table_name,
        'DatabaseName': database_name,
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'classification': 'parquet'
        },
        'StorageDescriptor': {
            'Columns': [
                {'Name': 'amount', 'Type': 'double'},
                {'Name': 'product_id', 'Type': 'string'},
                {'Name': 'customer_id', 'Type': 'string'},
                {'Name': 'date', 'Type': 'date'},
                {'Name': 'year', 'Type': 'int'},
                {'Name': 'month', 'Type': 'int'},
                {'Name': 'day_of_week', 'Type': 'int'},
                {'Name': 'hour', 'Type': 'int'},
                {'Name': 'is_weekend', 'Type': 'boolean'},
                {'Name': 'is_business_hour', 'Type': 'boolean'},
                {'Name': 'sales_category', 'Type': 'string'}
            ],
            'Location': target_path,
            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                'Parameters': {'serialization.format': '1'}
            }
        }
    }
    glue_client.create_table(DatabaseName=database_name, TableInput=table_input)

# Commit the job
job.commit() 