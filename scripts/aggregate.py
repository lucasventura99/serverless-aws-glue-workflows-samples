import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, sum, count, avg, max, min, collect_list, struct

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

# Create daily aggregations
daily_agg = df.groupBy('date') \
    .agg(
        sum('amount').alias('total_sales'),
        count('*').alias('transaction_count'),
        avg('amount').alias('average_transaction'),
        max('amount').alias('max_transaction'),
        min('amount').alias('min_transaction')
    ) \
    .orderBy('date')

# Create monthly aggregations
monthly_agg = df.groupBy('year', 'month') \
    .agg(
        sum('amount').alias('total_sales'),
        count('*').alias('transaction_count'),
        avg('amount').alias('average_transaction'),
        max('amount').alias('max_transaction'),
        min('amount').alias('min_transaction')
    ) \
    .orderBy('year', 'month')

# Create category aggregations
category_agg = df.groupBy('sales_category') \
    .agg(
        sum('amount').alias('total_sales'),
        count('*').alias('transaction_count'),
        avg('amount').alias('average_transaction')
    ) \
    .orderBy('sales_category')

# Create time-based aggregations
time_agg = df.groupBy('hour') \
    .agg(
        sum('amount').alias('total_sales'),
        count('*').alias('transaction_count'),
        avg('amount').alias('average_transaction')
    ) \
    .orderBy('hour')

# Write aggregated data
target_path = args['target']
daily_agg.write.mode('overwrite').parquet(f"{target_path}/daily")
monthly_agg.write.mode('overwrite').parquet(f"{target_path}/monthly")
category_agg.write.mode('overwrite').parquet(f"{target_path}/category")
time_agg.write.mode('overwrite').parquet(f"{target_path}/time")

# Update Glue catalog
glue_client = boto3.client('glue')
database_name = args['database']

# Create tables if they don't exist
tables = {
    'daily_sales': daily_agg,
    'monthly_sales': monthly_agg,
    'category_sales': category_agg,
    'time_sales': time_agg
}

for table_name, df in tables.items():
    try:
        glue_client.get_table(DatabaseName=database_name, Name=table_name)
    except glue_client.exceptions.EntityNotFoundException:
        # Create table with the aggregated schema
        table_input = {
            'Name': table_name,
            'DatabaseName': database_name,
            'TableType': 'EXTERNAL_TABLE',
            'Parameters': {
                'classification': 'parquet'
            },
            'StorageDescriptor': {
                'Columns': [
                    {'Name': col, 'Type': dtype}
                    for col, dtype in df.dtypes
                ],
                'Location': f"{target_path}/{table_name.replace('_', '')}",
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