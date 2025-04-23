import sys
import json
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, count, when, isnull, to_date
from datetime import datetime

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source', 'target', 'database', 'table', 'quality-rules'])

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

# Read quality rules
s3_client = boto3.client('s3')
rules_path = args['quality-rules']
bucket, key = rules_path.replace('s3://', '').split('/', 1)
response = s3_client.get_object(Bucket=bucket, Key=key)
rules = json.loads(response['Body'].read().decode('utf-8'))

# Apply data quality rules
def apply_quality_rules(df, rules):
    for rule in rules:
        if rule['type'] == 'not_null':
            df = df.filter(col(rule['column']).isNotNull())
        elif rule['type'] == 'date_format':
            df = df.withColumn(rule['column'], to_date(col(rule['column']), rule['format']))
        elif rule['type'] == 'value_range':
            df = df.filter(col(rule['column']).between(rule['min'], rule['max']))
    
    return df

# Apply rules and write valid data
valid_df = apply_quality_rules(df, rules)
valid_df.write.mode('overwrite').parquet(args['target'])

# Generate quality report
quality_report = {
    'timestamp': datetime.now().isoformat(),
    'total_records': df.count(),
    'valid_records': valid_df.count(),
    'invalid_records': df.count() - valid_df.count(),
    'rules_applied': len(rules)
}

# Write quality report
report_path = args['target'].replace('/validated-data/', '/quality-reports/')
s3_client.put_object(
    Bucket=bucket,
    Key=f"{report_path}/quality_report_{datetime.now().strftime('%Y%m%d')}.json",
    Body=json.dumps(quality_report, indent=2)
)

# Commit the job
job.commit() 