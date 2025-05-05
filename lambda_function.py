import json
import boto3
import pandas as pd
import io
import logging
import snowflake.connector
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

SNOWFLAKE_CONFIG = {
    'user': 'YOUR_USER',
    'password': 'YOUR_PASSWORD',
    'account': 'YOUR_ACCOUNT_ID',
    'warehouse': 'YOUR_WAREHOUSE',
    'database': 'YOUR_DATABASE',
    'schema': 'YOUR_SCHEMA',
    'role': 'YOUR_ROLE',
}

def lambda_handler(event, context):
    try:
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            logger.info(f"Processing file: s3://{bucket}/{key}")
            obj = s3.get_object(Bucket=bucket, Key=key)
            data = obj['Body'].read()
            df = pd.read_csv(io.BytesIO(data))
            expected_cols = ['ip', 'timestamp', 'url', 'user_agent']
            if list(df.columns) != expected_cols:
                logger.error("Schema mismatch")
                raise ValueError("Schema does not match expected format")
            logger.info(f"Validated schema for {len(df)} rows")
            s3.put_object_tagging(
                Bucket=bucket,
                Key=key,
                Tagging={
                    'TagSet': [
                        {'Key': 'source', 'Value': 'web-logs'},
                        {'Key': 'processed_at', 'Value': datetime.utcnow().isoformat()}
                    ]
                }
            )
            temp_file_path = f"/tmp/{key.split('/')[-1]}"
            df.to_csv(temp_file_path, index=False)
            conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            cursor = conn.cursor()
            stage_name = '@web_log_stage'
            table_name = 'web_logs'
            put_command = f"PUT file://{temp_file_path} {stage_name} OVERWRITE = TRUE"
            cursor.execute(put_command)
            copy_command = f"""COPY INTO {table_name}
FROM {stage_name}
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
ON_ERROR = 'CONTINUE'"""
            cursor.execute(copy_command)
            logger.info(f"Successfully ingested file into Snowflake table: {table_name}")
    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        raise e
