"""
AWS Lambda Function to Trigger Glue ETL Pipeline.
Monitors S3 for new files and triggers the Glue job for processing.

This Lambda function:
1. Receives S3 event notifications when new files are uploaded
2. Validates the file format and location
3. Triggers the appropriate Glue ETL job
4. Logs execution details to CloudWatch
"""

import json
import logging
import os
import boto3
from typing import Dict, Any, List
from urllib.parse import unquote_plus
from datetime import datetime


# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
glue_client = boto3.client('glue')
s3_client = boto3.client('s3')
cloudwatch_client = boto3.client('cloudwatch')

# Environment variables
GLUE_JOB_NAME = os.environ.get('GLUE_JOB_NAME', 'orders-etl-job')
GLUE_DATABASE = os.environ.get('GLUE_DATABASE', 'data_lake_db')
RAW_DATA_PREFIX = os.environ.get('RAW_DATA_PREFIX', 'raw/orders/')
ENABLE_NOTIFICATIONS = os.environ.get('ENABLE_NOTIFICATIONS', 'true').lower() == 'true'


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler function triggered by S3 events.
    
    Args:
        event: S3 event notification
        context: Lambda context
        
    Returns:
        Response dictionary with status and details
    """
    logger.info("Lambda function triggered")
    logger.info(f"Event: {json.dumps(event)}")
    
    try:
        # Parse S3 event
        s3_records = parse_s3_event(event)
        
        if not s3_records:
            logger.warning("No valid S3 records found in event")
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'No valid S3 records to process'})
            }
        
        # Process each S3 record
        results = []
        for record in s3_records:
            result = process_s3_record(record)
            results.append(result)
        
        # Aggregate results
        successful = sum(1 for r in results if r['success'])
        failed = len(results) - successful
        
        logger.info(f"Processing complete - Success: {successful}, Failed: {failed}")
        
        # Publish CloudWatch metrics
        publish_metrics(successful, failed)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Pipeline triggered successfully',
                'successful': successful,
                'failed': failed,
                'results': results
            })
        }
        
    except Exception as e:
        logger.error(f"Error in lambda handler: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Failed to trigger pipeline'
            })
        }


def parse_s3_event(event: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Parse S3 event notification and extract file details.
    
    Args:
        event: S3 event notification
        
    Returns:
        List of S3 record dictionaries
    """
    s3_records = []
    
    try:
        if 'Records' not in event:
            logger.warning("No Records found in event")
            return s3_records
        
        for record in event['Records']:
            # Check if it's an S3 event
            if record.get('eventSource') != 'aws:s3':
                continue
            
            # Extract S3 details
            s3_info = record['s3']
            bucket = s3_info['bucket']['name']
            key = unquote_plus(s3_info['object']['key'])
            size = s3_info['object']['size']
            
            # Validate file
            if not is_valid_file(key, size):
                logger.warning(f"Skipping invalid file: {key}")
                continue
            
            s3_records.append({
                'bucket': bucket,
                'key': key,
                'size': size,
                'event_time': record.get('eventTime', '')
            })
            
            logger.info(f"Parsed S3 record - Bucket: {bucket}, Key: {key}, Size: {size}")
    
    except Exception as e:
        logger.error(f"Error parsing S3 event: {str(e)}")
    
    return s3_records


def is_valid_file(key: str, size: int) -> bool:
    """
    Validate if the file should trigger the pipeline.
    
    Args:
        key: S3 object key
        size: File size in bytes
        
    Returns:
        True if file is valid, False otherwise
    """
    # Check if file is in the correct prefix
    if not key.startswith(RAW_DATA_PREFIX):
        logger.debug(f"File not in raw data prefix: {key}")
        return False
    
    # Check file extension
    valid_extensions = ['.csv', '.json', '.parquet']
    if not any(key.lower().endswith(ext) for ext in valid_extensions):
        logger.debug(f"File has invalid extension: {key}")
        return False
    
    # Check file size (skip empty files)
    if size == 0:
        logger.warning(f"File is empty: {key}")
        return False
    
    # Skip temporary or hidden files
    filename = key.split('/')[-1]
    if filename.startswith('.') or filename.startswith('_'):
        logger.debug(f"Skipping temporary/hidden file: {key}")
        return False
    
    return True


def process_s3_record(record: Dict[str, str]) -> Dict[str, Any]:
    """
    Process a single S3 record by triggering the Glue job.
    
    Args:
        record: S3 record dictionary
        
    Returns:
        Result dictionary with processing status
    """
    bucket = record['bucket']
    key = record['key']
    
    logger.info(f"Processing file: s3://{bucket}/{key}")
    
    try:
        # Construct input path
        input_path = f"s3://{bucket}/{key}"
        
        # Trigger Glue job
        job_run_id = trigger_glue_job(input_path)
        
        logger.info(f"Successfully triggered Glue job. Run ID: {job_run_id}")
        
        return {
            'success': True,
            'bucket': bucket,
            'key': key,
            'job_run_id': job_run_id,
            'message': 'Glue job triggered successfully'
        }
        
    except Exception as e:
        logger.error(f"Error processing S3 record: {str(e)}")
        return {
            'success': False,
            'bucket': bucket,
            'key': key,
            'error': str(e),
            'message': 'Failed to trigger Glue job'
        }


def trigger_glue_job(input_path: str) -> str:
    """
    Trigger AWS Glue ETL job.
    
    Args:
        input_path: S3 path to input data
        
    Returns:
        Glue job run ID
    """
    try:
        response = glue_client.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                '--INPUT_PATH': input_path,
                '--GLUE_DATABASE': GLUE_DATABASE,
                '--enable-metrics': 'true',
                '--enable-continuous-cloudwatch-log': 'true'
            }
        )
        
        job_run_id = response['JobRunId']
        logger.info(f"Glue job started - Job: {GLUE_JOB_NAME}, Run ID: {job_run_id}")
        
        return job_run_id
        
    except Exception as e:
        logger.error(f"Error triggering Glue job: {str(e)}")
        raise


def publish_metrics(successful: int, failed: int) -> None:
    """
    Publish custom metrics to CloudWatch.
    
    Args:
        successful: Number of successful triggers
        failed: Number of failed triggers
    """
    try:
        namespace = 'DataLake/Pipeline'
        timestamp = datetime.utcnow()
        
        metrics = [
            {
                'MetricName': 'SuccessfulTriggers',
                'Value': successful,
                'Unit': 'Count',
                'Timestamp': timestamp
            },
            {
                'MetricName': 'FailedTriggers',
                'Value': failed,
                'Unit': 'Count',
                'Timestamp': timestamp
            }
        ]
        
        for metric in metrics:
            cloudwatch_client.put_metric_data(
                Namespace=namespace,
                MetricData=[metric]
            )
        
        logger.info(f"Published CloudWatch metrics - Success: {successful}, Failed: {failed}")
        
    except Exception as e:
        logger.error(f"Error publishing metrics: {str(e)}")
        # Don't fail the function if metrics fail


def get_glue_job_status(job_run_id: str) -> Dict[str, Any]:
    """
    Get the status of a Glue job run.
    
    Args:
        job_run_id: Glue job run ID
        
    Returns:
        Job status dictionary
    """
    try:
        response = glue_client.get_job_run(
            JobName=GLUE_JOB_NAME,
            RunId=job_run_id
        )
        
        job_run = response['JobRun']
        
        return {
            'job_run_id': job_run_id,
            'state': job_run['JobRunState'],
            'started_on': job_run.get('StartedOn', ''),
            'completed_on': job_run.get('CompletedOn', ''),
            'execution_time': job_run.get('ExecutionTime', 0),
            'error_message': job_run.get('ErrorMessage', '')
        }
        
    except Exception as e:
        logger.error(f"Error getting job status: {str(e)}")
        return {
            'job_run_id': job_run_id,
            'state': 'UNKNOWN',
            'error': str(e)
        }
