"""
Configuration module for AWS Data Lake Schema Evolution Framework.
Contains all configurable parameters for the pipeline.
"""

import os
from typing import Dict, Any


class Config:
    """Central configuration class for the data pipeline."""
    
    # S3 Bucket Configuration
    S3_BUCKET = os.getenv("DATA_LAKE_BUCKET", "data-lake-bucket")
    
    # S3 Path Configuration
    RAW_LAYER_PATH = f"s3://{S3_BUCKET}/raw"
    CLEAN_LAYER_PATH = f"s3://{S3_BUCKET}/clean"
    CURATED_LAYER_PATH = f"s3://{S3_BUCKET}/curated"
    QUARANTINE_LAYER_PATH = f"s3://{S3_BUCKET}/quarantine"
    LOGS_LAYER_PATH = f"s3://{S3_BUCKET}/logs"
    
    # Glue Configuration
    GLUE_DATABASE = os.getenv("GLUE_DATABASE", "data_lake_db")
    GLUE_CATALOG_ID = os.getenv("AWS_ACCOUNT_ID", "")
    
    # Data Quality Thresholds
    MAX_INVALID_RECORDS_PERCENTAGE = float(os.getenv("MAX_INVALID_PERCENTAGE", "5.0"))
    MAX_DUPLICATE_RECORDS_PERCENTAGE = float(os.getenv("MAX_DUPLICATE_PERCENTAGE", "2.0"))
    
    # SNS Configuration
    SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN", "")
    ENABLE_ALERTS = os.getenv("ENABLE_ALERTS", "true").lower() == "true"
    
    # Spark Configuration
    SPARK_SHUFFLE_PARTITIONS = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "200"))
    SPARK_DEFAULT_PARALLELISM = int(os.getenv("SPARK_DEFAULT_PARALLELISM", "100"))
    
    # Parquet Configuration
    PARQUET_COMPRESSION = os.getenv("PARQUET_COMPRESSION", "snappy")
    PARTITION_COLUMNS = ["year", "month", "day"]
    
    # Schema Evolution Configuration
    ALLOW_SCHEMA_EVOLUTION = os.getenv("ALLOW_SCHEMA_EVOLUTION", "true").lower() == "true"
    STRICT_MODE = os.getenv("STRICT_MODE", "false").lower() == "true"
    
    # CloudWatch Configuration
    LOG_GROUP_NAME = os.getenv("LOG_GROUP_NAME", "/aws/glue/data-lake-pipeline")
    
    @classmethod
    def get_table_config(cls, table_name: str) -> Dict[str, Any]:
        """Get table-specific configuration."""
        table_configs = {
            "orders": {
                "raw_path": f"{cls.RAW_LAYER_PATH}/orders/",
                "clean_path": f"{cls.CLEAN_LAYER_PATH}/orders/",
                "quarantine_path": f"{cls.QUARANTINE_LAYER_PATH}/orders_invalid/",
                "partition_columns": cls.PARTITION_COLUMNS,
                "glue_table_name": "clean_orders"
            }
        }
        return table_configs.get(table_name, {})
    
    @classmethod
    def get_s3_paths(cls) -> Dict[str, str]:
        """Return all S3 paths as a dictionary."""
        return {
            "raw": cls.RAW_LAYER_PATH,
            "clean": cls.CLEAN_LAYER_PATH,
            "curated": cls.CURATED_LAYER_PATH,
            "quarantine": cls.QUARANTINE_LAYER_PATH,
            "logs": cls.LOGS_LAYER_PATH
        }
