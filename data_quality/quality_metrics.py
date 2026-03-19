"""
Data Quality Metrics Collection and Reporting.
Generates comprehensive data quality metrics and reports.
"""

import logging
import json
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field, asdict
from datetime import datetime
import boto3
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct, sum as spark_sum, when, isnan, isnull

from data_quality.validation_rules import ValidationResult


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class DataQualityMetrics:
    """Comprehensive data quality metrics."""
    table_name: str
    execution_timestamp: str
    total_records: int
    valid_records: int
    invalid_records: int
    duplicate_records: int
    null_counts: Dict[str, int] = field(default_factory=dict)
    validation_results: List[Dict[str, Any]] = field(default_factory=list)
    schema_info: Dict[str, Any] = field(default_factory=dict)
    data_quality_score: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        return asdict(self)
    
    def to_json(self) -> str:
        """Convert metrics to JSON string."""
        return json.dumps(self.to_dict(), indent=2)
    
    def calculate_quality_score(self) -> float:
        """
        Calculate overall data quality score (0-100).
        
        Score components:
        - Validity: 40% (percentage of valid records)
        - Uniqueness: 30% (percentage of unique records)
        - Completeness: 30% (percentage of non-null values)
        """
        if self.total_records == 0:
            return 0.0
        
        # Validity score
        validity_score = (self.valid_records / self.total_records) * 40
        
        # Uniqueness score
        unique_records = self.total_records - self.duplicate_records
        uniqueness_score = (unique_records / self.total_records) * 30
        
        # Completeness score
        total_nulls = sum(self.null_counts.values())
        total_cells = self.total_records * len(self.null_counts) if self.null_counts else self.total_records
        completeness_score = ((total_cells - total_nulls) / total_cells * 30) if total_cells > 0 else 0
        
        self.data_quality_score = round(validity_score + uniqueness_score + completeness_score, 2)
        return self.data_quality_score


@dataclass
class DataQualityAlert:
    """Represents a data quality alert."""
    alert_type: str
    severity: str
    message: str
    metric_value: float
    threshold_value: float
    timestamp: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert alert to dictionary."""
        return asdict(self)


class MetricsCollector:
    """
    Collects and generates data quality metrics from DataFrames.
    """
    
    def __init__(self, table_name: str):
        """
        Initialize metrics collector.
        
        Args:
            table_name: Name of the table being processed
        """
        self.table_name = table_name
        logger.info(f"Initialized MetricsCollector for table: {table_name}")
    
    def collect_metrics(
        self,
        original_df: DataFrame,
        valid_df: DataFrame,
        invalid_df: DataFrame,
        validation_results: List[ValidationResult],
        schema_changes: Optional[List[Dict[str, Any]]] = None
    ) -> DataQualityMetrics:
        """
        Collect comprehensive data quality metrics.
        
        Args:
            original_df: Original input DataFrame
            valid_df: DataFrame with valid records
            invalid_df: DataFrame with invalid records
            validation_results: List of validation results
            schema_changes: Optional list of schema changes detected
            
        Returns:
            DataQualityMetrics object
        """
        logger.info("Collecting data quality metrics")
        
        # Basic counts
        total_records = original_df.count()
        valid_records = valid_df.count()
        invalid_records = invalid_df.count()
        
        # Duplicate detection
        duplicate_records = self._count_duplicates(original_df)
        
        # Null counts per column
        null_counts = self._count_nulls(original_df)
        
        # Schema information
        schema_info = {
            "columns": [field.name for field in original_df.schema.fields],
            "column_count": len(original_df.schema.fields),
            "schema_changes": schema_changes or []
        }
        
        # Create metrics object
        metrics = DataQualityMetrics(
            table_name=self.table_name,
            execution_timestamp=datetime.utcnow().isoformat(),
            total_records=total_records,
            valid_records=valid_records,
            invalid_records=invalid_records,
            duplicate_records=duplicate_records,
            null_counts=null_counts,
            validation_results=[r.to_dict() for r in validation_results],
            schema_info=schema_info
        )
        
        # Calculate quality score
        metrics.calculate_quality_score()
        
        logger.info(
            f"Metrics collected - Total: {total_records}, "
            f"Valid: {valid_records}, Invalid: {invalid_records}, "
            f"Quality Score: {metrics.data_quality_score}"
        )
        
        return metrics
    
    def _count_duplicates(self, df: DataFrame) -> int:
        """
        Count duplicate records in DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Number of duplicate records
        """
        try:
            total_count = df.count()
            distinct_count = df.distinct().count()
            duplicates = total_count - distinct_count
            logger.debug(f"Found {duplicates} duplicate records")
            return duplicates
        except Exception as e:
            logger.error(f"Error counting duplicates: {str(e)}")
            return 0
    
    def _count_nulls(self, df: DataFrame) -> Dict[str, int]:
        """
        Count null values for each column.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Dictionary mapping column names to null counts
        """
        null_counts = {}
        
        try:
            for column in df.columns:
                null_count = df.filter(
                    col(column).isNull() | isnan(col(column))
                ).count()
                null_counts[column] = null_count
            
            logger.debug(f"Null counts: {null_counts}")
        except Exception as e:
            logger.error(f"Error counting nulls: {str(e)}")
        
        return null_counts
    
    def generate_summary_statistics(self, df: DataFrame) -> Dict[str, Any]:
        """
        Generate summary statistics for numeric columns.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Dictionary with summary statistics
        """
        from pyspark.sql.types import NumericType
        
        summary_stats = {}
        
        try:
            numeric_cols = [
                field.name for field in df.schema.fields 
                if isinstance(field.dataType, NumericType)
            ]
            
            if numeric_cols:
                stats_df = df.select(numeric_cols).summary()
                summary_stats = {
                    col_name: stats_df.select(col_name).collect()
                    for col_name in numeric_cols
                }
            
            logger.debug("Generated summary statistics")
        except Exception as e:
            logger.error(f"Error generating summary statistics: {str(e)}")
        
        return summary_stats


class MetricsReporter:
    """
    Reports data quality metrics to various destinations.
    """
    
    def __init__(self, s3_bucket: str, logs_prefix: str = "logs/data_quality_reports"):
        """
        Initialize metrics reporter.
        
        Args:
            s3_bucket: S3 bucket for storing reports
            logs_prefix: S3 prefix for log files
        """
        self.s3_bucket = s3_bucket
        self.logs_prefix = logs_prefix
        self.s3_client = boto3.client('s3')
        logger.info(f"Initialized MetricsReporter (bucket: {s3_bucket})")
    
    def save_metrics_to_s3(self, metrics: DataQualityMetrics) -> str:
        """
        Save metrics to S3 as JSON.
        
        Args:
            metrics: DataQualityMetrics object
            
        Returns:
            S3 path where metrics were saved
        """
        try:
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            s3_key = f"{self.logs_prefix}/{metrics.table_name}/metrics_{timestamp}.json"
            
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=metrics.to_json(),
                ContentType='application/json'
            )
            
            s3_path = f"s3://{self.s3_bucket}/{s3_key}"
            logger.info(f"Metrics saved to: {s3_path}")
            return s3_path
            
        except Exception as e:
            logger.error(f"Error saving metrics to S3: {str(e)}")
            raise
    
    def check_quality_thresholds(
        self,
        metrics: DataQualityMetrics,
        max_invalid_percentage: float = 5.0,
        max_duplicate_percentage: float = 2.0,
        min_quality_score: float = 85.0
    ) -> List[DataQualityAlert]:
        """
        Check if metrics exceed quality thresholds.
        
        Args:
            metrics: DataQualityMetrics object
            max_invalid_percentage: Maximum allowed invalid records percentage
            max_duplicate_percentage: Maximum allowed duplicate records percentage
            min_quality_score: Minimum acceptable quality score
            
        Returns:
            List of DataQualityAlert objects
        """
        alerts = []
        timestamp = datetime.utcnow().isoformat()
        
        # Check invalid records percentage
        if metrics.total_records > 0:
            invalid_percentage = (metrics.invalid_records / metrics.total_records) * 100
            if invalid_percentage > max_invalid_percentage:
                alerts.append(DataQualityAlert(
                    alert_type="INVALID_RECORDS_THRESHOLD_EXCEEDED",
                    severity="ERROR",
                    message=f"Invalid records percentage ({invalid_percentage:.2f}%) exceeds threshold ({max_invalid_percentage}%)",
                    metric_value=invalid_percentage,
                    threshold_value=max_invalid_percentage,
                    timestamp=timestamp
                ))
            
            # Check duplicate records percentage
            duplicate_percentage = (metrics.duplicate_records / metrics.total_records) * 100
            if duplicate_percentage > max_duplicate_percentage:
                alerts.append(DataQualityAlert(
                    alert_type="DUPLICATE_RECORDS_THRESHOLD_EXCEEDED",
                    severity="WARNING",
                    message=f"Duplicate records percentage ({duplicate_percentage:.2f}%) exceeds threshold ({max_duplicate_percentage}%)",
                    metric_value=duplicate_percentage,
                    threshold_value=max_duplicate_percentage,
                    timestamp=timestamp
                ))
        
        # Check quality score
        if metrics.data_quality_score < min_quality_score:
            alerts.append(DataQualityAlert(
                alert_type="LOW_QUALITY_SCORE",
                severity="ERROR",
                message=f"Data quality score ({metrics.data_quality_score}) is below threshold ({min_quality_score})",
                metric_value=metrics.data_quality_score,
                threshold_value=min_quality_score,
                timestamp=timestamp
            ))
        
        if alerts:
            logger.warning(f"Generated {len(alerts)} quality alerts")
        else:
            logger.info("All quality thresholds passed")
        
        return alerts
    
    def send_sns_alert(self, alerts: List[DataQualityAlert], sns_topic_arn: str) -> None:
        """
        Send alerts via Amazon SNS.
        
        Args:
            alerts: List of DataQualityAlert objects
            sns_topic_arn: ARN of SNS topic
        """
        if not alerts:
            return
        
        try:
            sns_client = boto3.client('sns')
            
            # Group alerts by severity
            error_alerts = [a for a in alerts if a.severity == "ERROR"]
            warning_alerts = [a for a in alerts if a.severity == "WARNING"]
            
            subject = f"Data Quality Alert - {len(error_alerts)} Errors, {len(warning_alerts)} Warnings"
            
            message_lines = ["Data Quality Alerts:\n"]
            for alert in alerts:
                message_lines.append(f"[{alert.severity}] {alert.alert_type}")
                message_lines.append(f"  {alert.message}")
                message_lines.append(f"  Timestamp: {alert.timestamp}\n")
            
            message = "\n".join(message_lines)
            
            sns_client.publish(
                TopicArn=sns_topic_arn,
                Subject=subject,
                Message=message
            )
            
            logger.info(f"Sent SNS alert to: {sns_topic_arn}")
            
        except Exception as e:
            logger.error(f"Error sending SNS alert: {str(e)}")
            # Don't fail the job if alert fails
