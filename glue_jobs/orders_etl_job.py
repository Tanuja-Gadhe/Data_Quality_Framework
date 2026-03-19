"""
AWS Glue ETL Job for Orders Data Processing.
Implements schema evolution and data quality validation for orders data.

This job:
1. Reads raw orders data from S3
2. Detects and handles schema changes
3. Applies data quality validation rules
4. Separates valid and invalid records
5. Writes valid records to clean layer
6. Writes invalid records to quarantine
7. Generates and saves data quality metrics
8. Sends alerts if quality thresholds are exceeded
"""

import sys
import logging
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, current_timestamp

# Import custom modules
from config.config import Config
from schema_engine import SchemaDetector, SchemaEvolutionHandler
from data_quality import (
    ValidationEngine,
    OrdersValidationRules,
    MetricsCollector,
    MetricsReporter
)


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OrdersETLJob:
    """
    Main ETL job class for processing orders data with schema evolution
    and data quality validation.
    """
    
    def __init__(self, glue_context: GlueContext, spark: SparkSession, job_name: str):
        """
        Initialize ETL job.
        
        Args:
            glue_context: AWS Glue context
            spark: Spark session
            job_name: Name of the Glue job
        """
        self.glue_context = glue_context
        self.spark = spark
        self.job_name = job_name
        self.config = Config
        
        # Initialize components
        self.schema_detector = SchemaDetector(
            glue_database=self.config.GLUE_DATABASE,
            catalog_id=self.config.GLUE_CATALOG_ID
        )
        
        self.schema_evolution_handler = SchemaEvolutionHandler(
            glue_database=self.config.GLUE_DATABASE,
            spark=self.spark,
            allow_evolution=self.config.ALLOW_SCHEMA_EVOLUTION,
            strict_mode=self.config.STRICT_MODE
        )
        
        self.validation_engine = ValidationEngine()
        self.validation_engine.add_rules(OrdersValidationRules.get_rules())
        
        self.metrics_collector = MetricsCollector(table_name="orders")
        self.metrics_reporter = MetricsReporter(
            s3_bucket=self.config.S3_BUCKET,
            logs_prefix="logs/data_quality_reports"
        )
        
        logger.info(f"Initialized OrdersETLJob: {job_name}")
    
    def run(self, input_path: str) -> None:
        """
        Execute the ETL job.
        
        Args:
            input_path: S3 path to input data
        """
        logger.info(f"Starting ETL job for input: {input_path}")
        job_start_time = datetime.utcnow()
        
        try:
            # Step 1: Read raw data
            logger.info("Step 1: Reading raw data from S3")
            raw_df = self._read_raw_data(input_path)
            
            if raw_df.count() == 0:
                logger.warning("No data found in input path")
                return
            
            logger.info(f"Read {raw_df.count()} records from raw layer")
            
            # Step 2: Detect schema changes
            logger.info("Step 2: Detecting schema changes")
            schema_comparison = self.schema_detector.detect_schema_changes(
                incoming_df=raw_df,
                table_name="clean_orders"
            )
            
            if schema_comparison.has_changes:
                logger.info(f"Schema changes detected: {len(schema_comparison.changes)}")
                for change in schema_comparison.changes:
                    logger.info(f"  - {change.to_dict()}")
            else:
                logger.info("No schema changes detected")
            
            # Step 3: Apply schema evolution
            logger.info("Step 3: Applying schema evolution")
            evolved_df = self.schema_evolution_handler.apply_schema_evolution(
                df=raw_df,
                comparison_result=schema_comparison,
                table_name="clean_orders"
            )
            
            # Step 4: Run data quality validation
            logger.info("Step 4: Running data quality validation")
            valid_df, invalid_df, validation_results = self.validation_engine.validate(evolved_df)
            
            logger.info(f"Validation complete - Valid: {valid_df.count()}, Invalid: {invalid_df.count()}")
            
            # Step 5: Add partition columns
            logger.info("Step 5: Adding partition columns")
            valid_df_partitioned = self._add_partition_columns(valid_df)
            invalid_df_partitioned = self._add_partition_columns(invalid_df)
            
            # Step 6: Write valid records to clean layer
            logger.info("Step 6: Writing valid records to clean layer")
            clean_path = self.config.get_table_config("orders")["clean_path"]
            self._write_to_clean_layer(valid_df_partitioned, clean_path)
            
            # Step 7: Write invalid records to quarantine
            if invalid_df.count() > 0:
                logger.info("Step 7: Writing invalid records to quarantine")
                quarantine_path = self.config.get_table_config("orders")["quarantine_path"]
                self._write_to_quarantine(invalid_df_partitioned, quarantine_path)
            else:
                logger.info("Step 7: No invalid records to quarantine")
            
            # Step 8: Collect and save metrics
            logger.info("Step 8: Collecting data quality metrics")
            metrics = self.metrics_collector.collect_metrics(
                original_df=raw_df,
                valid_df=valid_df,
                invalid_df=invalid_df,
                validation_results=validation_results,
                schema_changes=[c.to_dict() for c in schema_comparison.changes]
            )
            
            metrics_path = self.metrics_reporter.save_metrics_to_s3(metrics)
            logger.info(f"Metrics saved to: {metrics_path}")
            
            # Step 9: Check quality thresholds and send alerts
            logger.info("Step 9: Checking quality thresholds")
            alerts = self.metrics_reporter.check_quality_thresholds(
                metrics=metrics,
                max_invalid_percentage=self.config.MAX_INVALID_RECORDS_PERCENTAGE,
                max_duplicate_percentage=self.config.MAX_DUPLICATE_RECORDS_PERCENTAGE
            )
            
            if alerts and self.config.ENABLE_ALERTS and self.config.SNS_TOPIC_ARN:
                logger.warning(f"Quality thresholds exceeded - sending {len(alerts)} alerts")
                self.metrics_reporter.send_sns_alert(alerts, self.config.SNS_TOPIC_ARN)
            
            # Job summary
            job_duration = (datetime.utcnow() - job_start_time).total_seconds()
            logger.info("=" * 80)
            logger.info("ETL JOB SUMMARY")
            logger.info("=" * 80)
            logger.info(f"Job Name: {self.job_name}")
            logger.info(f"Duration: {job_duration:.2f} seconds")
            logger.info(f"Total Records: {metrics.total_records}")
            logger.info(f"Valid Records: {metrics.valid_records}")
            logger.info(f"Invalid Records: {metrics.invalid_records}")
            logger.info(f"Duplicate Records: {metrics.duplicate_records}")
            logger.info(f"Data Quality Score: {metrics.data_quality_score}/100")
            logger.info(f"Schema Changes: {len(schema_comparison.changes)}")
            logger.info(f"Alerts Generated: {len(alerts)}")
            logger.info("=" * 80)
            
            logger.info("ETL job completed successfully")
            
        except Exception as e:
            logger.error(f"ETL job failed: {str(e)}", exc_info=True)
            raise
    
    def _read_raw_data(self, input_path: str):
        """
        Read raw data from S3.
        
        Args:
            input_path: S3 path to input data
            
        Returns:
            Spark DataFrame
        """
        try:
            # Try reading as CSV first
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(input_path)
            
            logger.info(f"Successfully read CSV data from: {input_path}")
            return df
            
        except Exception as e:
            logger.error(f"Error reading data: {str(e)}")
            raise
    
    def _add_partition_columns(self, df):
        """
        Add partition columns (year, month, day) based on order_date.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with partition columns
        """
        # Check if order_date exists
        if "order_date" in df.columns:
            return df \
                .withColumn("year", year("order_date")) \
                .withColumn("month", month("order_date")) \
                .withColumn("day", dayofmonth("order_date"))
        else:
            # Use current timestamp if order_date doesn't exist
            logger.warning("order_date column not found - using current timestamp for partitioning")
            return df \
                .withColumn("_temp_date", current_timestamp()) \
                .withColumn("year", year("_temp_date")) \
                .withColumn("month", month("_temp_date")) \
                .withColumn("day", dayofmonth("_temp_date")) \
                .drop("_temp_date")
    
    def _write_to_clean_layer(self, df, output_path: str) -> None:
        """
        Write valid records to clean layer in Parquet format.
        
        Args:
            df: DataFrame to write
            output_path: S3 output path
        """
        try:
            df.write \
                .mode("append") \
                .partitionBy("year", "month", "day") \
                .format("parquet") \
                .option("compression", self.config.PARQUET_COMPRESSION) \
                .save(output_path)
            
            logger.info(f"Successfully wrote {df.count()} records to clean layer: {output_path}")
            
        except Exception as e:
            logger.error(f"Error writing to clean layer: {str(e)}")
            raise
    
    def _write_to_quarantine(self, df, output_path: str) -> None:
        """
        Write invalid records to quarantine layer.
        
        Args:
            df: DataFrame to write
            output_path: S3 output path
        """
        try:
            # Add quarantine metadata
            df_with_metadata = df.withColumn("quarantine_timestamp", current_timestamp())
            
            df_with_metadata.write \
                .mode("append") \
                .partitionBy("year", "month", "day") \
                .format("parquet") \
                .option("compression", self.config.PARQUET_COMPRESSION) \
                .save(output_path)
            
            logger.info(f"Successfully wrote {df.count()} records to quarantine: {output_path}")
            
        except Exception as e:
            logger.error(f"Error writing to quarantine: {str(e)}")
            raise


def main():
    """Main entry point for Glue job."""
    
    # Get job parameters
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'INPUT_PATH'
    ])
    
    job_name = args['JOB_NAME']
    input_path = args['INPUT_PATH']
    
    logger.info(f"Starting Glue job: {job_name}")
    logger.info(f"Input path: {input_path}")
    
    # Initialize Glue context
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    
    # Configure Spark
    spark.conf.set("spark.sql.shuffle.partitions", Config.SPARK_SHUFFLE_PARTITIONS)
    spark.conf.set("spark.default.parallelism", Config.SPARK_DEFAULT_PARALLELISM)
    
    # Initialize and run job
    job = Job(glue_context)
    job.init(job_name, args)
    
    try:
        etl_job = OrdersETLJob(glue_context, spark, job_name)
        etl_job.run(input_path)
        
        job.commit()
        logger.info("Job committed successfully")
        
    except Exception as e:
        logger.error(f"Job failed: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
