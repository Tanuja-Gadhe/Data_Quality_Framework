"""
Schema Evolution Handler for AWS Data Lake.
Applies schema transformations to handle schema drift automatically.
"""

import logging
from typing import List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, col, cast
from pyspark.sql.types import StructType, StructField, DataType
import boto3

from schema_engine.schema_detector import (
    SchemaComparisonResult, 
    SchemaChange, 
    SchemaChangeType
)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SchemaEvolutionHandler:
    """
    Handles schema evolution by applying transformations to incoming data
    to match expected schema or evolve the target schema.
    """
    
    def __init__(
        self, 
        glue_database: str, 
        spark: SparkSession,
        allow_evolution: bool = True,
        strict_mode: bool = False
    ):
        """
        Initialize schema evolution handler.
        
        Args:
            glue_database: Name of the Glue database
            spark: SparkSession instance
            allow_evolution: Whether to allow automatic schema evolution
            strict_mode: If True, fail on any schema changes
        """
        self.glue_database = glue_database
        self.spark = spark
        self.allow_evolution = allow_evolution
        self.strict_mode = strict_mode
        self.glue_client = boto3.client('glue')
        logger.info(f"Initialized SchemaEvolutionHandler (allow_evolution={allow_evolution}, strict_mode={strict_mode})")
    
    def apply_schema_evolution(
        self, 
        df: DataFrame, 
        comparison_result: SchemaComparisonResult,
        table_name: str
    ) -> DataFrame:
        """
        Apply schema evolution transformations to the DataFrame.
        
        Args:
            df: Incoming DataFrame
            comparison_result: Result from schema comparison
            table_name: Target table name
            
        Returns:
            Transformed DataFrame with evolved schema
            
        Raises:
            ValueError: If strict mode is enabled and changes are detected
        """
        if not comparison_result.has_changes:
            logger.info("No schema changes detected - returning original DataFrame")
            return df
        
        if self.strict_mode:
            raise ValueError(
                f"Schema changes detected in strict mode for table {table_name}. "
                f"Changes: {[c.to_dict() for c in comparison_result.changes]}"
            )
        
        if not self.allow_evolution:
            logger.warning("Schema evolution is disabled - attempting to match existing schema")
            return self._match_existing_schema(df, comparison_result)
        
        logger.info(f"Applying schema evolution for {len(comparison_result.changes)} changes")
        
        # Apply transformations based on change types
        transformed_df = df
        
        # Handle new columns
        new_columns = comparison_result.get_changes_by_type(SchemaChangeType.NEW_COLUMN)
        if new_columns:
            logger.info(f"Processing {len(new_columns)} new columns")
            # New columns are already in the DataFrame, just log them
            for change in new_columns:
                logger.info(f"New column detected: {change.column_name} ({change.new_datatype})")
        
        # Handle missing columns
        missing_columns = comparison_result.get_changes_by_type(SchemaChangeType.MISSING_COLUMN)
        if missing_columns:
            transformed_df = self._add_missing_columns(transformed_df, missing_columns)
        
        # Handle datatype changes
        datatype_changes = comparison_result.get_changes_by_type(SchemaChangeType.DATATYPE_CHANGE)
        if datatype_changes:
            transformed_df = self._handle_datatype_changes(transformed_df, datatype_changes)
        
        # Handle column order changes
        order_changes = comparison_result.get_changes_by_type(SchemaChangeType.COLUMN_ORDER_CHANGE)
        if order_changes and comparison_result.catalog_schema:
            transformed_df = self._reorder_columns(
                transformed_df, 
                comparison_result.catalog_schema,
                new_columns
            )
        
        # Update Glue catalog if schema evolved
        if self.allow_evolution and comparison_result.catalog_schema:
            self._update_glue_catalog(table_name, transformed_df.schema)
        
        logger.info("Schema evolution applied successfully")
        return transformed_df
    
    def _add_missing_columns(
        self, 
        df: DataFrame, 
        missing_columns: List[SchemaChange]
    ) -> DataFrame:
        """
        Add missing columns to DataFrame with NULL values.
        
        Args:
            df: Input DataFrame
            missing_columns: List of missing column changes
            
        Returns:
            DataFrame with missing columns added
        """
        logger.info(f"Adding {len(missing_columns)} missing columns with NULL values")
        
        result_df = df
        for change in missing_columns:
            col_name = change.column_name
            col_type = self._parse_datatype(change.old_datatype)
            
            result_df = result_df.withColumn(col_name, lit(None).cast(col_type))
            logger.info(f"Added missing column: {col_name} as {col_type}")
        
        return result_df
    
    def _handle_datatype_changes(
        self, 
        df: DataFrame, 
        datatype_changes: List[SchemaChange]
    ) -> DataFrame:
        """
        Handle datatype changes by casting columns to new types.
        
        Args:
            df: Input DataFrame
            datatype_changes: List of datatype change objects
            
        Returns:
            DataFrame with casted columns
        """
        logger.info(f"Handling {len(datatype_changes)} datatype changes")
        
        result_df = df
        for change in datatype_changes:
            col_name = change.column_name
            target_type = self._parse_datatype(change.old_datatype)  # Cast to catalog type
            
            try:
                result_df = result_df.withColumn(
                    col_name, 
                    col(col_name).cast(target_type)
                )
                logger.info(f"Casted column {col_name} from {change.new_datatype} to {change.old_datatype}")
            except Exception as e:
                logger.error(f"Failed to cast column {col_name}: {str(e)}")
                # Keep original column if cast fails
                logger.warning(f"Keeping original datatype for {col_name}")
        
        return result_df
    
    def _reorder_columns(
        self, 
        df: DataFrame, 
        catalog_schema: StructType,
        new_columns: List[SchemaChange]
    ) -> DataFrame:
        """
        Reorder columns to match catalog schema, appending new columns at the end.
        
        Args:
            df: Input DataFrame
            catalog_schema: Target schema from catalog
            new_columns: List of new columns to append
            
        Returns:
            DataFrame with reordered columns
        """
        logger.info("Reordering columns to match catalog schema")
        
        # Get catalog column order
        catalog_cols = [field.name for field in catalog_schema.fields]
        
        # Get new column names
        new_col_names = [change.column_name for change in new_columns]
        
        # Build final column order: catalog columns + new columns
        final_order = catalog_cols + new_col_names
        
        # Select columns in the correct order
        result_df = df.select(*final_order)
        logger.info(f"Reordered columns: {final_order}")
        
        return result_df
    
    def _match_existing_schema(
        self, 
        df: DataFrame, 
        comparison_result: SchemaComparisonResult
    ) -> DataFrame:
        """
        Transform DataFrame to match existing catalog schema without evolution.
        
        Args:
            df: Input DataFrame
            comparison_result: Schema comparison result
            
        Returns:
            DataFrame matching catalog schema
        """
        if not comparison_result.catalog_schema:
            return df
        
        logger.info("Matching DataFrame to existing catalog schema")
        
        result_df = df
        catalog_schema = comparison_result.catalog_schema
        
        # Add missing columns
        missing_columns = comparison_result.get_changes_by_type(SchemaChangeType.MISSING_COLUMN)
        if missing_columns:
            result_df = self._add_missing_columns(result_df, missing_columns)
        
        # Remove new columns (not in catalog)
        new_columns = comparison_result.get_changes_by_type(SchemaChangeType.NEW_COLUMN)
        if new_columns:
            logger.warning(f"Dropping {len(new_columns)} new columns to match catalog")
            for change in new_columns:
                result_df = result_df.drop(change.column_name)
        
        # Cast datatypes to match catalog
        datatype_changes = comparison_result.get_changes_by_type(SchemaChangeType.DATATYPE_CHANGE)
        if datatype_changes:
            result_df = self._handle_datatype_changes(result_df, datatype_changes)
        
        # Reorder to match catalog
        catalog_cols = [field.name for field in catalog_schema.fields]
        result_df = result_df.select(*catalog_cols)
        
        return result_df
    
    def _update_glue_catalog(self, table_name: str, new_schema: StructType) -> None:
        """
        Update Glue Data Catalog with evolved schema.
        
        Args:
            table_name: Name of the table to update
            new_schema: New schema to apply
        """
        try:
            logger.info(f"Updating Glue catalog for table: {table_name}")
            
            # Get existing table
            response = self.glue_client.get_table(
                DatabaseName=self.glue_database,
                Name=table_name
            )
            
            table_input = response['Table']
            
            # Remove read-only fields
            table_input.pop('DatabaseName', None)
            table_input.pop('CreateTime', None)
            table_input.pop('UpdateTime', None)
            table_input.pop('CreatedBy', None)
            table_input.pop('IsRegisteredWithLakeFormation', None)
            table_input.pop('CatalogId', None)
            table_input.pop('VersionId', None)
            
            # Update schema
            columns = []
            for field in new_schema.fields:
                columns.append({
                    'Name': field.name,
                    'Type': self._spark_type_to_glue_type(field.dataType),
                    'Comment': ''
                })
            
            table_input['StorageDescriptor']['Columns'] = columns
            
            # Update table
            self.glue_client.update_table(
                DatabaseName=self.glue_database,
                TableInput=table_input
            )
            
            logger.info(f"Successfully updated Glue catalog for table: {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to update Glue catalog: {str(e)}")
            # Don't fail the job if catalog update fails
            logger.warning("Continuing without catalog update")
    
    def _parse_datatype(self, datatype_str: str) -> str:
        """
        Parse datatype string to Spark SQL type string.
        
        Args:
            datatype_str: String representation of datatype
            
        Returns:
            Spark SQL type string
        """
        # Remove parentheses and extract base type
        base_type = datatype_str.split("(")[0] if "(" in datatype_str else datatype_str
        
        type_mapping = {
            "StringType": "string",
            "IntegerType": "int",
            "LongType": "bigint",
            "DoubleType": "double",
            "FloatType": "float",
            "BooleanType": "boolean",
            "TimestampType": "timestamp",
            "DateType": "date",
            "DecimalType": "decimal(10,2)"
        }
        
        return type_mapping.get(base_type, "string")
    
    def _spark_type_to_glue_type(self, spark_type: DataType) -> str:
        """
        Convert Spark DataType to Glue type string.
        
        Args:
            spark_type: Spark DataType
            
        Returns:
            Glue type string
        """
        type_str = str(spark_type)
        
        type_mapping = {
            "StringType": "string",
            "IntegerType": "int",
            "LongType": "bigint",
            "DoubleType": "double",
            "FloatType": "float",
            "BooleanType": "boolean",
            "TimestampType": "timestamp",
            "DateType": "date",
        }
        
        for spark_name, glue_name in type_mapping.items():
            if spark_name in type_str:
                return glue_name
        
        # Handle decimal
        if "DecimalType" in type_str:
            return "decimal(10,2)"
        
        return "string"
