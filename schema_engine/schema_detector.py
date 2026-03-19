"""
Schema Detection Engine for AWS Data Lake.
Detects schema changes in incoming data files and compares with Glue Data Catalog.
"""

import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from enum import Enum
import boto3
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, DataType


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SchemaChangeType(Enum):
    """Enumeration of possible schema change types."""
    NEW_COLUMN = "new_column"
    MISSING_COLUMN = "missing_column"
    DATATYPE_CHANGE = "datatype_change"
    COLUMN_ORDER_CHANGE = "column_order_change"
    NO_CHANGE = "no_change"


@dataclass
class SchemaChange:
    """Represents a detected schema change."""
    change_type: SchemaChangeType
    column_name: Optional[str] = None
    old_datatype: Optional[str] = None
    new_datatype: Optional[str] = None
    position: Optional[int] = None
    details: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert schema change to dictionary."""
        return {
            "change_type": self.change_type.value,
            "column_name": self.column_name,
            "old_datatype": self.old_datatype,
            "new_datatype": self.new_datatype,
            "position": self.position,
            "details": self.details
        }


@dataclass
class SchemaComparisonResult:
    """Result of schema comparison between incoming data and catalog."""
    has_changes: bool
    changes: List[SchemaChange]
    incoming_schema: StructType
    catalog_schema: Optional[StructType]
    is_backward_compatible: bool
    
    def get_changes_by_type(self, change_type: SchemaChangeType) -> List[SchemaChange]:
        """Filter changes by type."""
        return [c for c in self.changes if c.change_type == change_type]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert comparison result to dictionary."""
        return {
            "has_changes": self.has_changes,
            "changes": [c.to_dict() for c in self.changes],
            "is_backward_compatible": self.is_backward_compatible,
            "total_changes": len(self.changes)
        }


class SchemaDetector:
    """
    Schema detection engine that compares incoming data schemas 
    with existing Glue Data Catalog schemas.
    """
    
    def __init__(self, glue_database: str, catalog_id: Optional[str] = None):
        """
        Initialize schema detector.
        
        Args:
            glue_database: Name of the Glue database
            catalog_id: AWS account ID (optional)
        """
        self.glue_database = glue_database
        self.catalog_id = catalog_id
        self.glue_client = boto3.client('glue')
        logger.info(f"Initialized SchemaDetector for database: {glue_database}")
    
    def get_catalog_schema(self, table_name: str) -> Optional[StructType]:
        """
        Retrieve schema from Glue Data Catalog.
        
        Args:
            table_name: Name of the Glue table
            
        Returns:
            StructType schema or None if table doesn't exist
        """
        try:
            response = self.glue_client.get_table(
                DatabaseName=self.glue_database,
                Name=table_name
            )
            
            storage_descriptor = response['Table']['StorageDescriptor']
            columns = storage_descriptor['Columns']
            
            # Convert Glue schema to Spark StructType
            fields = []
            for col in columns:
                field = StructField(
                    col['Name'],
                    self._glue_type_to_spark_type(col['Type']),
                    True  # nullable
                )
                fields.append(field)
            
            schema = StructType(fields)
            logger.info(f"Retrieved catalog schema for table: {table_name}")
            return schema
            
        except self.glue_client.exceptions.EntityNotFoundException:
            logger.warning(f"Table {table_name} not found in Glue catalog")
            return None
        except Exception as e:
            logger.error(f"Error retrieving catalog schema: {str(e)}")
            raise
    
    def detect_schema_changes(
        self, 
        incoming_df: DataFrame, 
        table_name: str
    ) -> SchemaComparisonResult:
        """
        Detect schema changes between incoming data and catalog schema.
        
        Args:
            incoming_df: Incoming Spark DataFrame
            table_name: Name of the target table
            
        Returns:
            SchemaComparisonResult with detected changes
        """
        logger.info(f"Starting schema change detection for table: {table_name}")
        
        incoming_schema = incoming_df.schema
        catalog_schema = self.get_catalog_schema(table_name)
        
        # If table doesn't exist in catalog, it's a new table
        if catalog_schema is None:
            logger.info(f"Table {table_name} is new - no existing schema in catalog")
            return SchemaComparisonResult(
                has_changes=False,
                changes=[],
                incoming_schema=incoming_schema,
                catalog_schema=None,
                is_backward_compatible=True
            )
        
        # Compare schemas
        changes = self._compare_schemas(catalog_schema, incoming_schema)
        
        # Check backward compatibility
        is_backward_compatible = self._check_backward_compatibility(changes)
        
        result = SchemaComparisonResult(
            has_changes=len(changes) > 0,
            changes=changes,
            incoming_schema=incoming_schema,
            catalog_schema=catalog_schema,
            is_backward_compatible=is_backward_compatible
        )
        
        logger.info(f"Schema detection complete. Changes found: {len(changes)}")
        return result
    
    def _compare_schemas(
        self, 
        catalog_schema: StructType, 
        incoming_schema: StructType
    ) -> List[SchemaChange]:
        """
        Compare two schemas and identify changes.
        
        Args:
            catalog_schema: Existing schema from catalog
            incoming_schema: New schema from incoming data
            
        Returns:
            List of detected schema changes
        """
        changes = []
        
        catalog_fields = {field.name: field for field in catalog_schema.fields}
        incoming_fields = {field.name: field for field in incoming_schema.fields}
        
        # Check for new columns
        for col_name, field in incoming_fields.items():
            if col_name not in catalog_fields:
                changes.append(SchemaChange(
                    change_type=SchemaChangeType.NEW_COLUMN,
                    column_name=col_name,
                    new_datatype=str(field.dataType),
                    details={"nullable": field.nullable}
                ))
                logger.info(f"Detected new column: {col_name}")
        
        # Check for missing columns
        for col_name, field in catalog_fields.items():
            if col_name not in incoming_fields:
                changes.append(SchemaChange(
                    change_type=SchemaChangeType.MISSING_COLUMN,
                    column_name=col_name,
                    old_datatype=str(field.dataType),
                    details={"nullable": field.nullable}
                ))
                logger.info(f"Detected missing column: {col_name}")
        
        # Check for datatype changes
        for col_name in catalog_fields.keys() & incoming_fields.keys():
            catalog_type = str(catalog_fields[col_name].dataType)
            incoming_type = str(incoming_fields[col_name].dataType)
            
            if catalog_type != incoming_type:
                changes.append(SchemaChange(
                    change_type=SchemaChangeType.DATATYPE_CHANGE,
                    column_name=col_name,
                    old_datatype=catalog_type,
                    new_datatype=incoming_type
                ))
                logger.info(f"Detected datatype change for {col_name}: {catalog_type} -> {incoming_type}")
        
        # Check for column order changes
        if self._has_column_order_changed(catalog_schema, incoming_schema):
            changes.append(SchemaChange(
                change_type=SchemaChangeType.COLUMN_ORDER_CHANGE,
                details={
                    "catalog_order": [f.name for f in catalog_schema.fields],
                    "incoming_order": [f.name for f in incoming_schema.fields]
                }
            ))
            logger.info("Detected column order change")
        
        return changes
    
    def _has_column_order_changed(
        self, 
        catalog_schema: StructType, 
        incoming_schema: StructType
    ) -> bool:
        """Check if column order has changed."""
        catalog_cols = [f.name for f in catalog_schema.fields]
        incoming_cols = [f.name for f in incoming_schema.fields]
        
        # Only check order for common columns
        common_cols = set(catalog_cols) & set(incoming_cols)
        catalog_order = [c for c in catalog_cols if c in common_cols]
        incoming_order = [c for c in incoming_cols if c in common_cols]
        
        return catalog_order != incoming_order
    
    def _check_backward_compatibility(self, changes: List[SchemaChange]) -> bool:
        """
        Check if schema changes are backward compatible.
        
        Backward compatible changes:
        - Adding new nullable columns
        - Reordering columns
        
        Non-backward compatible changes:
        - Removing columns
        - Changing datatypes
        """
        for change in changes:
            if change.change_type == SchemaChangeType.MISSING_COLUMN:
                return False
            if change.change_type == SchemaChangeType.DATATYPE_CHANGE:
                # Check if it's a safe cast
                if not self._is_safe_type_cast(change.old_datatype, change.new_datatype):
                    return False
        
        return True
    
    def _is_safe_type_cast(self, old_type: str, new_type: str) -> bool:
        """
        Determine if a type cast is safe.
        
        Safe casts examples:
        - int -> long
        - float -> double
        - string -> string (always safe)
        """
        safe_casts = {
            ("IntegerType", "LongType"),
            ("FloatType", "DoubleType"),
            ("ShortType", "IntegerType"),
            ("ShortType", "LongType"),
            ("IntegerType", "DoubleType"),
        }
        
        # Extract type names
        old_type_name = old_type.split("(")[0] if "(" in old_type else old_type
        new_type_name = new_type.split("(")[0] if "(" in new_type else new_type
        
        return (old_type_name, new_type_name) in safe_casts
    
    def _glue_type_to_spark_type(self, glue_type: str) -> DataType:
        """Convert Glue data type to Spark data type."""
        from pyspark.sql.types import (
            StringType, IntegerType, LongType, DoubleType, 
            BooleanType, TimestampType, DateType, DecimalType
        )
        
        type_mapping = {
            "string": StringType(),
            "int": IntegerType(),
            "bigint": LongType(),
            "double": DoubleType(),
            "float": DoubleType(),
            "boolean": BooleanType(),
            "timestamp": TimestampType(),
            "date": DateType(),
        }
        
        glue_type_lower = glue_type.lower()
        
        # Handle decimal types
        if glue_type_lower.startswith("decimal"):
            return DecimalType()
        
        return type_mapping.get(glue_type_lower, StringType())
