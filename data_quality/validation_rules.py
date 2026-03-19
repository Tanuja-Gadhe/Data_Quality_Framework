"""
Data Quality Validation Rules Framework.
Defines and applies validation rules to DataFrames.
"""

import logging
from typing import List, Dict, Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull, lit


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ValidationRuleType(Enum):
    """Types of validation rules."""
    NOT_NULL = "not_null"
    UNIQUE = "unique"
    RANGE = "range"
    DATATYPE = "datatype"
    PATTERN = "pattern"
    CUSTOM = "custom"


class ValidationSeverity(Enum):
    """Severity levels for validation failures."""
    ERROR = "error"  # Record is invalid
    WARNING = "warning"  # Record has issues but may be acceptable
    INFO = "info"  # Informational only


@dataclass
class ValidationRule:
    """Represents a single validation rule."""
    rule_name: str
    rule_type: ValidationRuleType
    column_name: str
    severity: ValidationSeverity = ValidationSeverity.ERROR
    parameters: Dict[str, Any] = field(default_factory=dict)
    description: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert rule to dictionary."""
        return {
            "rule_name": self.rule_name,
            "rule_type": self.rule_type.value,
            "column_name": self.column_name,
            "severity": self.severity.value,
            "parameters": self.parameters,
            "description": self.description
        }


@dataclass
class ValidationResult:
    """Result of applying a validation rule."""
    rule_name: str
    column_name: str
    passed: bool
    failed_count: int
    total_count: int
    failure_percentage: float
    severity: ValidationSeverity
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "rule_name": self.rule_name,
            "column_name": self.column_name,
            "passed": self.passed,
            "failed_count": self.failed_count,
            "total_count": self.total_count,
            "failure_percentage": self.failure_percentage,
            "severity": self.severity.value,
            "error_message": self.error_message
        }


class ValidationEngine:
    """
    Engine for applying data quality validation rules to DataFrames.
    """
    
    def __init__(self):
        """Initialize validation engine."""
        self.rules: List[ValidationRule] = []
        logger.info("Initialized ValidationEngine")
    
    def add_rule(self, rule: ValidationRule) -> None:
        """
        Add a validation rule.
        
        Args:
            rule: ValidationRule to add
        """
        self.rules.append(rule)
        logger.info(f"Added validation rule: {rule.rule_name}")
    
    def add_rules(self, rules: List[ValidationRule]) -> None:
        """
        Add multiple validation rules.
        
        Args:
            rules: List of ValidationRules
        """
        for rule in rules:
            self.add_rule(rule)
    
    def clear_rules(self) -> None:
        """Clear all validation rules."""
        self.rules = []
        logger.info("Cleared all validation rules")
    
    def validate(self, df: DataFrame) -> tuple[DataFrame, DataFrame, List[ValidationResult]]:
        """
        Apply all validation rules to DataFrame.
        
        Args:
            df: Input DataFrame to validate
            
        Returns:
            Tuple of (valid_df, invalid_df, validation_results)
        """
        if not self.rules:
            logger.warning("No validation rules defined - returning all records as valid")
            return df, df.filter(lit(False)), []
        
        logger.info(f"Starting validation with {len(self.rules)} rules")
        
        # Add validation columns for each rule
        df_with_validations = df
        validation_results = []
        
        for rule in self.rules:
            try:
                df_with_validations, result = self._apply_rule(df_with_validations, rule)
                validation_results.append(result)
            except Exception as e:
                logger.error(f"Error applying rule {rule.rule_name}: {str(e)}")
                validation_results.append(ValidationResult(
                    rule_name=rule.rule_name,
                    column_name=rule.column_name,
                    passed=False,
                    failed_count=0,
                    total_count=0,
                    failure_percentage=0.0,
                    severity=rule.severity,
                    error_message=str(e)
                ))
        
        # Separate valid and invalid records
        # A record is valid if all ERROR-level rules pass
        error_rules = [r for r in self.rules if r.severity == ValidationSeverity.ERROR]
        
        if error_rules:
            # Build condition: all validation columns must be True
            valid_condition = col(f"_valid_{error_rules[0].rule_name}")
            for rule in error_rules[1:]:
                valid_condition = valid_condition & col(f"_valid_{rule.rule_name}")
            
            valid_df = df_with_validations.filter(valid_condition)
            invalid_df = df_with_validations.filter(~valid_condition)
            
            # Drop validation columns from output
            for rule in self.rules:
                valid_df = valid_df.drop(f"_valid_{rule.rule_name}")
                invalid_df = invalid_df.drop(f"_valid_{rule.rule_name}")
        else:
            # No error rules, all records are valid
            valid_df = df
            invalid_df = df.filter(lit(False))
        
        logger.info(f"Validation complete. Results: {len(validation_results)} rules applied")
        return valid_df, invalid_df, validation_results
    
    def _apply_rule(
        self, 
        df: DataFrame, 
        rule: ValidationRule
    ) -> tuple[DataFrame, ValidationResult]:
        """
        Apply a single validation rule.
        
        Args:
            df: Input DataFrame
            rule: Validation rule to apply
            
        Returns:
            Tuple of (DataFrame with validation column, ValidationResult)
        """
        logger.debug(f"Applying rule: {rule.rule_name}")
        
        # Apply rule based on type
        if rule.rule_type == ValidationRuleType.NOT_NULL:
            df_validated = self._apply_not_null_rule(df, rule)
        elif rule.rule_type == ValidationRuleType.UNIQUE:
            df_validated = self._apply_unique_rule(df, rule)
        elif rule.rule_type == ValidationRuleType.RANGE:
            df_validated = self._apply_range_rule(df, rule)
        elif rule.rule_type == ValidationRuleType.DATATYPE:
            df_validated = self._apply_datatype_rule(df, rule)
        elif rule.rule_type == ValidationRuleType.PATTERN:
            df_validated = self._apply_pattern_rule(df, rule)
        else:
            raise ValueError(f"Unsupported rule type: {rule.rule_type}")
        
        # Calculate validation statistics
        validation_col = f"_valid_{rule.rule_name}"
        total_count = df_validated.count()
        failed_count = df_validated.filter(~col(validation_col)).count()
        failure_percentage = (failed_count / total_count * 100) if total_count > 0 else 0.0
        
        result = ValidationResult(
            rule_name=rule.rule_name,
            column_name=rule.column_name,
            passed=failed_count == 0,
            failed_count=failed_count,
            total_count=total_count,
            failure_percentage=failure_percentage,
            severity=rule.severity
        )
        
        logger.info(
            f"Rule {rule.rule_name}: {failed_count}/{total_count} failures "
            f"({failure_percentage:.2f}%)"
        )
        
        return df_validated, result
    
    def _apply_not_null_rule(self, df: DataFrame, rule: ValidationRule) -> DataFrame:
        """Apply NOT NULL validation rule."""
        validation_col = f"_valid_{rule.rule_name}"
        return df.withColumn(
            validation_col,
            col(rule.column_name).isNotNull()
        )
    
    def _apply_unique_rule(self, df: DataFrame, rule: ValidationRule) -> DataFrame:
        """Apply UNIQUE validation rule."""
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number
        
        validation_col = f"_valid_{rule.rule_name}"
        
        # Count occurrences of each value
        window_spec = Window.partitionBy(rule.column_name).orderBy(rule.column_name)
        df_with_row_num = df.withColumn("_row_num", row_number().over(window_spec))
        
        # Mark as valid if it's the first occurrence
        return df_with_row_num.withColumn(
            validation_col,
            col("_row_num") == 1
        ).drop("_row_num")
    
    def _apply_range_rule(self, df: DataFrame, rule: ValidationRule) -> DataFrame:
        """Apply RANGE validation rule."""
        validation_col = f"_valid_{rule.rule_name}"
        min_val = rule.parameters.get("min")
        max_val = rule.parameters.get("max")
        
        condition = lit(True)
        if min_val is not None:
            condition = condition & (col(rule.column_name) >= min_val)
        if max_val is not None:
            condition = condition & (col(rule.column_name) <= max_val)
        
        return df.withColumn(validation_col, condition)
    
    def _apply_datatype_rule(self, df: DataFrame, rule: ValidationRule) -> DataFrame:
        """Apply DATATYPE validation rule."""
        validation_col = f"_valid_{rule.rule_name}"
        expected_type = rule.parameters.get("expected_type")
        
        # Try to cast and check if result is not null
        try:
            return df.withColumn(
                validation_col,
                col(rule.column_name).cast(expected_type).isNotNull()
            )
        except Exception:
            return df.withColumn(validation_col, lit(False))
    
    def _apply_pattern_rule(self, df: DataFrame, rule: ValidationRule) -> DataFrame:
        """Apply PATTERN validation rule (regex)."""
        from pyspark.sql.functions import regexp_extract
        
        validation_col = f"_valid_{rule.rule_name}"
        pattern = rule.parameters.get("pattern")
        
        if not pattern:
            raise ValueError(f"Pattern parameter required for rule {rule.rule_name}")
        
        return df.withColumn(
            validation_col,
            regexp_extract(col(rule.column_name), pattern, 0) != ""
        )


class OrdersValidationRules:
    """
    Predefined validation rules for orders table.
    """
    
    @staticmethod
    def get_rules() -> List[ValidationRule]:
        """
        Get validation rules for orders table.
        
        Returns:
            List of ValidationRule objects
        """
        return [
            ValidationRule(
                rule_name="order_id_not_null",
                rule_type=ValidationRuleType.NOT_NULL,
                column_name="order_id",
                severity=ValidationSeverity.ERROR,
                description="Order ID must not be null"
            ),
            ValidationRule(
                rule_name="order_id_unique",
                rule_type=ValidationRuleType.UNIQUE,
                column_name="order_id",
                severity=ValidationSeverity.ERROR,
                description="Order ID must be unique"
            ),
            ValidationRule(
                rule_name="price_positive",
                rule_type=ValidationRuleType.RANGE,
                column_name="price",
                severity=ValidationSeverity.ERROR,
                parameters={"min": 0},
                description="Price must be greater than or equal to 0"
            ),
            ValidationRule(
                rule_name="quantity_positive",
                rule_type=ValidationRuleType.RANGE,
                column_name="quantity",
                severity=ValidationSeverity.ERROR,
                parameters={"min": 1},
                description="Quantity must be at least 1"
            ),
            ValidationRule(
                rule_name="order_date_not_null",
                rule_type=ValidationRuleType.NOT_NULL,
                column_name="order_date",
                severity=ValidationSeverity.ERROR,
                description="Order date must not be null"
            ),
            ValidationRule(
                rule_name="customer_id_not_null",
                rule_type=ValidationRuleType.NOT_NULL,
                column_name="customer_id",
                severity=ValidationSeverity.ERROR,
                description="Customer ID must not be null"
            )
        ]
