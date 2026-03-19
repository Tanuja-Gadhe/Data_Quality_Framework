# Architecture Documentation

Detailed technical architecture of the AWS Data Lake Schema Evolution Framework.

---

## Table of Contents

1. [System Architecture](#system-architecture)
2. [Component Design](#component-design)
3. [Data Flow](#data-flow)
4. [Schema Evolution Logic](#schema-evolution-logic)
5. [Data Quality Framework](#data-quality-framework)
6. [Error Handling](#error-handling)
7. [Performance Considerations](#performance-considerations)
8. [Security Architecture](#security-architecture)

---

## System Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         AWS CLOUD ENVIRONMENT                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌────────────────┐                                                 │
│  │  Data Sources  │                                                 │
│  │  - CSV Files   │                                                 │
│  │  - JSON Files  │                                                 │
│  └───────┬────────┘                                                 │
│          │                                                           │
│          ▼                                                           │
│  ┌─────────────────────────────────────────────────────────┐       │
│  │              Amazon S3 (Data Lake)                      │       │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────────┐         │       │
│  │  │   Raw    │  │  Clean   │  │  Quarantine  │         │       │
│  │  │  Layer   │  │  Layer   │  │    Layer     │         │       │
│  │  └────┬─────┘  └──────────┘  └──────────────┘         │       │
│  └───────┼──────────────────────────────────────────────────       │
│          │ S3 Event                                                 │
│          ▼                                                           │
│  ┌─────────────────┐                                                │
│  │  AWS Lambda     │                                                │
│  │  - Validate     │                                                │
│  │  - Trigger Job  │                                                │
│  └────────┬────────┘                                                │
│           │ StartJobRun                                             │
│           ▼                                                          │
│  ┌──────────────────────────────────────────────────────┐          │
│  │           AWS Glue ETL Job (Spark)                   │          │
│  │  ┌──────────────────────────────────────────────┐   │          │
│  │  │  Schema Detection Engine                     │   │          │
│  │  │  - Compare schemas                           │   │          │
│  │  │  - Identify changes                          │   │          │
│  │  └──────────────────────────────────────────────┘   │          │
│  │  ┌──────────────────────────────────────────────┐   │          │
│  │  │  Schema Evolution Handler                    │   │          │
│  │  │  - Apply transformations                     │   │          │
│  │  │  - Update catalog                            │   │          │
│  │  └──────────────────────────────────────────────┘   │          │
│  │  ┌──────────────────────────────────────────────┐   │          │
│  │  │  Data Quality Validation Engine              │   │          │
│  │  │  - Execute rules                             │   │          │
│  │  │  - Separate valid/invalid                    │   │          │
│  │  └──────────────────────────────────────────────┘   │          │
│  │  ┌──────────────────────────────────────────────┐   │          │
│  │  │  Metrics Collection & Reporting              │   │          │
│  │  │  - Calculate metrics                         │   │          │
│  │  │  - Generate reports                          │   │          │
│  │  └──────────────────────────────────────────────┘   │          │
│  └──────────┬───────────────────┬───────────────────────┘          │
│             │                   │                                   │
│             ▼                   ▼                                   │
│  ┌──────────────────┐  ┌──────────────────┐                       │
│  │  AWS Glue Data   │  │  Amazon SNS      │                       │
│  │  Catalog         │  │  - Alerts        │                       │
│  │  - Metadata      │  │  - Notifications │                       │
│  └──────────────────┘  └──────────────────┘                       │
│             │                                                       │
│             ▼                                                       │
│  ┌──────────────────────────────────────────────────────┐         │
│  │              Amazon Athena                            │         │
│  │              - SQL Analytics                          │         │
│  └──────────────────────────────────────────────────────┘         │
│                                                                     │
│  ┌──────────────────────────────────────────────────────┐         │
│  │           Amazon CloudWatch                           │         │
│  │           - Logs, Metrics, Alarms                     │         │
│  └──────────────────────────────────────────────────────┘         │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Component Design

### 1. Schema Detection Engine

**Purpose**: Detect schema changes between incoming data and catalog

**Components**:

```python
SchemaDetector
├── get_catalog_schema()      # Retrieve schema from Glue
├── detect_schema_changes()   # Compare schemas
└── _compare_schemas()        # Detailed comparison logic

SchemaChange (Data Class)
├── change_type: Enum         # Type of change
├── column_name: str          # Affected column
├── old_datatype: str         # Previous type
└── new_datatype: str         # New type

SchemaComparisonResult
├── has_changes: bool         # Whether changes detected
├── changes: List             # List of changes
├── incoming_schema           # New schema
├── catalog_schema            # Existing schema
└── is_backward_compatible    # Compatibility flag
```

**Algorithm**:

1. Retrieve existing schema from Glue Data Catalog
2. Extract schema from incoming DataFrame
3. Compare column names (detect new/missing)
4. Compare datatypes (detect changes)
5. Compare column order
6. Assess backward compatibility
7. Return comprehensive comparison result

### 2. Schema Evolution Handler

**Purpose**: Apply transformations to handle schema changes

**Components**:

```python
SchemaEvolutionHandler
├── apply_schema_evolution()     # Main orchestrator
├── _add_missing_columns()       # Add NULL columns
├── _handle_datatype_changes()   # Cast types
├── _reorder_columns()           # Fix column order
├── _update_glue_catalog()       # Sync catalog
└── _match_existing_schema()     # Strict mode handler
```

**Evolution Strategies**:

| Change Type | Action | Backward Compatible |
|-------------|--------|---------------------|
| New Column | Add to DataFrame, update catalog | ✅ Yes |
| Missing Column | Add with NULL values | ❌ No |
| Datatype Change | Safe cast if possible | ⚠️ Depends |
| Column Order | Reorder to match catalog | ✅ Yes |

### 3. Data Quality Validation Engine

**Purpose**: Validate data against configurable rules

**Components**:

```python
ValidationEngine
├── add_rule()                # Register rule
├── validate()                # Execute all rules
└── _apply_rule()             # Apply single rule

ValidationRule (Data Class)
├── rule_name: str            # Unique identifier
├── rule_type: Enum           # Type of validation
├── column_name: str          # Target column
├── severity: Enum            # ERROR/WARNING/INFO
└── parameters: Dict          # Rule-specific params

ValidationResult
├── rule_name: str
├── passed: bool
├── failed_count: int
├── total_count: int
└── failure_percentage: float
```

**Rule Types**:

1. **NOT_NULL**: Column must have a value
2. **UNIQUE**: No duplicate values allowed
3. **RANGE**: Numeric value within bounds
4. **DATATYPE**: Value matches expected type
5. **PATTERN**: Value matches regex pattern
6. **CUSTOM**: User-defined validation logic

### 4. Metrics Collection & Reporting

**Purpose**: Generate comprehensive data quality metrics

**Components**:

```python
MetricsCollector
├── collect_metrics()         # Gather all metrics
├── _count_duplicates()       # Detect duplicates
└── _count_nulls()            # Count NULL values

DataQualityMetrics
├── total_records: int
├── valid_records: int
├── invalid_records: int
├── duplicate_records: int
├── null_counts: Dict
├── validation_results: List
└── data_quality_score: float

MetricsReporter
├── save_metrics_to_s3()      # Persist metrics
├── check_quality_thresholds() # Evaluate thresholds
└── send_sns_alert()          # Send notifications
```

**Quality Score Formula**:

```
Score = (Validity × 0.4) + (Uniqueness × 0.3) + (Completeness × 0.3)

Where:
  Validity = (valid_records / total_records) × 100
  Uniqueness = ((total - duplicates) / total) × 100
  Completeness = ((total_cells - null_cells) / total_cells) × 100
```

---

## Data Flow

### End-to-End Processing Flow

```
┌─────────────────────────────────────────────────────────────────┐
│ STEP 1: File Upload                                             │
├─────────────────────────────────────────────────────────────────┤
│ User uploads file → s3://bucket/raw/orders/file.csv            │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 2: S3 Event Trigger                                        │
├─────────────────────────────────────────────────────────────────┤
│ S3 sends event notification → Lambda function                   │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 3: Lambda Validation                                       │
├─────────────────────────────────────────────────────────────────┤
│ - Check file extension (.csv, .json, .parquet)                 │
│ - Verify file size > 0                                          │
│ - Confirm correct S3 prefix                                     │
│ - Skip temp/hidden files                                        │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 4: Trigger Glue Job                                        │
├─────────────────────────────────────────────────────────────────┤
│ Lambda calls: glue.start_job_run()                             │
│ Parameters: INPUT_PATH, GLUE_DATABASE                           │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 5: Read Raw Data                                           │
├─────────────────────────────────────────────────────────────────┤
│ Spark reads file into DataFrame                                 │
│ Schema inferred from data                                       │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 6: Schema Detection                                        │
├─────────────────────────────────────────────────────────────────┤
│ SchemaDetector:                                                 │
│ 1. Get catalog schema (if exists)                              │
│ 2. Compare with incoming schema                                │
│ 3. Identify changes:                                            │
│    - New columns: [shipping_method]                            │
│    - Missing columns: []                                        │
│    - Datatype changes: []                                       │
│    - Column order changes: No                                   │
│ 4. Return SchemaComparisonResult                               │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 7: Schema Evolution                                        │
├─────────────────────────────────────────────────────────────────┤
│ SchemaEvolutionHandler:                                         │
│ IF new columns:                                                 │
│   - Include in DataFrame                                        │
│   - Update Glue catalog                                         │
│ IF missing columns:                                             │
│   - Add with NULL values                                        │
│ IF datatype changes:                                            │
│   - Cast to target type                                         │
│ IF order changes:                                               │
│   - Reorder columns                                             │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 8: Data Quality Validation                                 │
├─────────────────────────────────────────────────────────────────┤
│ ValidationEngine:                                               │
│ FOR EACH rule:                                                  │
│   1. Apply validation logic                                     │
│   2. Add validation column (_valid_rule_name)                  │
│   3. Count failures                                             │
│                                                                  │
│ Example Results:                                                │
│   - order_id_not_null: PASS (100/100)                         │
│   - order_id_unique: FAIL (98/100) - 2 duplicates             │
│   - price_positive: FAIL (99/100) - 1 negative                │
│                                                                  │
│ Separate DataFrames:                                            │
│   - valid_df: Records passing ALL ERROR rules                  │
│   - invalid_df: Records failing ANY ERROR rule                 │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 9: Add Partition Columns                                   │
├─────────────────────────────────────────────────────────────────┤
│ Extract from order_date:                                        │
│   - year = 2024                                                 │
│   - month = 1                                                   │
│   - day = 15                                                    │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 10: Write to Clean Layer                                   │
├─────────────────────────────────────────────────────────────────┤
│ valid_df.write                                                  │
│   .mode("append")                                               │
│   .partitionBy("year", "month", "day")                         │
│   .format("parquet")                                            │
│   .option("compression", "snappy")                              │
│   .save("s3://bucket/clean/orders/")                           │
│                                                                  │
│ Result: 97 records written                                      │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 11: Write to Quarantine (if invalid records exist)         │
├─────────────────────────────────────────────────────────────────┤
│ invalid_df                                                      │
│   .withColumn("quarantine_timestamp", current_timestamp())     │
│   .write                                                        │
│   .mode("append")                                               │
│   .partitionBy("year", "month", "day")                         │
│   .save("s3://bucket/quarantine/orders_invalid/")             │
│                                                                  │
│ Result: 3 records quarantined                                   │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 12: Collect Metrics                                        │
├─────────────────────────────────────────────────────────────────┤
│ MetricsCollector:                                               │
│   - total_records: 100                                          │
│   - valid_records: 97                                           │
│   - invalid_records: 3                                          │
│   - duplicate_records: 2                                        │
│   - null_counts: {order_id: 0, price: 0, ...}                 │
│   - data_quality_score: 94.5                                    │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 13: Save Metrics to S3                                     │
├─────────────────────────────────────────────────────────────────┤
│ MetricsReporter.save_metrics_to_s3()                           │
│ → s3://bucket/logs/data_quality_reports/orders/                │
│   metrics_20240115_120000.json                                  │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 14: Check Quality Thresholds                               │
├─────────────────────────────────────────────────────────────────┤
│ MetricsReporter.check_quality_thresholds()                     │
│                                                                  │
│ Checks:                                                         │
│   - Invalid %: 3% < 5% threshold ✅ PASS                       │
│   - Duplicate %: 2% = 2% threshold ⚠️ AT LIMIT                │
│   - Quality score: 94.5 > 85 threshold ✅ PASS                 │
│                                                                  │
│ Result: No alerts triggered                                     │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 15: Send Alerts (if thresholds exceeded)                   │
├─────────────────────────────────────────────────────────────────┤
│ IF alerts exist:                                                │
│   MetricsReporter.send_sns_alert()                             │
│   → SNS Topic → Email notification                              │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 16: Job Complete                                           │
├─────────────────────────────────────────────────────────────────┤
│ Log summary to CloudWatch                                       │
│ Update job status to SUCCEEDED                                  │
│ Data ready for analytics via Athena                            │
└─────────────────────────────────────────────────────────────────┘
```

---

## Schema Evolution Logic

### Decision Tree

```
┌─────────────────────────────────────┐
│  Incoming Schema vs Catalog Schema  │
└─────────────┬───────────────────────┘
              │
              ▼
        ┌──────────┐
        │ Changes? │
        └─┬──────┬─┘
          │      │
       No │      │ Yes
          │      │
          ▼      ▼
     ┌────────┐  ┌─────────────────┐
     │ Return │  │ Strict Mode ON? │
     │   DF   │  └─┬─────────────┬─┘
     └────────┘    │             │
                Yes│             │No
                   │             │
                   ▼             ▼
              ┌────────┐  ┌──────────────────┐
              │ FAIL   │  │ Evolution Allowed?│
              │  Job   │  └─┬──────────────┬─┘
              └────────┘    │              │
                         Yes│              │No
                            │              │
                            ▼              ▼
                     ┌─────────────┐  ┌────────────┐
                     │Apply Changes│  │Match Schema│
                     └──────┬──────┘  └──────┬─────┘
                            │                │
                            ▼                ▼
                     ┌─────────────┐  ┌────────────┐
                     │Update Catalog│  │Drop New Cols│
                     └──────┬──────┘  └──────┬─────┘
                            │                │
                            └────────┬───────┘
                                     │
                                     ▼
                              ┌──────────┐
                              │ Return DF│
                              └──────────┘
```

---

## Performance Considerations

### Optimization Strategies

1. **Partitioning**
   - Data partitioned by year/month/day
   - Enables partition pruning in queries
   - Reduces scan costs in Athena

2. **File Format**
   - Parquet: Columnar storage
   - Snappy compression: Balance speed/size
   - Predicate pushdown support

3. **Spark Configuration**
   ```python
   spark.conf.set("spark.sql.shuffle.partitions", 200)
   spark.conf.set("spark.default.parallelism", 100)
   ```

4. **Caching**
   - Schema cached in memory during job
   - Avoid repeated catalog lookups

5. **Broadcast Joins**
   - Small lookup tables broadcast
   - Reduces shuffle operations

---

## Security Architecture

### Defense in Depth

1. **Network Security**
   - VPC for Glue jobs (optional)
   - Private subnets
   - Security groups

2. **Data Encryption**
   - S3: Server-side encryption (AES-256)
   - In-transit: TLS/SSL
   - At-rest: Encrypted storage

3. **Access Control**
   - IAM roles with least privilege
   - Resource-based policies
   - No hardcoded credentials

4. **Audit Logging**
   - CloudTrail for API calls
   - CloudWatch for application logs
   - S3 access logs

---

## Error Handling

### Error Handling Strategy

```python
try:
    # Main processing logic
    result = process_data()
except SpecificException as e:
    logger.error(f"Specific error: {e}")
    # Handle specific case
    handle_specific_error(e)
except Exception as e:
    logger.error(f"Unexpected error: {e}", exc_info=True)
    # General error handling
    send_alert(e)
    raise
finally:
    # Cleanup resources
    cleanup()
```

### Failure Modes

| Failure Type | Handling | Recovery |
|--------------|----------|----------|
| Schema incompatible | Log + quarantine | Manual review |
| Validation failure | Quarantine records | Reprocess after fix |
| Catalog update fail | Log warning | Continue processing |
| S3 write failure | Retry + fail job | Automatic retry |
| SNS send failure | Log error | Continue (non-critical) |

---

**Architecture designed for reliability, scalability, and maintainability** 🏗️
