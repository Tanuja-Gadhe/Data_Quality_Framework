# AWS Data Lake Schema Evolution and Data Quality Framework

A production-grade data engineering framework for building resilient data pipelines that automatically detect schema changes, validate data quality, and process data into a clean data lake.

![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![AWS](https://img.shields.io/badge/AWS-Glue%20%7C%20S3%20%7C%20Lambda-orange)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.4-red)
![License](https://img.shields.io/badge/License-MIT-green)

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Data Lake Structure](#data-lake-structure)
- [Pipeline Workflow](#pipeline-workflow)
- [Schema Evolution](#schema-evolution)
- [Data Quality Framework](#data-quality-framework)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Deployment](#deployment)
- [Usage](#usage)
- [Monitoring](#monitoring)
- [Testing](#testing)
- [Configuration](#configuration)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

---

## Overview

This framework provides an enterprise-ready solution for managing schema evolution and data quality in AWS data lakes. It automatically handles schema drift, validates data quality, quarantines invalid records, and generates comprehensive metrics.

### Key Benefits

- **Automatic Schema Evolution**: Detects and handles schema changes without manual intervention
- **Data Quality Assurance**: Validates data against configurable rules before loading
- **Quarantine Layer**: Isolates invalid records for investigation and reprocessing
- **Comprehensive Metrics**: Generates detailed data quality reports and alerts
- **Production-Ready**: Built with error handling, logging, and monitoring
- **Scalable**: Leverages Apache Spark for distributed processing

---

## Architecture

```
┌─────────────────┐
│  Incoming Files │
│   (CSV/JSON)    │
└────────┬────────┘
         │
         ▼
┌─────────────────────────┐
│  S3 Raw Landing Zone    │
│  s3://bucket/raw/       │
└────────┬────────────────┘
         │
         ▼ (S3 Event)
┌─────────────────────────┐
│  Lambda Trigger         │
│  - Validates file       │
│  - Triggers Glue job    │
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────────────────────────┐
│         AWS Glue Spark ETL Job              │
│  ┌─────────────────────────────────────┐   │
│  │  1. Schema Detection Engine         │   │
│  │     - Compare with catalog          │   │
│  │     - Identify changes              │   │
│  └─────────────────────────────────────┘   │
│  ┌─────────────────────────────────────┐   │
│  │  2. Schema Evolution Handler        │   │
│  │     - Add missing columns           │   │
│  │     - Handle datatype changes       │   │
│  │     - Reorder columns               │   │
│  └─────────────────────────────────────┘   │
│  ┌─────────────────────────────────────┐   │
│  │  3. Data Quality Validation         │   │
│  │     - NOT NULL checks               │   │
│  │     - Uniqueness validation         │   │
│  │     - Range checks                  │   │
│  │     - Pattern matching              │   │
│  └─────────────────────────────────────┘   │
└────────┬──────────────────────┬─────────────┘
         │                      │
         │ (Valid)              │ (Invalid)
         ▼                      ▼
┌──────────────────┐   ┌──────────────────┐
│  Clean Layer     │   │  Quarantine      │
│  (Parquet)       │   │  (Parquet)       │
│  Partitioned     │   │  For Review      │
└────────┬─────────┘   └──────────────────┘
         │
         ▼
┌──────────────────┐
│  Athena          │
│  Analytics       │
└──────────────────┘
         │
         ▼
┌──────────────────────────────────────┐
│  Monitoring & Alerts                 │
│  - CloudWatch Logs                   │
│  - Data Quality Metrics (S3)         │
│  - SNS Alerts (threshold exceeded)   │
└──────────────────────────────────────┘
```

---

## Features

### 1. Schema Detection Engine

Automatically detects schema changes in incoming data:

- **New Columns**: Identifies columns not present in the catalog
- **Missing Columns**: Detects columns that were removed
- **Datatype Changes**: Identifies type mismatches
- **Column Order Changes**: Detects reordering of columns

### 2. Schema Evolution Handling

Handles schema changes intelligently:

- **New Column**: Automatically adds to DataFrame and updates catalog
- **Missing Column**: Creates column with NULL values
- **Datatype Change**: Safely casts to target type
- **Column Order**: Reorders to match expected schema

### 3. Data Quality Validation

Comprehensive validation framework:

- **NOT NULL Validation**: Ensures critical fields are populated
- **Uniqueness Checks**: Detects duplicate records
- **Range Validation**: Validates numeric ranges
- **Datatype Validation**: Ensures correct data types
- **Pattern Matching**: Validates against regex patterns

### 4. Quarantine Layer

Invalid records are automatically:

- Separated from valid data
- Written to quarantine location
- Tagged with quarantine timestamp
- Available for investigation and reprocessing

### 5. Data Quality Metrics

Generates comprehensive metrics:

- Total records processed
- Valid vs invalid record counts
- Duplicate detection
- Null value statistics
- Overall data quality score (0-100)
- Validation rule results

### 6. Monitoring and Alerts

Production-ready monitoring:

- CloudWatch logs for all operations
- Custom metrics for pipeline health
- SNS alerts when quality thresholds exceeded
- Detailed error tracking

---

## Data Lake Structure

```
s3://data-lake-bucket/
├── raw/                          # Landing zone for incoming files
│   └── orders/
│       ├── 2024/
│       │   ├── 01/
│       │   │   └── sample_orders.csv
│       │   └── 02/
│       └── README.md
│
├── clean/                        # Validated, clean data
│   └── orders/
│       └── year=2024/
│           └── month=01/
│               └── day=15/
│                   └── part-00000.snappy.parquet
│
├── curated/                      # Aggregated business metrics
│   └── sales_metrics/
│
├── quarantine/                   # Invalid records
│   └── orders_invalid/
│       └── year=2024/
│           └── month=01/
│               └── day=15/
│                   └── part-00000.snappy.parquet
│
├── logs/                         # Data quality reports
│   └── data_quality_reports/
│       └── orders/
│           └── metrics_20240115_120000.json
│
└── scripts/                      # ETL job scripts
    ├── orders_etl_job.py
    └── dependencies.zip
```

---

## Pipeline Workflow

### Step-by-Step Process

1. **File Upload**: Data file uploaded to `s3://bucket/raw/orders/`
2. **S3 Event Trigger**: S3 sends event notification to Lambda
3. **Lambda Validation**: Lambda validates file and triggers Glue job
4. **Schema Detection**: Glue job compares incoming schema with catalog
5. **Schema Evolution**: Applies transformations to handle schema changes
6. **Data Quality Validation**: Runs validation rules on all records
7. **Record Separation**: Splits valid and invalid records
8. **Clean Layer Write**: Valid records written to clean layer (Parquet)
9. **Quarantine Write**: Invalid records written to quarantine
10. **Metrics Generation**: Collects and saves data quality metrics
11. **Threshold Check**: Compares metrics against thresholds
12. **Alert Generation**: Sends SNS alert if thresholds exceeded
13. **Catalog Update**: Updates Glue Data Catalog with new schema

---

## Schema Evolution

### How It Works

The framework detects and handles four types of schema changes:

#### 1. New Column Added

**Scenario**: Incoming data has a new column `shipping_method`

**Action**:
- Column is automatically included in the DataFrame
- Glue catalog is updated with new column definition
- Historical data will have NULL for this column

**Example**:
```python
# Before: order_id, customer_id, price
# After:  order_id, customer_id, price, shipping_method
```

#### 2. Column Removed

**Scenario**: Incoming data is missing the `region` column

**Action**:
- Column is added to DataFrame with NULL values
- Data is processed normally
- Downstream systems see consistent schema

**Example**:
```python
# Expected: order_id, customer_id, price, region
# Incoming: order_id, customer_id, price
# Result:   order_id, customer_id, price, region (NULL)
```

#### 3. Datatype Changed

**Scenario**: `quantity` changed from INT to BIGINT

**Action**:
- Column is safely cast to target type
- Invalid casts are logged
- Original value retained if cast fails

#### 4. Column Order Changed

**Scenario**: Columns appear in different order

**Action**:
- Columns are reordered to match catalog schema
- New columns appended at the end
- Ensures consistent output format

### Configuration

Control schema evolution behavior:

```python
# config/config.py
ALLOW_SCHEMA_EVOLUTION = True   # Enable automatic evolution
STRICT_MODE = False              # Fail on any schema change
```

---

## Data Quality Framework

### Validation Rules

#### Built-in Rules for Orders Table

1. **order_id_not_null**: Order ID must not be null (ERROR)
2. **order_id_unique**: Order ID must be unique (ERROR)
3. **price_positive**: Price must be >= 0 (ERROR)
4. **quantity_positive**: Quantity must be >= 1 (ERROR)
5. **order_date_not_null**: Order date must not be null (ERROR)
6. **customer_id_not_null**: Customer ID must not be null (ERROR)

### Custom Validation Rules

Create custom rules:

```python
from data_quality import ValidationRule, ValidationRuleType, ValidationSeverity

custom_rule = ValidationRule(
    rule_name="email_format",
    rule_type=ValidationRuleType.PATTERN,
    column_name="email",
    severity=ValidationSeverity.ERROR,
    parameters={"pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"},
    description="Email must be valid format"
)

validation_engine.add_rule(custom_rule)
```

### Data Quality Score

The framework calculates an overall quality score (0-100):

- **Validity (40%)**: Percentage of valid records
- **Uniqueness (30%)**: Percentage of unique records
- **Completeness (30%)**: Percentage of non-null values

### Quality Thresholds

Configure acceptable thresholds:

```python
# config/config.py
MAX_INVALID_RECORDS_PERCENTAGE = 5.0    # Max 5% invalid records
MAX_DUPLICATE_RECORDS_PERCENTAGE = 2.0  # Max 2% duplicates
```

If thresholds are exceeded, SNS alerts are sent.

---

## Project Structure

```
aws-schema-evolution-framework/
├── config/
│   ├── __init__.py
│   └── config.py                    # Configuration settings
│
├── schema_engine/
│   ├── __init__.py
│   ├── schema_detector.py           # Schema change detection
│   └── schema_evolution.py          # Schema evolution handling
│
├── data_quality/
│   ├── __init__.py
│   ├── validation_rules.py          # Validation rule engine
│   └── quality_metrics.py           # Metrics collection and reporting
│
├── glue_jobs/
│   └── orders_etl_job.py            # Main Glue ETL job
│
├── lambda/
│   ├── __init__.py
│   └── trigger_pipeline.py          # Lambda trigger function
│
├── infrastructure/
│   ├── iam_roles.md                 # IAM role documentation
│   └── cloudformation_template.yaml # CloudFormation template
│
├── datasets/
│   ├── sample_orders.csv            # Clean sample data
│   ├── sample_orders_with_new_column.csv
│   ├── sample_orders_with_invalid_data.csv
│   └── README.md
│
├── sql/
│   └── athena_queries.sql           # Athena SQL queries
│
├── tests/
│   └── (test files)
│
├── requirements.txt                  # Python dependencies
├── setup.py                          # Package setup
├── Makefile                          # Build automation
├── .gitignore
└── README.md                         # This file
```

---

## Prerequisites

### AWS Services

- AWS Account with appropriate permissions
- Access to AWS Management Console
- S3 bucket for data lake
- AWS Glue (Data Catalog and ETL)
- AWS Lambda
- Amazon Athena
- Amazon SNS (for alerts)
- Amazon CloudWatch (for logging)

### Local Development

- Python 3.9 or higher (for packaging code)
- Git (for cloning repository)

---

## Installation

### 1. Clone Repository

```bash
git clone https://github.com/your-org/aws-schema-evolution-framework.git
cd aws-schema-evolution-framework
```

### 2. Package Dependencies Locally

```bash
# Install dependencies for packaging
pip install -r requirements.txt -t /tmp/python-packages

# Create deployment package
cd /tmp/python-packages && zip -r ~/dependencies.zip . && cd -

# Add project modules
cd schema_engine && zip -r ~/dependencies.zip . && cd ..
cd data_quality && zip -r ~/dependencies.zip . && cd ..
cd config && zip -r ~/dependencies.zip . && cd ..

# Package Lambda function
cd lambda && zip lambda_function.zip trigger_pipeline.py && cd ..
```

### 3. Access AWS Console

- Open your web browser
- Navigate to https://console.aws.amazon.com
- Sign in with your AWS credentials
- Select your preferred region from the top-right dropdown

---

## Deployment

### Complete Deployment Guide

For detailed step-by-step instructions using AWS Management Console, see:

**[DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md)** - Complete manual deployment using AWS Console UI

### Quick Overview

The deployment involves these main steps:

1. **Create S3 Bucket** - Set up data lake storage with folder structure
2. **Create IAM Roles** - Configure permissions for Glue and Lambda
3. **Create Glue Database** - Set up data catalog
4. **Create SNS Topic** - Configure email alerts
5. **Create Glue Job** - Deploy ETL processing job
6. **Create Lambda Function** - Deploy trigger function
7. **Configure S3 Events** - Connect S3 to Lambda trigger

Each step is detailed with screenshots-style instructions in the deployment guide

---

## Usage

### Running the Pipeline

#### 1. Upload Data File

1. **Navigate to S3 Console**
   - Open AWS Management Console
   - Search for "S3" and click on the S3 service

2. **Upload Sample Data**
   - Click on your data lake bucket
   - Navigate to `raw/orders/` folder
   - Click **"Upload"**
   - Click **"Add files"** and select `datasets/sample_orders.csv`
   - Click **"Upload"**

The pipeline will automatically:
- Detect the new file via S3 event
- Trigger Lambda function
- Start Glue ETL job
- Process data through all stages
- Generate metrics and alerts

#### 2. Monitor Execution

**Monitor Lambda Function:**
1. Navigate to **Lambda Console**
2. Click on `trigger-pipeline` function
3. Click **"Monitor"** tab
4. Click **"View CloudWatch logs"**
5. Click on the most recent log stream

**Monitor Glue Job:**
1. Navigate to **AWS Glue Console**
2. Click **"Jobs"** in the left sidebar
3. Click on `orders-etl-job`
4. Click **"Runs"** tab
5. View the latest run status and logs

#### 3. Query Results with Athena

1. **Navigate to Athena Console**
   - Search for "Athena" in AWS Console
   - Click on **"Amazon Athena"**

2. **Set Query Result Location** (first time only)
   - Click **"Settings"** tab
   - Click **"Manage"**
   - Set location: `s3://your-bucket-name/athena-results/`
   - Click **"Save"**

3. **Create External Table** (first time only)

```sql
CREATE EXTERNAL TABLE data_lake_db.clean_orders (
    order_id STRING,
    customer_id STRING,
    product_name STRING,
    quantity INT,
    price DECIMAL(10,2),
    order_date DATE,
    status STRING,
    region STRING
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
LOCATION 's3://your-data-lake-bucket/clean/orders/';
```

4. **Discover Partitions**

```sql
MSCK REPAIR TABLE data_lake_db.clean_orders;
```

5. **Query Data**

```sql
SELECT * FROM data_lake_db.clean_orders LIMIT 10;
```

### Testing Schema Evolution

1. **Upload Data with New Column**
   - Navigate to **S3 Console**
   - Go to `raw/orders/` folder
   - Upload `datasets/sample_orders_with_new_column.csv`

2. **Monitor Schema Changes**
   - Navigate to **CloudWatch Console**
   - Click **"Log groups"**
   - Click on `/aws-glue/jobs/orders-etl-job`
   - Search logs for "Schema changes detected"

3. **Verify Catalog Update**
   - Navigate to **AWS Glue Console**
   - Click **"Tables"** under **"Data Catalog"**
   - Select `data_lake_db` database
   - Click on `orders_clean` table
   - Verify new column `shipping_method` appears in schema

### Testing Data Quality

1. **Upload Data with Quality Issues**
   - Navigate to **S3 Console**
   - Go to `raw/orders/` folder
   - Upload `datasets/sample_orders_with_invalid_data.csv`

2. **Check Quarantine Layer**
   - In **S3 Console**, navigate to `quarantine/orders_invalid/`
   - Verify invalid records are stored here
   - Download and review quarantined data

3. **View Quality Metrics**
   - Navigate to `logs/data_quality_reports/orders/`
   - Download the latest JSON metrics file
   - Review quality score and validation results

4. **Check Email Alert**
   - You should receive an email alert (invalid records exceed 5% threshold)

---

## Monitoring

### CloudWatch Dashboards

1. **Navigate to CloudWatch Console**
   - Search for "CloudWatch" in AWS Console
   - Click on **"Dashboards"** in the left sidebar
   - Click **"Create dashboard"**

2. **Create Dashboard**
   - **Dashboard name**: `DataLakePipeline`
   - Click **"Create dashboard"**

3. **Add Widgets**
   - Click **"Add widget"**
   - Select widget type (Line, Number, etc.)
   - Add metrics for Glue jobs, Lambda, and custom metrics
   - Click **"Create widget"**

### Key Metrics to Monitor

1. **Glue Job Metrics**
   - Navigate to **CloudWatch** → **"Metrics"** → **"Glue"**
   - Monitor:
     - Job duration
     - Number of records processed
     - Job failures

2. **Data Quality Metrics**
   - Navigate to **CloudWatch** → **"Metrics"** → **"Custom Namespaces"**
   - Look for `DataLake/SchemaEvolution`
   - Monitor:
     - Invalid record percentage
     - Duplicate record count
     - Quality score

3. **Lambda Metrics**
   - Navigate to **Lambda Console** → Select function → **"Monitor"** tab
   - Monitor:
     - Invocation count
     - Error rate
     - Duration

### Configure CloudWatch Alarms

1. **Navigate to CloudWatch Console**
   - Click **"Alarms"** → **"All alarms"**
   - Click **"Create alarm"**

2. **Create Glue Job Failure Alarm**
   - Click **"Select metric"**
   - Choose **"Glue"** → **"Job Metrics"**
   - Select `glue.driver.aggregate.numFailedTasks` for `orders-etl-job`
   - Click **"Select metric"**
   - **Threshold**: Static, Greater than or equal to `1`
   - **Period**: 5 minutes
   - Click **"Next"**

3. **Configure Actions**
   - **Alarm state trigger**: In alarm
   - **SNS topic**: Select `data-quality-alerts`
   - Click **"Next"**

4. **Name and Create**
   - **Alarm name**: `glue-job-failures`
   - Click **"Create alarm"**

---

## Testing

### Local Unit Tests

```bash
# Run unit tests locally
make test

# Run linters
make lint

# Format code
make format
```

### Integration Testing via AWS Console

1. **Test Basic Pipeline**
   - Upload `datasets/sample_orders.csv` to S3 `raw/orders/` folder
   - Monitor Glue job execution in AWS Glue Console
   - Verify output in `clean/orders/` folder

2. **Test Schema Evolution**
   - Upload `datasets/sample_orders_with_new_column.csv`
   - Check CloudWatch logs for schema change detection
   - Verify new column in Glue catalog table

3. **Test Data Quality**
   - Upload `datasets/sample_orders_with_invalid_data.csv`
   - Check `quarantine/orders_invalid/` for invalid records
   - Verify email alert received
   - Review quality metrics in S3

---

## Configuration

### Environment Variables

Set in Glue job or Lambda:

```bash
# Glue Job
DATA_LAKE_BUCKET=your-bucket-name
GLUE_DATABASE=data_lake_db
SNS_TOPIC_ARN=arn:aws:sns:region:account:topic
ALLOW_SCHEMA_EVOLUTION=true
STRICT_MODE=false
MAX_INVALID_PERCENTAGE=5.0

# Lambda
GLUE_JOB_NAME=orders-etl-job
RAW_DATA_PREFIX=raw/orders/
ENABLE_NOTIFICATIONS=true
```

### Configuration File

Edit `config/config.py`:

```python
class Config:
    S3_BUCKET = "your-bucket-name"
    GLUE_DATABASE = "data_lake_db"
    MAX_INVALID_RECORDS_PERCENTAGE = 5.0
    ALLOW_SCHEMA_EVOLUTION = True
    STRICT_MODE = False
```

---

## Best Practices

### 1. Schema Management

- Use consistent naming conventions
- Document schema changes
- Test schema evolution in dev environment first
- Version your schemas

### 2. Data Quality

- Define validation rules early
- Set appropriate thresholds
- Review quarantined data regularly
- Implement data quality monitoring

### 3. Performance

- Partition data appropriately
- Use Parquet with Snappy compression
- Optimize Spark configurations
- Monitor job metrics

### 4. Security

- Enable S3 bucket encryption
- Use IAM roles with least privilege
- Enable CloudTrail logging
- Rotate access keys regularly

### 5. Cost Optimization

- Use appropriate Glue worker types
- Set job timeouts
- Archive old data to Glacier
- Monitor and optimize queries

---

## Troubleshooting

### Common Issues

#### 1. Glue Job Fails to Start

**Symptoms**: Job stays in "STARTING" state

**Solutions**:
- Check IAM role permissions
- Verify S3 script location
- Check VPC/subnet configuration

#### 2. Schema Evolution Not Working

**Symptoms**: New columns not added to catalog

**Solutions**:
- Verify `ALLOW_SCHEMA_EVOLUTION=true`
- Check Glue catalog permissions
- Review CloudWatch logs for errors

#### 3. All Records Going to Quarantine

**Symptoms**: No valid records in clean layer

**Solutions**:
- Review validation rules
- Check data format
- Verify column names match rules

#### 4. SNS Alerts Not Received

**Symptoms**: No email alerts despite threshold breach

**Solutions**:
- Confirm email subscription
- Check SNS topic permissions
- Verify `ENABLE_ALERTS=true`

### Debug Mode

Enable verbose logging:

```python
# In config/config.py
import logging
logging.basicConfig(level=logging.DEBUG)
```

### View Detailed Logs

**View Glue Job Logs:**
1. Navigate to **CloudWatch Console**
2. Click **"Log groups"**
3. Click on `/aws-glue/jobs/orders-etl-job`
4. Click on the latest log stream
5. Use the search bar to filter specific messages

**View Lambda Logs:**
1. Navigate to **CloudWatch Console**
2. Click **"Log groups"**
3. Click on `/aws/lambda/trigger-pipeline`
4. Click on the latest log stream
5. Review execution details and errors

---

## Contributing

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Code Standards

- Follow PEP 8 style guide
- Add docstrings to all functions
- Write unit tests for new features
- Update documentation

---

## License

This project is licensed under the MIT License - see the LICENSE file for details.

---

## Support

For questions or issues:

- Open a GitHub issue
- Contact the data engineering team
- Check the troubleshooting guide

---

## Acknowledgments

- AWS Glue documentation
- Apache Spark community
- Data engineering best practices from industry leaders

---

## Roadmap

Future enhancements:

- [ ] Support for additional file formats (Avro, ORC)
- [ ] Real-time streaming support with Kinesis
- [ ] Machine learning-based anomaly detection
- [ ] Auto-scaling based on workload
- [ ] Multi-region replication
- [ ] Data lineage tracking
- [ ] Advanced data profiling

---

**Built with ❤️ by Tanuja Gadhe**
