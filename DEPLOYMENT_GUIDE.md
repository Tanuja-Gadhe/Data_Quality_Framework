# Deployment Guide

Complete manual step-by-step guide for deploying the AWS Data Lake Schema Evolution Framework using AWS Management Console (UI).

---

## Pre-Deployment Checklist

- [ ] AWS Account with admin access
- [ ] Access to AWS Management Console
- [ ] Python 3.9+ installed locally (for packaging code)
- [ ] Git installed (for cloning repository)
- [ ] S3 bucket name decided (must be globally unique)
- [ ] Email address for alerts
- [ ] AWS region selected (e.g., us-east-1)

---

## Deployment Steps

### Step 0: Prepare Local Environment

```bash
# Clone repository
git clone https://github.com/your-org/aws-schema-evolution-framework.git
cd aws-schema-evolution-framework

# Package Python dependencies for Glue
pip install -r requirements.txt -t /tmp/python-packages
cd /tmp/python-packages && zip -r ~/dependencies.zip . && cd -

# Add project modules to dependencies
cd schema_engine && zip -r ~/dependencies.zip . && cd ..
cd data_quality && zip -r ~/dependencies.zip . && cd ..
cd config && zip -r ~/dependencies.zip . && cd ..

# Package Lambda function
cd lambda && zip lambda_function.zip trigger_pipeline.py && cd ..

echo "Code artifacts packaged and ready for upload"
```

**Note down these values for later steps:**
- Bucket name: `my-datalake-bucket-<unique-suffix>`
- Email: `your-email@example.com`
- Region: `us-east-1` (or your chosen region)

### Step 1: Create S3 Bucket

1. **Navigate to S3 Console**
   - Open AWS Management Console: https://console.aws.amazon.com
   - Search for "S3" in the search bar and click on "S3"

2. **Create Bucket**
   - Click **"Create bucket"** button
   - **Bucket name**: Enter a globally unique name (e.g., `my-datalake-bucket-20260316`)
   - **AWS Region**: Select your region (e.g., `us-east-1`)
   - **Object Ownership**: Keep default (ACLs disabled)
   - **Block Public Access settings**: Keep all boxes checked (block all public access)
   - **Bucket Versioning**: Select **"Enable"**
   - **Default encryption**: 
     - Encryption type: **"Server-side encryption with Amazon S3 managed keys (SSE-S3)"**
     - Bucket Key: **Enable**
   - Click **"Create bucket"**

3. **Create Folder Structure**
   - Click on your newly created bucket name
   - Click **"Create folder"** and create the following folders one by one:
     - `raw/orders/`
     - `clean/orders/`
     - `quarantine/orders_invalid/`
     - `logs/data_quality_reports/`
     - `logs/spark-logs/`
     - `scripts/`
     - `temp/`

4. **Upload Code Artifacts**
   - Navigate to the `scripts/` folder
   - Click **"Upload"**
   - Add files:
     - `orders_etl_job.py` (from `glue_jobs/` directory)
     - `dependencies.zip` (from `~/dependencies.zip`)
   - Click **"Upload"**

### Step 2: Create IAM Roles

#### 2.1: Create Glue Job Role

1. **Navigate to IAM Console**
   - Search for "IAM" in the AWS Console search bar
   - Click on **"Roles"** in the left sidebar
   - Click **"Create role"**

2. **Configure Trust Relationship**
   - **Trusted entity type**: Select **"AWS service"**
   - **Use case**: Select **"Glue"**
   - Click **"Next"**

3. **Attach Permissions Policies**
   - Search and select: **"AWSGlueServiceRole"** (AWS managed policy)
   - Click **"Next"**

4. **Name and Create Role**
   - **Role name**: `DataLakeGlueJobRole`
   - **Description**: `IAM role for AWS Glue ETL jobs in data lake`
   - Click **"Create role"**

5. **Add Custom Inline Policy**
   - Find and click on the newly created role `DataLakeGlueJobRole`
   - Click **"Add permissions"** → **"Create inline policy"**
   - Click **"JSON"** tab and paste:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3Access",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::YOUR-BUCKET-NAME/*",
        "arn:aws:s3:::YOUR-BUCKET-NAME"
      ]
    },
    {
      "Sid": "GlueAccess",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:GetPartitions",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:BatchCreatePartition",
        "glue:BatchUpdatePartition"
      ],
      "Resource": ["*"]
    },
    {
      "Sid": "CloudWatchLogs",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": ["arn:aws:logs:*:*:*"]
    },
    {
      "Sid": "CloudWatchMetrics",
      "Effect": "Allow",
      "Action": ["cloudwatch:PutMetricData"],
      "Resource": ["*"]
    },
    {
      "Sid": "SNSPublish",
      "Effect": "Allow",
      "Action": ["sns:Publish"],
      "Resource": ["*"]
    }
  ]
}
```

   - Replace `YOUR-BUCKET-NAME` with your actual bucket name
   - Click **"Next"**
   - **Policy name**: `GlueJobPermissions`
   - Click **"Create policy"**

#### 2.2: Create Lambda Trigger Role

1. **Create New Role**
   - In IAM Console, click **"Roles"** → **"Create role"**

2. **Configure Trust Relationship**
   - **Trusted entity type**: Select **"AWS service"**
   - **Use case**: Select **"Lambda"**
   - Click **"Next"**

3. **Attach Permissions Policies**
   - Search and select: **"AWSLambdaBasicExecutionRole"** (AWS managed policy)
   - Click **"Next"**

4. **Name and Create Role**
   - **Role name**: `DataLakeLambdaTriggerRole`
   - **Description**: `IAM role for Lambda function that triggers Glue jobs`
   - Click **"Create role"**

5. **Add Custom Inline Policy**
   - Find and click on the newly created role `DataLakeLambdaTriggerRole`
   - Click **"Add permissions"** → **"Create inline policy"**
   - Click **"JSON"** tab and paste:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3ReadAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::YOUR-BUCKET-NAME/raw/*",
        "arn:aws:s3:::YOUR-BUCKET-NAME"
      ]
    },
    {
      "Sid": "GlueJobTrigger",
      "Effect": "Allow",
      "Action": [
        "glue:StartJobRun",
        "glue:GetJobRun",
        "glue:GetJob"
      ],
      "Resource": ["*"]
    },
    {
      "Sid": "CloudWatchMetrics",
      "Effect": "Allow",
      "Action": ["cloudwatch:PutMetricData"],
      "Resource": ["*"]
    }
  ]
}
```

   - Replace `YOUR-BUCKET-NAME` with your actual bucket name
   - Click **"Next"**
   - **Policy name**: `LambdaTriggerPermissions`
   - Click **"Create policy"**

### Step 3: Create Glue Database

1. **Navigate to AWS Glue Console**
   - Search for "Glue" in the AWS Console search bar
   - Click on **"AWS Glue"**

2. **Create Database**
   - In the left sidebar, under **"Data Catalog"**, click **"Databases"**
   - Click **"Add database"**
   - **Database name**: `data_lake_db`
   - **Description**: `Data lake database for schema evolution framework`
   - **Location**: Leave empty (optional)
   - Click **"Create database"**

3. **Verify Database Creation**
   - You should see `data_lake_db` in the databases list

### Step 4: Create SNS Topic for Alerts

1. **Navigate to SNS Console**
   - Search for "SNS" in the AWS Console search bar
   - Click on **"Simple Notification Service"**

2. **Create Topic**
   - In the left sidebar, click **"Topics"**
   - Click **"Create topic"**
   - **Type**: Select **"Standard"**
   - **Name**: `data-quality-alerts`
   - **Display name**: `Data Quality Alerts`
   - **Tags** (optional):
     - Key: `Project`, Value: `DataLakeSchemaEvolution`
     - Key: `Environment`, Value: `Production`
   - Click **"Create topic"**

3. **Copy Topic ARN**
   - After creation, copy the **Topic ARN** (e.g., `arn:aws:sns:us-east-1:123456789012:data-quality-alerts`)
   - **Save this ARN** - you'll need it for Glue job configuration

4. **Create Email Subscription**
   - On the topic details page, click **"Create subscription"**
   - **Protocol**: Select **"Email"**
   - **Endpoint**: Enter your email address
   - Click **"Create subscription"**

5. **Confirm Email Subscription**
   - Check your email inbox
   - Click the confirmation link in the email from AWS
   - Return to SNS console and verify status shows **"Confirmed"**

### Step 5: Create AWS Glue ETL Job

1. **Navigate to AWS Glue Console**
   - Search for "Glue" in the AWS Console
   - Click on **"AWS Glue"**

2. **Create ETL Job**
   - In the left sidebar, under **"ETL Jobs"**, click **"Jobs"**
   - Click **"Create job"**

3. **Configure Job Properties**
   - **Name**: `orders-etl-job`
   - **IAM Role**: Select `DataLakeGlueJobRole` (created in Step 2)
   - **Type**: Select **"Spark"**
   - **Glue version**: Select **"Glue 4.0"**
   - **Language**: Select **"Python 3"**

4. **Configure Script**
   - **Script filename**: `orders_etl_job.py`
   - **Script path**: `s3://YOUR-BUCKET-NAME/scripts/orders_etl_job.py`
   - Replace `YOUR-BUCKET-NAME` with your actual bucket name

5. **Advanced Properties**
   - Expand **"Advanced properties"** section
   - **Python library path**: `s3://YOUR-BUCKET-NAME/scripts/dependencies.zip`
   - **Temporary directory**: `s3://YOUR-BUCKET-NAME/temp/`
   - **Spark event logs**: `s3://YOUR-BUCKET-NAME/logs/spark-logs/`

6. **Job Parameters**
   - Scroll to **"Job parameters"** section
   - Click **"Add new parameter"** for each of the following:
     - Key: `--GLUE_DATABASE`, Value: `data_lake_db`
     - Key: `--DATA_LAKE_BUCKET`, Value: `YOUR-BUCKET-NAME`
     - Key: `--SNS_TOPIC_ARN`, Value: `YOUR-SNS-TOPIC-ARN` (from Step 4)
     - Key: `--enable-metrics`, Value: `true`
     - Key: `--enable-continuous-cloudwatch-log`, Value: `true`
     - Key: `--enable-spark-ui`, Value: `true`

7. **Job Capacity**
   - **Worker type**: Select **"G.1X"**
   - **Number of workers**: `10`
   - **Job timeout**: `60` minutes
   - **Max retries**: `1`

8. **Monitoring and Logging**
   - **CloudWatch Logs**: Enable
   - **Spark UI**: Enable
   - **Job metrics**: Enable

9. **Create Job**
   - Click **"Save"** or **"Create job"**
   - Wait for job creation to complete

### Step 6: Create Lambda Function

1. **Navigate to Lambda Console**
   - Search for "Lambda" in the AWS Console
   - Click on **"Lambda"**

2. **Create Function**
   - Click **"Create function"**
   - Select **"Author from scratch"**
   - **Function name**: `trigger-pipeline`
   - **Runtime**: Select **"Python 3.11"** or **"Python 3.12"**
   - **Architecture**: Select **"x86_64"**

3. **Configure Permissions**
   - Expand **"Change default execution role"**
   - Select **"Use an existing role"**
   - **Existing role**: Select `DataLakeLambdaTriggerRole`
   - Click **"Create function"**

4. **Upload Function Code**
   - In the function page, scroll to **"Code source"** section
   - Click **"Upload from"** → **".zip file"**
   - Click **"Upload"** and select `lambda/lambda_function.zip`
   - Click **"Save"**

5. **Configure Function Settings**
   - Click on **"Configuration"** tab
   - Click **"General configuration"** → **"Edit"**
     - **Timeout**: `5` minutes `0` seconds
     - **Memory**: `256` MB
     - Click **"Save"**

6. **Add Environment Variables**
   - Still in **"Configuration"** tab, click **"Environment variables"**
   - Click **"Edit"** → **"Add environment variable"**
   - Add the following variables:
     - Key: `GLUE_JOB_NAME`, Value: `orders-etl-job`
     - Key: `GLUE_DATABASE`, Value: `data_lake_db`
     - Key: `RAW_DATA_PREFIX`, Value: `raw/orders/`
     - Key: `ENABLE_NOTIFICATIONS`, Value: `true`
   - Click **"Save"**

7. **Copy Lambda Function ARN**
   - At the top of the function page, copy the **Function ARN**
   - **Save this ARN** - you'll need it for S3 event configuration

### Step 7: Configure S3 Event Notification to Trigger Lambda

1. **Add Lambda Trigger Permission**
   - Go back to **Lambda Console**
   - Select your `trigger-pipeline` function
   - Click **"Configuration"** tab → **"Permissions"**
   - Scroll down to **"Resource-based policy statements"**
   - Click **"Add permissions"**
   - **Policy statement**:
     - **Statement ID**: `s3-trigger-permission`
     - **Principal**: `s3.amazonaws.com`
     - **Source ARN**: `arn:aws:s3:::YOUR-BUCKET-NAME` (replace with your bucket name)
     - **Action**: `lambda:InvokeFunction`
   - Click **"Save"**

2. **Configure S3 Event Notification**
   - Navigate to **S3 Console**
   - Click on your bucket name
   - Click **"Properties"** tab
   - Scroll down to **"Event notifications"** section
   - Click **"Create event notification"**

3. **Configure Event Details**
   - **Event name**: `TriggerPipelineOnUpload`
   - **Prefix**: `raw/orders/`
   - **Suffix**: Leave empty (or add `.csv` to trigger only on CSV files)
   - **Event types**: Check **"All object create events"** or specifically:
     - `s3:ObjectCreated:Put`
     - `s3:ObjectCreated:Post`
     - `s3:ObjectCreated:Copy`
     - `s3:ObjectCreated:CompleteMultipartUpload`

4. **Configure Destination**
   - **Destination**: Select **"Lambda function"**
   - **Lambda function**: Select `trigger-pipeline`
   - Click **"Save changes"**

5. **Verify Configuration**
   - You should see the event notification listed under **"Event notifications"**
   - Status should show as **"Enabled"**


---

## Post-Deployment Verification

### Step 8: Test the Pipeline

1. **Upload Sample Data**
   - Navigate to **S3 Console**
   - Open your bucket
   - Navigate to `raw/orders/` folder
   - Click **"Upload"**
   - Add the sample file: `datasets/sample_orders.csv`
   - Click **"Upload"**

2. **Monitor Lambda Execution**
   - Navigate to **Lambda Console**
   - Click on `trigger-pipeline` function
   - Click **"Monitor"** tab
   - Click **"View CloudWatch logs"**
   - Click on the most recent log stream
   - Verify the Lambda was triggered and started the Glue job

3. **Monitor Glue Job Execution**
   - Navigate to **AWS Glue Console**
   - Click **"Jobs"** in the left sidebar
   - Click on `orders-etl-job`
   - Click **"Runs"** tab
   - You should see a job run in **"Running"** or **"Succeeded"** state
   - Click on the run to view details and logs

4. **View CloudWatch Logs**
   - From the Glue job run details, click **"Logs"**
   - Or navigate to **CloudWatch Console** → **"Log groups"**
   - Find log group: `/aws-glue/jobs/orders-etl-job`
   - Click on the latest log stream to view execution logs

### Step 9: Verify Results

1. **Check Processed Data**
   - Navigate to **S3 Console**
   - Open your bucket
   - Navigate to `clean/orders/` folder
   - Verify that Parquet files have been created
   - Check the folder structure (should be partitioned by year/month/day)

2. **Check Data Quality Reports**
   - In S3, navigate to `logs/data_quality_reports/orders/`
   - Download the latest JSON report
   - Open and review the data quality metrics

3. **Verify Glue Catalog Tables**
   - Navigate to **AWS Glue Console**
   - Click **"Tables"** under **"Data Catalog"**
   - Select database: `data_lake_db`
   - Verify that `orders_clean` table exists
   - Click on the table to view schema and partitions

4. **Check SNS Notifications**
   - Check your email for any data quality alerts
   - Alerts are sent if data quality thresholds are breached

5. **View CloudWatch Metrics**
   - Navigate to **CloudWatch Console**
   - Click **"Metrics"** → **"All metrics"**
   - Look for custom namespace: `DataLake/SchemaEvolution`
   - View metrics for:
     - Records processed
     - Schema changes detected
     - Data quality scores

---

## Cleanup (Tear Down)

Follow these steps in order to completely remove all resources:

### 1. Delete S3 Event Notification

1. Navigate to **S3 Console**
2. Click on your bucket
3. Click **"Properties"** tab
4. Scroll to **"Event notifications"**
5. Select the event notification `TriggerPipelineOnUpload`
6. Click **"Delete"**

### 2. Delete Lambda Function

1. Navigate to **Lambda Console**
2. Select `trigger-pipeline` function
3. Click **"Actions"** → **"Delete"**
4. Type "delete" to confirm
5. Click **"Delete"**

### 3. Delete Glue Job

1. Navigate to **AWS Glue Console**
2. Click **"Jobs"** in the left sidebar
3. Select `orders-etl-job`
4. Click **"Action"** → **"Delete job"**
5. Confirm deletion

### 4. Delete Glue Database

1. In **AWS Glue Console**
2. Click **"Databases"** under **"Data Catalog"**
3. Select `data_lake_db`
4. Click **"Action"** → **"Delete database"**
5. Confirm deletion

### 5. Delete SNS Topic

1. Navigate to **SNS Console**
2. Click **"Topics"** in the left sidebar
3. Select `data-quality-alerts`
4. Click **"Delete"**
5. Type "delete me" to confirm
6. Click **"Delete"**

### 6. Delete S3 Bucket

1. Navigate to **S3 Console**
2. Select your bucket (checkbox)
3. Click **"Empty"** button
4. Type "permanently delete" to confirm
5. Click **"Empty"**
6. After bucket is empty, select the bucket again
7. Click **"Delete"**
8. Type the bucket name to confirm
9. Click **"Delete bucket"**

### 7. Delete IAM Roles

1. Navigate to **IAM Console**
2. Click **"Roles"** in the left sidebar

**Delete Glue Role:**
3. Search for `DataLakeGlueJobRole`
4. Click on the role name
5. Click **"Delete"** button
6. Type the role name to confirm
7. Click **"Delete"**

**Delete Lambda Role:**
8. Search for `DataLakeLambdaTriggerRole`
9. Click on the role name
10. Click **"Delete"** button
11. Type the role name to confirm
12. Click **"Delete"**

### 8. Delete CloudWatch Log Groups (Optional)

1. Navigate to **CloudWatch Console**
2. Click **"Log groups"** in the left sidebar
3. Search for and delete:
   - `/aws-glue/jobs/orders-etl-job`
   - `/aws/lambda/trigger-pipeline`
4. Select each log group and click **"Actions"** → **"Delete log group(s)"**

---

## Troubleshooting

### Issue: Lambda function not triggered when uploading to S3

**Possible Causes:**
- S3 event notification not configured correctly
- Lambda doesn't have permission to be invoked by S3
- File uploaded to wrong prefix

**Solution:**
1. Go to **S3 Console** → Your bucket → **"Properties"** → **"Event notifications"**
2. Verify the event notification exists and is enabled
3. Check the prefix is set to `raw/orders/`
4. Go to **Lambda Console** → `trigger-pipeline` → **"Configuration"** → **"Permissions"**
5. Verify resource-based policy allows S3 to invoke the function
6. Test by uploading a file directly to `raw/orders/` folder

### Issue: Glue job fails to start

**Possible Causes:**
- IAM role doesn't have necessary permissions
- Script path incorrect in S3
- Dependencies not uploaded

**Solution:**
1. Go to **AWS Glue Console** → **"Jobs"** → `orders-etl-job`
2. Check the **"Script path"** points to correct S3 location
3. Verify the script file exists in S3
4. Go to **IAM Console** → **"Roles"** → `DataLakeGlueJobRole`
5. Review attached policies and ensure S3, Glue, and CloudWatch permissions are present
6. Check **CloudWatch Logs** for error messages

### Issue: Glue job fails during execution

**Possible Causes:**
- Missing dependencies
- Incorrect job parameters
- Data format issues

**Solution:**
1. Go to **AWS Glue Console** → **"Jobs"** → `orders-etl-job` → **"Runs"**
2. Click on the failed run
3. Click **"Logs"** to view error details
4. Common fixes:
   - Verify `dependencies.zip` is uploaded to S3
   - Check job parameters are set correctly
   - Verify input data format matches expected schema

### Issue: No email notifications received

**Possible Causes:**
- Email subscription not confirmed
- SNS topic ARN incorrect in Glue job parameters
- Glue role doesn't have SNS publish permissions

**Solution:**
1. Go to **SNS Console** → **"Subscriptions"**
2. Verify your email subscription status is **"Confirmed"** (not "Pending")
3. If pending, check your email for confirmation link
4. Go to **AWS Glue Console** → **"Jobs"** → `orders-etl-job`
5. Verify job parameter `--SNS_TOPIC_ARN` is set correctly
6. Go to **IAM Console** → **"Roles"** → `DataLakeGlueJobRole`
7. Verify the role has `sns:Publish` permission

### Issue: Access denied errors in Glue job logs

**Possible Causes:**
- IAM role missing required permissions
- S3 bucket policy blocking access

**Solution:**
1. Go to **CloudWatch Logs** and identify which service is being denied
2. Go to **IAM Console** → **"Roles"** → `DataLakeGlueJobRole`
3. Add missing permissions to the inline policy
4. Common missing permissions:
   - `s3:GetObject`, `s3:PutObject` for S3 access
   - `glue:CreateTable`, `glue:UpdateTable` for catalog operations
   - `logs:CreateLogStream`, `logs:PutLogEvents` for CloudWatch

### Issue: Glue catalog table not created

**Possible Causes:**
- Database doesn't exist
- Glue role lacks catalog permissions
- Job failed before table creation

**Solution:**
1. Go to **AWS Glue Console** → **"Databases"**
2. Verify `data_lake_db` exists
3. Check Glue job logs for errors during table creation
4. Manually run the Glue job and monitor execution

---

## Additional Configuration (Optional)

### Configure CloudWatch Alarms

1. **Navigate to CloudWatch Console**
2. Click **"Alarms"** → **"Create alarm"**
3. Click **"Select metric"**
4. Choose **"Glue"** → **"Job Metrics"**
5. Select metrics for `orders-etl-job`:
   - `glue.driver.aggregate.numFailedTasks`
   - `glue.driver.aggregate.elapsedTime`
6. Set threshold conditions (e.g., alert if job fails)
7. Configure SNS notification to `data-quality-alerts` topic
8. Name the alarm and create it

### Set Up VPC for Glue Jobs (Enhanced Security)

1. **Navigate to VPC Console**
2. Create or select an existing VPC
3. Ensure VPC has:
   - Private subnets
   - NAT Gateway or VPC endpoints for AWS services
   - Security group allowing outbound traffic

4. **Update Glue Job with VPC**
   - Go to **AWS Glue Console** → **"Jobs"** → `orders-etl-job`
   - Click **"Edit job"**
   - Expand **"Advanced properties"**
   - Under **"Connections"**, add VPC configuration
   - Select VPC, subnets, and security groups
   - Click **"Save"**

### Enable S3 Cross-Region Replication (Disaster Recovery)

1. **Navigate to S3 Console**
2. Click on your bucket
3. Click **"Management"** tab
4. Scroll to **"Replication rules"**
5. Click **"Create replication rule"**
6. Configure:
   - Rule name: `disaster-recovery-replication`
   - Source bucket: Current bucket
   - Destination: Create or select bucket in different region
   - IAM role: Create new or select existing
7. Click **"Save"**

---

## Production Deployment Considerations

1. **Multi-Environment Setup**: 
   - Create separate buckets and resources for dev, staging, and prod
   - Use naming conventions like `datalake-dev`, `datalake-staging`, `datalake-prod`

2. **Enhanced Security**:
   - Run Glue jobs in VPC with private subnets
   - Use KMS encryption for S3 instead of SSE-S3
   - Enable CloudTrail for audit logging
   - Implement least-privilege IAM policies

3. **Monitoring and Alerting**:
   - Set up CloudWatch dashboards for key metrics
   - Create alarms for job failures, high costs, and performance issues
   - Configure SNS for multiple notification channels (email, Slack, PagerDuty)

4. **Cost Optimization**:
   - Use appropriate Glue worker types (G.1X for standard, G.2X for memory-intensive)
   - Set job timeouts to prevent runaway costs
   - Enable S3 Intelligent-Tiering for cost savings
   - Use Glue job bookmarks to process only new data

5. **Backup and Disaster Recovery**:
   - Enable S3 versioning (already done in Step 1)
   - Set up cross-region replication
   - Document recovery procedures
   - Test restore procedures regularly

6. **Data Governance**:
   - Use AWS Lake Formation for fine-grained access control
   - Tag all resources for cost allocation
   - Implement data retention policies
   - Set up data lineage tracking

---

## Summary

You have successfully deployed the AWS Data Lake Schema Evolution Framework using the AWS Management Console! The system is now ready to:

- Automatically detect and adapt to schema changes
- Process incoming data with quality validation
- Send alerts for data quality issues
- Maintain a versioned data catalog in AWS Glue
- Store processed data in optimized Parquet format

**Key Resources Created:**
- S3 Bucket: Data storage with folder structure
- IAM Roles: `DataLakeGlueJobRole`, `DataLakeLambdaTriggerRole`
- Glue Database: `data_lake_db`
- Glue Job: `orders-etl-job`
- Lambda Function: `trigger-pipeline`
- SNS Topic: `data-quality-alerts`

**Next Steps:**
- Upload your production data to `s3://YOUR-BUCKET/raw/orders/`
- Monitor job executions in AWS Glue Console
- Review data quality reports in S3
- Set up additional CloudWatch alarms as needed
