# IAM Roles and Policies

This document describes the IAM roles and policies required for the AWS Data Lake Schema Evolution Framework.

## Table of Contents

1. [Glue Job IAM Role](#glue-job-iam-role)
2. [Lambda Function IAM Role](#lambda-function-iam-role)
3. [S3 Bucket Policies](#s3-bucket-policies)
4. [Glue Data Catalog Permissions](#glue-data-catalog-permissions)

---

## Glue Job IAM Role

### Role Name
`DataLakeGlueJobRole`

### Trust Policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

### Permissions Policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3DataLakeAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::data-lake-bucket/*",
        "arn:aws:s3:::data-lake-bucket"
      ]
    },
    {
      "Sid": "GlueCatalogAccess",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:GetTables",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:CreatePartition",
        "glue:UpdatePartition",
        "glue:DeletePartition",
        "glue:BatchCreatePartition",
        "glue:BatchDeletePartition"
      ],
      "Resource": [
        "arn:aws:glue:*:*:catalog",
        "arn:aws:glue:*:*:database/data_lake_db",
        "arn:aws:glue:*:*:table/data_lake_db/*"
      ]
    },
    {
      "Sid": "CloudWatchLogsAccess",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": [
        "arn:aws:logs:*:*:/aws-glue/*"
      ]
    },
    {
      "Sid": "CloudWatchMetricsAccess",
      "Effect": "Allow",
      "Action": [
        "cloudwatch:PutMetricData"
      ],
      "Resource": "*",
      "Condition": {
        "StringEquals": {
          "cloudwatch:namespace": "AWS/Glue"
        }
      }
    },
    {
      "Sid": "SNSPublishAccess",
      "Effect": "Allow",
      "Action": [
        "sns:Publish"
      ],
      "Resource": [
        "arn:aws:sns:*:*:data-quality-alerts"
      ]
    }
  ]
}
```

### Managed Policies to Attach

- `AWSGlueServiceRole` (AWS Managed)

---

## Lambda Function IAM Role

### Role Name
`DataLakeLambdaTriggerRole`

### Trust Policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

### Permissions Policy

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
        "arn:aws:s3:::data-lake-bucket/raw/*",
        "arn:aws:s3:::data-lake-bucket"
      ]
    },
    {
      "Sid": "GlueJobTrigger",
      "Effect": "Allow",
      "Action": [
        "glue:StartJobRun",
        "glue:GetJobRun",
        "glue:GetJobRuns",
        "glue:BatchStopJobRun"
      ],
      "Resource": [
        "arn:aws:glue:*:*:job/orders-etl-job"
      ]
    },
    {
      "Sid": "CloudWatchLogsAccess",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": [
        "arn:aws:logs:*:*:log-group:/aws/lambda/*"
      ]
    },
    {
      "Sid": "CloudWatchMetricsAccess",
      "Effect": "Allow",
      "Action": [
        "cloudwatch:PutMetricData"
      ],
      "Resource": "*"
    }
  ]
}
```

### Managed Policies to Attach

- `AWSLambdaBasicExecutionRole` (AWS Managed)

---

## S3 Bucket Policies

### Bucket Name
`data-lake-bucket`

### Bucket Policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowGlueJobAccess",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::ACCOUNT_ID:role/DataLakeGlueJobRole"
      },
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::data-lake-bucket/*"
    },
    {
      "Sid": "AllowLambdaReadAccess",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::ACCOUNT_ID:role/DataLakeLambdaTriggerRole"
      },
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::data-lake-bucket/raw/*",
        "arn:aws:s3:::data-lake-bucket"
      ]
    },
    {
      "Sid": "DenyUnencryptedObjectUploads",
      "Effect": "Deny",
      "Principal": "*",
      "Action": "s3:PutObject",
      "Resource": "arn:aws:s3:::data-lake-bucket/*",
      "Condition": {
        "StringNotEquals": {
          "s3:x-amz-server-side-encryption": "AES256"
        }
      }
    }
  ]
}
```

### S3 Event Notification Configuration

Configure S3 to send event notifications to Lambda:

```json
{
  "LambdaFunctionConfigurations": [
    {
      "Id": "TriggerPipelineOnNewFile",
      "LambdaFunctionArn": "arn:aws:lambda:REGION:ACCOUNT_ID:function:trigger-pipeline",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {
        "Key": {
          "FilterRules": [
            {
              "Name": "prefix",
              "Value": "raw/orders/"
            },
            {
              "Name": "suffix",
              "Value": ".csv"
            }
          ]
        }
      }
    }
  ]
}
```

---

## Glue Data Catalog Permissions

### Database Name
`data_lake_db`

### Required Permissions

All roles that need to access the Glue Data Catalog require:

- `glue:GetDatabase`
- `glue:GetTable`
- `glue:GetTables`
- `glue:GetPartition`
- `glue:GetPartitions`

For roles that need to modify the catalog (like the Glue job):

- `glue:CreateTable`
- `glue:UpdateTable`
- `glue:DeleteTable`
- `glue:CreatePartition`
- `glue:UpdatePartition`
- `glue:DeletePartition`

---

## SNS Topic Configuration

### Topic Name
`data-quality-alerts`

### Topic Policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowGlueJobPublish",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::ACCOUNT_ID:role/DataLakeGlueJobRole"
      },
      "Action": "SNS:Publish",
      "Resource": "arn:aws:sns:REGION:ACCOUNT_ID:data-quality-alerts"
    }
  ]
}
```

### Subscription Configuration

Subscribe email addresses to receive alerts using AWS Console:

1. Navigate to **SNS Console** → **"Topics"**
2. Click on `data-quality-alerts` topic
3. Click **"Create subscription"**
4. **Protocol**: Email
5. **Endpoint**: your-email@example.com
6. Click **"Create subscription"**
7. Confirm subscription via email

---

## CloudWatch Log Groups

### Log Group Configuration

Create log groups for monitoring:

1. **Glue Job Logs**
   - Log Group: `/aws-glue/jobs/orders-etl-job`
   - Retention: 30 days

2. **Lambda Function Logs**
   - Log Group: `/aws/lambda/trigger-pipeline`
   - Retention: 14 days

3. **Data Quality Logs**
   - Log Group: `/aws/glue/data-lake-pipeline`
   - Retention: 90 days

---

## Security Best Practices

1. **Least Privilege**: Grant only the minimum permissions required for each role
2. **Encryption**: Enable S3 bucket encryption and enforce encrypted uploads
3. **VPC Configuration**: Run Glue jobs in a VPC for enhanced security
4. **Secrets Management**: Use AWS Secrets Manager for sensitive configuration
5. **Audit Logging**: Enable CloudTrail for API call auditing
6. **Resource Tagging**: Tag all resources for cost allocation and governance
7. **Regular Reviews**: Periodically review and update IAM policies

---

## Deployment Steps (AWS Console)

### Create Glue Job Role

1. **Navigate to IAM Console**
   - Open AWS Management Console
   - Search for "IAM" and click on **"IAM"**

2. **Create Role**
   - Click **"Roles"** → **"Create role"**
   - **Trusted entity type**: AWS service
   - **Use case**: Glue
   - Click **"Next"**

3. **Attach Managed Policy**
   - Search and select: **"AWSGlueServiceRole"**
   - Click **"Next"**

4. **Name Role**
   - **Role name**: `DataLakeGlueJobRole`
   - **Description**: IAM role for AWS Glue ETL jobs in data lake
   - Click **"Create role"**

5. **Add Inline Policy**
   - Click on the newly created role
   - **"Add permissions"** → **"Create inline policy"**
   - Click **"JSON"** tab
   - Paste the Permissions Policy JSON from above (replace bucket name)
   - **Policy name**: `GlueJobPermissions`
   - Click **"Create policy"**

### Create Lambda Role

1. **Create Role**
   - In IAM Console, click **"Roles"** → **"Create role"**
   - **Trusted entity type**: AWS service
   - **Use case**: Lambda
   - Click **"Next"**

2. **Attach Managed Policy**
   - Search and select: **"AWSLambdaBasicExecutionRole"**
   - Click **"Next"**

3. **Name Role**
   - **Role name**: `DataLakeLambdaTriggerRole`
   - **Description**: IAM role for Lambda function that triggers Glue jobs
   - Click **"Create role"**

4. **Add Inline Policy**
   - Click on the newly created role
   - **"Add permissions"** → **"Create inline policy"**
   - Click **"JSON"** tab
   - Paste the Permissions Policy JSON from above (replace bucket name)
   - **Policy name**: `LambdaTriggerPermissions`
   - Click **"Create policy"**

---

## Notes

- Replace `ACCOUNT_ID`, `REGION`, and bucket names with your actual values in all JSON policies
- Adjust resource ARNs based on your AWS environment
- Use the AWS Management Console for all resource creation
- Review and test all permissions in a non-production environment first
- For detailed deployment steps, see [DEPLOYMENT_GUIDE.md](../DEPLOYMENT_GUIDE.md)
