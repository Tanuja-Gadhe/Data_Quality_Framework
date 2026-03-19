# Sample Datasets

This directory contains sample datasets for testing the AWS Data Lake Schema Evolution Framework.

## Files

### 1. sample_orders.csv
**Purpose**: Clean baseline dataset with standard schema

**Schema**:
- `order_id` (string): Unique order identifier
- `customer_id` (string): Customer identifier
- `product_name` (string): Name of the product
- `quantity` (integer): Quantity ordered
- `price` (decimal): Price per unit
- `order_date` (date): Date of order
- `status` (string): Order status (completed, pending, shipped)
- `region` (string): Geographic region

**Records**: 30 valid orders

**Use Case**: Initial data load to establish baseline schema

---

### 2. sample_orders_with_new_column.csv
**Purpose**: Demonstrates schema evolution with a new column

**Schema**: Same as sample_orders.csv PLUS:
- `shipping_method` (string): Shipping method (standard, express)

**Records**: 10 orders with new column

**Use Case**: Test schema evolution handling when new columns are added

**Expected Behavior**:
- Schema detector identifies new column `shipping_method`
- Schema evolution handler adds the column to the DataFrame
- Glue catalog is updated with the new column
- Previous records will have NULL for `shipping_method`

---

### 3. sample_orders_with_invalid_data.csv
**Purpose**: Contains data quality issues for validation testing

**Data Quality Issues**:
1. **Row 2**: Missing `order_id` (NULL violation)
2. **Row 3**: Missing `customer_id` (NULL violation)
3. **Row 4**: Negative `price` value (range violation)
4. **Row 5**: `quantity` = 0 (range violation, must be >= 1)
5. **Row 6**: Invalid `order_date` format
6. **Row 8**: Duplicate `order_id` (ORD041 appears twice)

**Records**: 10 orders (4 valid, 6 invalid)

**Use Case**: Test data quality validation rules

**Expected Behavior**:
- 6 records should be quarantined
- 4 valid records should reach clean layer
- Data quality metrics should show 60% invalid records
- Alert should be triggered (exceeds 5% threshold)

---

## Testing Scenarios

### Scenario 1: Initial Load

**Steps:**
1. Navigate to **S3 Console**
2. Open your data lake bucket
3. Navigate to `raw/orders/2024/01/` folder
4. Click **"Upload"**
5. Upload `sample_orders.csv`
6. Click **"Upload"**

**Expected Results:**
- 30 records in `clean/orders/` layer
- 0 records in quarantine
- Data quality score: 100%

**Verification:**
- Check `clean/orders/` folder for Parquet files
- Monitor Glue job in AWS Glue Console
- Review CloudWatch logs

### Scenario 2: Schema Evolution

**Steps:**
1. Navigate to **S3 Console**
2. Go to `raw/orders/2024/01/` folder
3. Upload `sample_orders_with_new_column.csv`

**Expected Results:**
- New column `shipping_method` detected and added
- 10 records in clean layer with new column
- Glue catalog updated with new schema

**Verification:**
- Navigate to **AWS Glue Console** → **"Tables"**
- Click on `orders_clean` table
- Verify `shipping_method` column appears in schema
- Check CloudWatch logs for "Schema changes detected" message

### Scenario 3: Data Quality Validation

**Steps:**
1. Navigate to **S3 Console**
2. Go to `raw/orders/2024/02/` folder
3. Upload `sample_orders_with_invalid_data.csv`

**Expected Results:**
- 4 valid records in clean layer
- 6 invalid records in quarantine
- Data quality alert triggered (60% invalid exceeds 5% threshold)
- Metrics report saved to S3

**Verification:**
- Check `quarantine/orders_invalid/` folder for invalid records
- Download and review quality metrics from `logs/data_quality_reports/orders/`
- Check email for data quality alert
- Review CloudWatch logs for validation details

### Scenario 4: Combined Test

**Steps:**
1. Navigate to **S3 Console**
2. Upload all three files sequentially to `raw/orders/`:
   - `sample_orders.csv` → `batch1/` subfolder
   - `sample_orders_with_new_column.csv` → `batch2/` subfolder
   - `sample_orders_with_invalid_data.csv` → `batch3/` subfolder

**Expected Results:**
- Total: 44 valid records, 6 invalid
- Schema evolved to include `shipping_method`
- Complete audit trail in CloudWatch logs

**Verification:**
- Check total record count in `clean/orders/`
- Verify schema includes all columns
- Review complete execution history in AWS Glue Console

---

## Data Generation

To generate additional test data, you can use the following Python script:

```python
import csv
from datetime import datetime, timedelta
import random

def generate_orders(count, start_id=1000):
    products = ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones']
    regions = ['US-EAST', 'US-WEST', 'EU-WEST', 'ASIA-PACIFIC']
    statuses = ['completed', 'pending', 'shipped']
    
    orders = []
    for i in range(count):
        order = {
            'order_id': f'ORD{start_id + i:04d}',
            'customer_id': f'CUST{start_id + i:04d}',
            'product_name': random.choice(products),
            'quantity': random.randint(1, 5),
            'price': round(random.uniform(10, 500), 2),
            'order_date': (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d'),
            'status': random.choice(statuses),
            'region': random.choice(regions)
        }
        orders.append(order)
    
    return orders

# Generate and save
orders = generate_orders(100)
with open('generated_orders.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=orders[0].keys())
    writer.writeheader()
    writer.writerows(orders)
```

---

## Validation Rules Applied

The following validation rules are applied to orders data:

1. **order_id_not_null**: Order ID must not be null (ERROR)
2. **order_id_unique**: Order ID must be unique (ERROR)
3. **price_positive**: Price must be >= 0 (ERROR)
4. **quantity_positive**: Quantity must be >= 1 (ERROR)
5. **order_date_not_null**: Order date must not be null (ERROR)
6. **customer_id_not_null**: Customer ID must not be null (ERROR)

---

## Notes

- All dates are in ISO format (YYYY-MM-DD)
- Prices are in USD
- Files use UTF-8 encoding
- CSV files have headers
- NULL values are represented as empty strings in CSV
