# FinOps Cost Analytics API

## 🚀 Overview

The FinOps Cost Analytics API provides comprehensive REST endpoints for AWS cost data analysis, optimization, and reporting. Built with FastAPI and powered by DuckDB, it offers high-performance SQL queries on your AWS Cost and Usage Reports (CUR) and FOCUS data.

## 📡 Base URL

```
http://localhost:8000/api/v1/finops
```

## 🔍 Interactive Documentation

- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`

## 🛡️ Authentication

Currently supports:
- No authentication (development)
- Future: JWT/API key authentication with role-based access control

## 📊 Available Endpoints

### 🔍 SQL Query Endpoints

Execute custom SQL queries for flexible data analysis.

#### 1. Execute SQL Query

**Endpoint**: `POST /sql/query`

Execute custom SELECT queries on AWS cost data with powerful analytics capabilities.

**Request Body**:
```json
{
  "sql": "SELECT product_servicecode, SUM(line_item_unblended_cost) as total_cost FROM CUR WHERE line_item_unblended_cost > 0 GROUP BY product_servicecode ORDER BY total_cost DESC LIMIT 5",
  "limit": 1000,
  "force_s3": false,
  "format": "json"
}
```

**Parameters**:
- `sql` (string, required): SQL query to execute (SELECT only, max 10,000 chars)
- `limit` (integer, optional): Maximum rows to return (default: 1000, max: 10000)
- `force_s3` (boolean, optional): Force S3 querying even if local data available (default: false)
- `format` (string, optional): Output format - "json" or "csv" (default: "json")

**Response**:
```json
{
  "success": true,
  "query_metadata": {
    "query_timestamp": "2025-07-31 17:30:00 UTC",
    "data_source": "local_parquet",
    "data_export_type": "CUR2.0",
    "table_name": "CUR",
    "available_tables": ["CUR", "summary_view", "kpi_instance_all"],
    "query_length": 145,
    "format": "json",
    "limit_applied": 1000,
    "execution_time_ms": 45.2
  },
  "data": [
    {"product_servicecode": "AmazonVPC", "total_cost": 12.50},
    {"product_servicecode": "AmazonEC2", "total_cost": 8.68},
    {"product_servicecode": null, "total_cost": 1.70}
  ],
  "schema": {
    "product_servicecode": "object",
    "total_cost": "float64"
  },
  "row_count": 3,
  "execution_time_ms": 45.2
}
```

**Example cURL**:
```bash
curl -X POST "http://localhost:8000/api/v1/finops/sql/query" \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT product_servicecode, SUM(line_item_unblended_cost) as total_cost FROM CUR WHERE line_item_unblended_cost > 0 GROUP BY product_servicecode ORDER BY total_cost DESC LIMIT 5",
    "limit": 1000,
    "format": "json"
  }'
```

#### 2. Get Data Schema

**Endpoint**: `GET /sql/schema`

Get schema information, sample data, and query examples for available tables.

**Response**:
```json
{
  "main_table": {
    "name": "CUR",
    "schema": {
      "bill_bill_type": "object",
      "bill_billing_entity": "object",
      "line_item_unblended_cost": "float64",
      "product_servicecode": "object"
    },
    "sample_data": [
      {"bill_bill_type": "Anniversary", "product_servicecode": "AmazonVPC", "line_item_unblended_cost": 0.12}
    ],
    "row_count_estimate": "Run: SELECT COUNT(*) FROM CUR"
  },
  "available_tables": ["CUR", "summary_view", "kpi_instance_all"],
  "data_export_type": "CUR2.0",
  "query_examples": [
    {
      "name": "Top Services by Cost",
      "sql": "SELECT product_servicecode, SUM(line_item_unblended_cost) as total_cost FROM CUR WHERE line_item_unblended_cost > 0 GROUP BY product_servicecode ORDER BY total_cost DESC LIMIT 10"
    },
    {
      "name": "Monthly Cost Trends",
      "sql": "SELECT bill_billing_period_start_date, SUM(line_item_unblended_cost) as monthly_cost FROM CUR GROUP BY bill_billing_period_start_date ORDER BY bill_billing_period_start_date"
    }
  ]
}
```

#### 3. List Available Tables

**Endpoint**: `GET /sql/tables`

List all tables and views available for SQL queries.

**Response**:
```json
{
  "available_tables": [
    {
      "name": "CUR",
      "type": "main_table",
      "description": "Main CUR2.0 cost data export",
      "suggested_columns": ["product_servicecode", "line_item_unblended_cost", "bill_billing_period_start_date", "line_item_usage_account_id"]
    },
    {
      "name": "summary_view",
      "type": "view",
      "description": "Pre-aggregated cost summary data",
      "suggested_columns": ["billing_period", "unblended_cost", "amortized_cost"]
    },
    {
      "name": "kpi_instance_all",
      "type": "view",
      "description": "Instance-level cost metrics and optimization opportunities",
      "suggested_columns": ["ec2_all_cost", "rds_all_cost", "compute_all_cost"]
    }
  ],
  "data_export_type": "CUR2.0",
  "main_table": "CUR"
}
```

### 📊 KPI Summary Endpoints

#### Get Comprehensive KPI Summary

**Endpoint**: `GET /kpi/summary`

**Query Parameters**:
- `billing_period` (optional): Filter by billing period (YYYY-MM format)
- `payer_account_id` (optional): Filter by payer account
- `linked_account_id` (optional): Filter by linked account
- `tags_filter` (optional): Filter by tags (JSON format)

**Response**: Comprehensive KPI metrics including spend summary, EC2 metrics, RDS metrics, storage metrics, compute services, and savings opportunities.

## 🛡️ Security Features

### SQL Query Security

- **Query Validation**: Only SELECT statements allowed
- **Dangerous Operations Blocked**: DROP, DELETE, INSERT, UPDATE, CREATE, ALTER, TRUNCATE, GRANT, REVOKE, EXEC, EXECUTE
- **Row Limits**: Maximum 10,000 rows per query
- **Query Length Limits**: Maximum 10,000 characters
- **Input Sanitization**: SQL injection protection
- **Schema Access Control**: Only authorized tables accessible

### Blocked SQL Examples

```sql
-- ❌ These queries will be rejected:
DROP TABLE CUR;
DELETE FROM CUR WHERE line_item_unblended_cost > 0;
INSERT INTO CUR VALUES (1, 2, 3);
CREATE TABLE malicious AS SELECT * FROM CUR;
UPDATE CUR SET line_item_unblended_cost = 0;
```

### Allowed SQL Examples

```sql
-- ✅ These queries are allowed:
SELECT * FROM CUR LIMIT 10;
SELECT product_servicecode, SUM(line_item_unblended_cost) FROM CUR GROUP BY product_servicecode;
SELECT COUNT(*) FROM CUR WHERE bill_billing_period_start_date = '2025-07-01';
```

## 📚 Available Data Tables

### Main Data Table: `CUR` or `FOCUS`

The primary table containing your AWS cost data export. Table name depends on your data export type configuration.

**Key Columns for CUR 2.0**:
- `bill_billing_period_start_date`: Billing period
- `line_item_unblended_cost`: Actual cost charged
- `line_item_blended_cost`: Blended cost for consolidated billing
- `product_servicecode`: AWS service (e.g., 'AmazonEC2', 'AmazonS3')
- `line_item_usage_account_id`: AWS account ID
- `product_region`: AWS region
- `product_instance_type`: Instance type (for applicable services)
- `line_item_usage_type`: Usage type details
- `line_item_operation`: Operation performed

### Pre-built Views

- **`summary_view`**: Pre-aggregated cost summary data
- **`kpi_instance_all`**: Instance-level cost metrics with optimization opportunities
- **`kpi_ebs_storage_all`**: EBS storage cost analysis
- **`kpi_ebs_snap`**: EBS snapshot cost tracking
- **`kpi_s3_storage_all`**: S3 storage cost metrics
- **`kpi_instance_mapping`**: Instance type mapping and recommendations

## 🎯 Query Examples

### Basic Cost Analysis

```sql
-- Total costs by service
SELECT 
    product_servicecode,
    SUM(line_item_unblended_cost) as total_cost,
    COUNT(*) as line_items
FROM CUR 
WHERE line_item_unblended_cost > 0
GROUP BY product_servicecode 
ORDER BY total_cost DESC;
```

### Monthly Cost Trends

```sql
-- Monthly spend trends
SELECT 
    bill_billing_period_start_date as billing_period,
    SUM(line_item_unblended_cost) as monthly_cost,
    COUNT(DISTINCT line_item_usage_account_id) as unique_accounts
FROM CUR
GROUP BY bill_billing_period_start_date
ORDER BY billing_period;
```

### Regional Cost Analysis

```sql
-- Cost by AWS region
SELECT 
    product_region as region,
    product_servicecode,
    SUM(line_item_unblended_cost) as cost,
    COUNT(*) as usage_records
FROM CUR 
WHERE line_item_unblended_cost > 0 
  AND product_region IS NOT NULL 
GROUP BY product_region, product_servicecode
ORDER BY cost DESC
LIMIT 20;
```

### Advanced Analytics with Window Functions

```sql
-- Month-over-month cost growth
SELECT 
    product_servicecode,
    bill_billing_period_start_date as billing_period,
    SUM(line_item_unblended_cost) as current_cost,
    LAG(SUM(line_item_unblended_cost), 1) OVER (
        PARTITION BY product_servicecode 
        ORDER BY bill_billing_period_start_date
    ) as previous_cost,
    ROUND(
        ((SUM(line_item_unblended_cost) - LAG(SUM(line_item_unblended_cost), 1) OVER (
            PARTITION BY product_servicecode 
            ORDER BY bill_billing_period_start_date
        )) / NULLIF(LAG(SUM(line_item_unblended_cost), 1) OVER (
            PARTITION BY product_servicecode 
            ORDER BY bill_billing_period_start_date
        ), 0)) * 100, 2
    ) as cost_change_percent
FROM CUR 
WHERE line_item_unblended_cost > 0
GROUP BY bill_billing_period_start_date, product_servicecode
ORDER BY billing_period, current_cost DESC;
```

### Account Cost Distribution

```sql
-- Account cost distribution with percentages
WITH account_costs AS (
    SELECT 
        line_item_usage_account_id,
        SUM(line_item_unblended_cost) as total_cost
    FROM CUR 
    WHERE line_item_unblended_cost > 0
    GROUP BY line_item_usage_account_id
),
total_cost AS (
    SELECT SUM(total_cost) as grand_total FROM account_costs
)
SELECT 
    ac.line_item_usage_account_id,
    ac.total_cost,
    ROUND((ac.total_cost / tc.grand_total) * 100, 2) as cost_percentage,
    RANK() OVER (ORDER BY ac.total_cost DESC) as cost_rank
FROM account_costs ac
CROSS JOIN total_cost tc
ORDER BY ac.total_cost DESC;
```

## 🔧 Error Handling

### Common Error Types

1. **SQL_SYNTAX_ERROR**: Invalid SQL syntax
2. **TABLE_NOT_FOUND**: Referenced table doesn't exist
3. **COLUMN_NOT_FOUND**: Referenced column doesn't exist
4. **EXECUTION_ERROR**: General query execution error

### Error Response Format

```json
{
  "success": false,
  "error": "Binder Error: Referenced column 'invalid_column' not found",
  "error_type": "COLUMN_NOT_FOUND",
  "query_metadata": {
    "query_timestamp": "2025-07-31 17:30:00 UTC",
    "table_name": "CUR",
    "available_tables": ["CUR", "summary_view"],
    "query_length": 45,
    "execution_time_ms": 12.3
  }
}
```

## 🚀 Performance Tips

1. **Use Local Data**: Set `prefer_local_data=True` in configuration for 90%+ faster queries
2. **Limit Results**: Always use LIMIT clause for large datasets
3. **Filter Early**: Add WHERE clauses to reduce data processed
4. **Index Columns**: Filter on indexed columns like `bill_billing_period_start_date`
5. **Avoid SELECT \***: Select only needed columns for better performance

## 📈 Output Formats

### JSON Format (Default)

Returns data as array of objects, perfect for frontend consumption:

```json
{
  "data": [
    {"product_servicecode": "AmazonEC2", "total_cost": 123.45},
    {"product_servicecode": "AmazonS3", "total_cost": 67.89}
  ]
}
```

### CSV Format

Returns data as CSV string, ideal for exports:

```json
{
  "data": "product_servicecode,total_cost\nAmazonEC2,123.45\nAmazonS3,67.89\n"
}
```

## 🔌 Integration Examples

### Python Integration

```python
import requests

# Execute SQL query
response = requests.post("http://localhost:8000/api/v1/finops/sql/query", json={
    "sql": "SELECT product_servicecode, SUM(line_item_unblended_cost) as cost FROM CUR GROUP BY product_servicecode ORDER BY cost DESC LIMIT 5",
    "format": "json"
})

data = response.json()
if data["success"]:
    print(f"Query returned {data['row_count']} rows in {data['execution_time_ms']}ms")
    for row in data["data"]:
        print(f"{row['product_servicecode']}: ${row['cost']:.2f}")
```

### JavaScript Integration

```javascript
// Execute SQL query
const response = await fetch('http://localhost:8000/api/v1/finops/sql/query', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    sql: "SELECT product_servicecode, SUM(line_item_unblended_cost) as cost FROM CUR GROUP BY product_servicecode ORDER BY cost DESC LIMIT 5",
    format: "json"
  })
});

const data = await response.json();
if (data.success) {
  console.log(`Query returned ${data.row_count} rows in ${data.execution_time_ms}ms`);
  data.data.forEach(row => {
    console.log(`${row.product_servicecode}: $${row.cost.toFixed(2)}`);
  });
}
```

## 🛠️ Development

### Adding New Endpoints

1. Create endpoint in `de_polars/api/endpoints/`
2. Add router to `de_polars/api/endpoints/__init__.py`
3. Include router in `de_polars/api/fastapi_app.py`
4. Update this documentation

### Testing Endpoints

Use the comprehensive test suite:

```bash
# Test SQL endpoints specifically
python tests/test_14_sql_query_endpoint.py

# Test all API functionality
python tests/run_all_tests.py
```

## 📝 License

This API is part of the DE-Polars FinOps Cost Analytics Platform.