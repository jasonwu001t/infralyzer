# Infralyzer Usage Examples

This document provides comprehensive examples of how to use Infralyzer for various FinOps use cases.

## 🚀 Getting Started

### Basic Setup

```python
from infralyzer import FinOpsEngine, DataConfig, DataExportType

# Configure your data source
config = DataConfig(
    s3_bucket='your-cost-data-bucket',
    s3_data_prefix='cur2/data',
    data_export_type=DataExportType.CUR_2_0,
    local_data_path='./local_data',    # Enable caching
    table_name='CUR'
)

# Initialize engine
engine = FinOpsEngine(config)

# Optional: Download data locally for faster queries and cost savings
engine.download_data_locally()
```

## 📊 Query Examples

### 1. Basic Cost Analysis

```python
# Total cost overview
total_cost = engine.query("""
    SELECT
        COUNT(*) as line_items,
        SUM(line_item_unblended_cost) as total_cost,
        COUNT(DISTINCT product_servicecode) as unique_services,
        COUNT(DISTINCT line_item_usage_account_id) as accounts
    FROM CUR
    WHERE line_item_usage_start_date >= '2024-01-01'
""")

print(f"Total Cost: ${total_cost['total_cost'].iloc[0]:,.2f}")
```

### 2. Monthly Cost Trends

```python
# Monthly spend analysis
monthly_trends = engine.query("""
    SELECT
        DATE_TRUNC('month', line_item_usage_start_date) as month,
        SUM(line_item_unblended_cost) as monthly_cost,
        SUM(line_item_net_unblended_cost) as net_cost,
        COUNT(DISTINCT product_servicecode) as services_used
    FROM CUR
    WHERE line_item_usage_start_date >= '2024-01-01'
    GROUP BY 1
    ORDER BY 1
""")

# Convert to convenient JSON for visualization
monthly_json = engine.query_json("""
    SELECT
        TO_CHAR(DATE_TRUNC('month', line_item_usage_start_date), 'YYYY-MM') as month,
        ROUND(SUM(line_item_unblended_cost), 2) as cost
    FROM CUR
    WHERE line_item_usage_start_date >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY 1
    ORDER BY 1
""")
```

### 3. Service-Level Analysis

```python
# Top services by cost
top_services = engine.query("""
    SELECT
        product_servicecode,
        SUM(line_item_unblended_cost) as total_cost,
        AVG(line_item_unblended_cost) as avg_cost_per_line,
        COUNT(*) as line_item_count,
        COUNT(DISTINCT line_item_resource_id) as unique_resources
    FROM CUR
    WHERE line_item_usage_start_date >= CURRENT_DATE - INTERVAL '30 days'
    AND line_item_unblended_cost > 0
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 20
""")

# Service growth analysis
service_growth = engine.query("""
    WITH monthly_service_costs AS (
        SELECT
            product_servicecode,
            DATE_TRUNC('month', line_item_usage_start_date) as month,
            SUM(line_item_unblended_cost) as monthly_cost
        FROM CUR
        WHERE line_item_usage_start_date >= CURRENT_DATE - INTERVAL '6 months'
        GROUP BY 1, 2
    ),
    service_trends AS (
        SELECT
            product_servicecode,
            monthly_cost,
            LAG(monthly_cost) OVER (PARTITION BY product_servicecode ORDER BY month) as prev_month_cost,
            month
        FROM monthly_service_costs
    )
    SELECT
        product_servicecode,
        monthly_cost as current_cost,
        prev_month_cost,
        CASE
            WHEN prev_month_cost > 0 THEN
                ROUND(((monthly_cost - prev_month_cost) / prev_month_cost) * 100, 2)
            ELSE NULL
        END as growth_percentage
    FROM service_trends
    WHERE month = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
    AND monthly_cost > 100  -- Focus on significant costs
    ORDER BY growth_percentage DESC NULLS LAST
""")
```

### 4. Account and Resource Analysis

```python
# Account-level cost breakdown
account_analysis = engine.query("""
    SELECT
        line_item_usage_account_id as account_id,
        SUM(line_item_unblended_cost) as total_cost,
        COUNT(DISTINCT product_servicecode) as services_count,
        COUNT(DISTINCT line_item_resource_id) as resources_count,
        AVG(line_item_unblended_cost) as avg_line_cost
    FROM CUR
    WHERE line_item_usage_start_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY 1
    ORDER BY 2 DESC
""")

# Resource utilization analysis
resource_utilization = engine.query("""
    SELECT
        line_item_resource_id,
        product_servicecode,
        SUM(line_item_unblended_cost) as total_cost,
        SUM(line_item_usage_amount) as total_usage,
        COUNT(*) as billing_records,
        MIN(line_item_usage_start_date) as first_usage,
        MAX(line_item_usage_start_date) as last_usage
    FROM CUR
    WHERE line_item_resource_id IS NOT NULL
    AND line_item_usage_start_date >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY 1, 2
    HAVING SUM(line_item_unblended_cost) > 10  -- Focus on resources with meaningful cost
    ORDER BY total_cost DESC
    LIMIT 100
""")
```

## 🎯 Cost Optimization Examples

### 1. Idle Resource Detection

```python
# Detect idle resources (cost but no usage)
idle_resources = engine.query("""
    SELECT
        line_item_resource_id,
        product_servicecode,
        line_item_operation,
        SUM(line_item_unblended_cost) as wasted_cost,
        COUNT(*) as idle_periods,
        AVG(line_item_unblended_cost) as avg_idle_cost
    FROM CUR
    WHERE line_item_usage_start_date >= CURRENT_DATE - INTERVAL '7 days'
    AND line_item_unblended_cost > 0
    AND (line_item_usage_amount = 0 OR line_item_usage_amount IS NULL)
    GROUP BY 1, 2, 3
    HAVING SUM(line_item_unblended_cost) > 5  -- Minimum threshold
    ORDER BY wasted_cost DESC
""")

print(f"Potential Monthly Savings: ${idle_resources['wasted_cost'].sum() * 4.33:.2f}")
```

### 2. Rightsizing Opportunities

```python
# Underutilized instances (low usage but high cost)
underutilized = engine.query("""
    WITH resource_metrics AS (
        SELECT
            line_item_resource_id,
            product_servicecode,
            product_instance_type,
            SUM(line_item_unblended_cost) as total_cost,
            SUM(line_item_usage_amount) as total_usage_hours,
            COUNT(*) as billing_records
        FROM CUR
        WHERE line_item_usage_start_date >= CURRENT_DATE - INTERVAL '30 days'
        AND product_servicecode = 'AmazonEC2'
        AND line_item_operation LIKE '%RunInstances%'
        GROUP BY 1, 2, 3
    )
    SELECT
        line_item_resource_id,
        product_instance_type,
        total_cost,
        total_usage_hours,
        ROUND(total_usage_hours / 720.0, 2) as utilization_ratio,  -- 720 hours in 30 days
        CASE
            WHEN total_usage_hours / 720.0 < 0.20 THEN 'Terminate'
            WHEN total_usage_hours / 720.0 < 0.50 THEN 'Downsize'
            ELSE 'Monitor'
        END as recommendation
    FROM resource_metrics
    WHERE total_cost > 50  -- Focus on meaningful costs
    ORDER BY total_cost DESC
""")
```

### 3. Reserved Instance Analysis

```python
# RI utilization and coverage
ri_analysis = engine.query("""
    WITH ri_usage AS (
        SELECT
            product_servicecode,
            product_instance_type,
            product_region,
            SUM(CASE WHEN line_item_line_item_type = 'RIFee' THEN line_item_unblended_cost ELSE 0 END) as ri_fees,
            SUM(CASE WHEN line_item_line_item_type = 'Usage' AND reservation_reservation_a_r_n IS NOT NULL
                THEN line_item_usage_amount ELSE 0 END) as ri_usage_hours,
            SUM(CASE WHEN line_item_line_item_type = 'Usage' AND reservation_reservation_a_r_n IS NULL
                THEN line_item_usage_amount ELSE 0 END) as on_demand_hours,
            SUM(CASE WHEN line_item_line_item_type = 'Usage' THEN line_item_usage_amount ELSE 0 END) as total_hours
        FROM CUR
        WHERE line_item_usage_start_date >= CURRENT_DATE - INTERVAL '30 days'
        AND product_servicecode = 'AmazonEC2'
        GROUP BY 1, 2, 3
    )
    SELECT
        product_servicecode,
        product_instance_type,
        product_region,
        ri_fees,
        ri_usage_hours,
        on_demand_hours,
        total_hours,
        CASE WHEN total_hours > 0 THEN
            ROUND((ri_usage_hours / total_hours) * 100, 2)
        ELSE 0 END as ri_coverage_percent
    FROM ri_usage
    WHERE ri_fees > 0 OR ri_usage_hours > 0
    ORDER BY ri_fees DESC
""")
```

## 📊 Advanced Analytics Examples

### 1. Using Specialized Analytics Modules

```python
# KPI Dashboard Data
kpi_data = engine.kpi.get_summary()
print("=== KPI Summary ===")
print(f"Total Monthly Cost: ${kpi_data.get('total_cost', 0):,.2f}")
print(f"Top Service: {kpi_data.get('top_service', 'N/A')}")
print(f"Cost Trend: {kpi_data.get('trend', 'N/A')}")

# Spend Analytics
spend_data = engine.spend.get_invoice_summary()
print("\n=== Spend Analysis ===")
for month_data in spend_data.get('monthly_breakdown', []):
    print(f"{month_data['month']}: ${month_data['cost']:,.2f}")

# Optimization Insights
opt_data = engine.optimization.get_idle_resources()
print(f"\n=== Optimization Opportunities ===")
print(f"Idle Resources Found: {len(opt_data.get('resources', []))}")
print(f"Potential Savings: ${opt_data.get('potential_savings', 0):,.2f}/month")

# AI-powered insights
ai_insights = engine.ai.get_anomaly_detection()
print(f"\n=== AI Insights ===")
for anomaly in ai_insights.get('anomalies', []):
    print(f"Anomaly: {anomaly['service']} - {anomaly['description']}")
```

### 2. Multi-Engine Performance Comparison

```python
import time

# Compare query performance across engines
test_query = """
    SELECT
        product_servicecode,
        COUNT(*) as records,
        SUM(line_item_unblended_cost) as total_cost
    FROM CUR
    WHERE line_item_usage_start_date >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY 1
    ORDER BY 3 DESC
    LIMIT 10
"""

engines = ['duckdb', 'polars', 'athena']
results = {}

for engine_name in engines:
    start_time = time.time()
    try:
        result = engine.query(test_query, engine_name=engine_name)
        execution_time = time.time() - start_time
        results[engine_name] = {
            'success': True,
            'time': execution_time,
            'rows': len(result)
        }
        print(f"{engine_name}: {execution_time:.2f}s ({len(result)} rows)")
    except Exception as e:
        results[engine_name] = {
            'success': False,
            'error': str(e)
        }
        print(f"{engine_name}: Failed - {e}")
```

### 3. SQL File Execution

```python
# Execute SQL files directly
try:
    # Run a pre-built analytics query
    monthly_summary = engine.query("cur2_analytics/monthly_summary.sql")
    print("Monthly summary executed successfully")

    # Save results to local parquet for further analysis
    monthly_summary.to_parquet("output/monthly_summary.parquet")

except FileNotFoundError:
    print("SQL file not found - create cur2_analytics/monthly_summary.sql")
except Exception as e:
    print(f"Query execution failed: {e}")

# Query parquet files directly
try:
    parquet_data = engine.query("""
        SELECT *
        FROM 'output/monthly_summary.parquet'
        WHERE total_cost > 1000
        ORDER BY total_cost DESC
    """)
    print(f"Loaded {len(parquet_data)} high-cost records from parquet")
except Exception as e:
    print(f"Parquet query failed: {e}")
```

## 🌐 API Usage Examples

### Using the FastAPI Server

```bash
# Start the server
python main.py

# Or for production
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
```

### REST API Examples

```python
import requests
import json

# Query via REST API
def query_api(sql_query, engine="duckdb", output_format="json"):
    url = "http://localhost:8000/api/v1/finops/query"
    payload = {
        "query": sql_query,
        "engine": engine,
        "output_format": output_format
    }

    response = requests.post(url, json=payload)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"API Error: {response.status_code} - {response.text}")
        return None

# Example API calls
total_cost = query_api("SELECT SUM(line_item_unblended_cost) as total FROM CUR")
top_services = query_api("""
    SELECT product_servicecode, SUM(line_item_unblended_cost) as cost
    FROM CUR
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 5
""")

# Natural language queries
def natural_language_query(question):
    url = "http://localhost:8000/api/v1/finops/mcp/query"
    payload = {
        "query": question,
        "query_type": "natural_language"
    }

    response = requests.post(url, json=payload)
    return response.json() if response.status_code == 200 else None

# Examples
nl_result = natural_language_query("What are my top 5 AWS services by cost this month?")
```

### Bash/cURL Examples

```bash
# Simple cost query
curl -X POST "http://localhost:8000/api/v1/finops/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT COUNT(*) as total_records FROM CUR",
    "engine": "duckdb",
    "output_format": "json"
  }'

# Export to CSV
curl -X POST "http://localhost:8000/api/v1/finops/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT product_servicecode, SUM(line_item_unblended_cost) as cost FROM CUR GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
    "engine": "duckdb",
    "output_format": "csv"
  }' > top_services.csv

# Get KPI summary
curl -X GET "http://localhost:8000/api/v1/finops/kpi/summary" | jq .

# Natural language query
curl -X POST "http://localhost:8000/api/v1/finops/mcp/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Show me the cost trend for EC2 over the last 6 months",
    "query_type": "natural_language"
  }'
```

## 🔧 Advanced Configuration Examples

### Environment-Based Configuration

```python
import os
from infralyzer import DataConfig, DataExportType

# Production configuration
def get_production_config():
    return DataConfig(
        s3_bucket=os.environ['FINOPS_S3_BUCKET'],
        s3_data_prefix=os.environ['FINOPS_S3_PREFIX'],
        data_export_type=DataExportType(os.environ.get('FINOPS_DATA_TYPE', 'CUR2.0')),
        local_data_path=os.environ.get('FINOPS_LOCAL_PATH', '/opt/infralyzer/cache'),
        aws_region=os.environ.get('AWS_REGION'),
        aws_profile=os.environ.get('AWS_PROFILE'),
        prefer_local_data=True
    )

# Development configuration
def get_dev_config():
    return DataConfig(
        s3_bucket='dev-cost-bucket',
        s3_data_prefix='cur2/dev/data',
        data_export_type=DataExportType.CUR_2_0,
        local_data_path='./dev_cache',
        aws_profile='dev-profile',
        date_start='2024-01-01',
        date_end='2024-12-31'
    )

# Choose configuration based on environment
env = os.environ.get('ENVIRONMENT', 'development')
if env == 'production':
    config = get_production_config()
else:
    config = get_dev_config()

engine = FinOpsEngine(config)
```

### Custom Analytics Pipeline

```python
class CustomCostAnalytics:
    def __init__(self, engine):
        self.engine = engine

    def analyze_service_efficiency(self, service_name, days=30):
        """Analyze efficiency metrics for a specific service"""
        sql = f"""
        WITH service_metrics AS (
            SELECT
                DATE(line_item_usage_start_date) as usage_date,
                SUM(line_item_unblended_cost) as daily_cost,
                SUM(line_item_usage_amount) as daily_usage,
                COUNT(DISTINCT line_item_resource_id) as active_resources
            FROM CUR
            WHERE product_servicecode = '{service_name}'
            AND line_item_usage_start_date >= CURRENT_DATE - INTERVAL '{days} days'
            GROUP BY 1
        ),
        efficiency_metrics AS (
            SELECT
                usage_date,
                daily_cost,
                daily_usage,
                active_resources,
                CASE WHEN daily_usage > 0 THEN daily_cost / daily_usage ELSE 0 END as cost_per_unit,
                CASE WHEN active_resources > 0 THEN daily_cost / active_resources ELSE 0 END as cost_per_resource
            FROM service_metrics
        )
        SELECT
            usage_date,
            daily_cost,
            daily_usage,
            active_resources,
            ROUND(cost_per_unit, 4) as cost_per_unit,
            ROUND(cost_per_resource, 2) as cost_per_resource,
            ROUND(AVG(cost_per_unit) OVER (ORDER BY usage_date ROWS 6 PRECEDING), 4) as avg_cost_per_unit_7day
        FROM efficiency_metrics
        ORDER BY usage_date
        """

        return self.engine.query(sql)

    def forecast_monthly_cost(self, confidence_level=0.8):
        """Simple cost forecasting based on recent trends"""
        sql = f"""
        WITH daily_costs AS (
            SELECT
                DATE(line_item_usage_start_date) as cost_date,
                SUM(line_item_unblended_cost) as daily_cost
            FROM CUR
            WHERE line_item_usage_start_date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY 1
        ),
        trend_analysis AS (
            SELECT
                AVG(daily_cost) as avg_daily_cost,
                STDDEV(daily_cost) as std_daily_cost,
                COUNT(*) as days_analyzed
            FROM daily_costs
        )
        SELECT
            ROUND(avg_daily_cost * 30, 2) as forecasted_monthly_cost,
            ROUND((avg_daily_cost + std_daily_cost * 1.96) * 30, 2) as upper_bound_95pct,
            ROUND((avg_daily_cost - std_daily_cost * 1.96) * 30, 2) as lower_bound_95pct,
            days_analyzed
        FROM trend_analysis
        """

        return self.engine.query(sql)

# Usage
analytics = CustomCostAnalytics(engine)
ec2_efficiency = analytics.analyze_service_efficiency('AmazonEC2', days=30)
cost_forecast = analytics.forecast_monthly_cost()

print(f"Forecasted monthly cost: ${cost_forecast['forecasted_monthly_cost'].iloc[0]:,.2f}")
```

## 📈 Performance Optimization Examples

### Optimizing for Large Datasets

```python
# For very large datasets, use targeted queries
def analyze_large_dataset_efficiently(engine, start_date, end_date):
    """Efficiently analyze large datasets by breaking down queries"""

    # 1. Get overview first
    overview = engine.query(f"""
        SELECT
            COUNT(*) as total_records,
            SUM(line_item_unblended_cost) as total_cost,
            MIN(line_item_usage_start_date) as earliest_date,
            MAX(line_item_usage_start_date) as latest_date
        FROM CUR
        WHERE line_item_usage_start_date BETWEEN '{start_date}' AND '{end_date}'
    """)

    print(f"Dataset overview: {overview['total_records'].iloc[0]:,} records")

    # 2. Get top services to focus analysis
    top_services = engine.query(f"""
        SELECT product_servicecode
        FROM CUR
        WHERE line_item_usage_start_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 1
        ORDER BY SUM(line_item_unblended_cost) DESC
        LIMIT 10
    """)

    # 3. Analyze each top service separately
    service_analysis = {}
    for service in top_services['product_servicecode']:
        service_data = engine.query(f"""
            SELECT
                product_servicecode,
                DATE_TRUNC('week', line_item_usage_start_date) as week,
                SUM(line_item_unblended_cost) as weekly_cost,
                COUNT(DISTINCT line_item_resource_id) as unique_resources
            FROM CUR
            WHERE product_servicecode = '{service}'
            AND line_item_usage_start_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY 1, 2
            ORDER BY 2
        """)
        service_analysis[service] = service_data

    return overview, service_analysis

# Use efficient analysis
overview, services = analyze_large_dataset_efficiently(
    engine,
    '2024-01-01',
    '2024-12-31'
)
```

## 🔧 Troubleshooting Examples

### Common Issues and Solutions

```python
# Check data availability
def diagnose_data_issues(engine):
    """Diagnose common data availability issues"""

    try:
        # Test basic connectivity
        basic_test = engine.query("SELECT 1 as test")
        print("✅ Basic query connectivity working")

        # Check if CUR table exists and has data
        table_check = engine.query("""
            SELECT
                COUNT(*) as record_count,
                MIN(line_item_usage_start_date) as earliest_date,
                MAX(line_item_usage_start_date) as latest_date
            FROM CUR
        """)

        if table_check['record_count'].iloc[0] > 0:
            print(f"✅ CUR table has {table_check['record_count'].iloc[0]:,} records")
            print(f"   Date range: {table_check['earliest_date'].iloc[0]} to {table_check['latest_date'].iloc[0]}")
        else:
            print("❌ CUR table exists but has no data")

    except Exception as e:
        print(f"❌ Data connectivity issue: {e}")

        # Check if it's a local data issue
        try:
            cache_status = engine.check_local_data_status()
            if cache_status['local_file_count'] == 0:
                print("💡 No local data found. Try running: engine.download_data_locally()")
            else:
                print(f"📁 Local cache has {cache_status['local_file_count']} files")
        except:
            print("❌ Cannot check local data status")

# Performance debugging
def debug_query_performance(engine, sql_query):
    """Debug query performance issues"""

    import time

    print(f"🔍 Debugging query performance...")
    print(f"Query: {sql_query[:100]}...")

    # Test with different engines
    engines = ['duckdb', 'polars']

    for engine_name in engines:
        try:
            start_time = time.time()
            result = engine.query(sql_query, engine_name=engine_name)
            execution_time = time.time() - start_time

            print(f"✅ {engine_name}: {execution_time:.2f}s ({len(result)} rows)")

        except Exception as e:
            print(f"❌ {engine_name}: Failed - {e}")

    # Check if local data is being used
    if hasattr(engine.engine, 'has_local_data'):
        if engine.engine.has_local_data():
            print("💾 Using local data cache")
        else:
            print("☁️ Querying S3 directly (slower, more expensive)")

# Usage
diagnose_data_issues(engine)
debug_query_performance(engine, "SELECT COUNT(*) FROM CUR")
```

These examples provide a comprehensive guide to using Infralyzer for various FinOps scenarios. Start with the basic examples and gradually move to more advanced use cases as you become familiar with the platform.
