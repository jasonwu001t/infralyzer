# DE-Polars: Advanced FinOps Cost Analytics Platform

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104+-green.svg)](https://fastapi.tiangolo.com/)
[![DuckDB](https://img.shields.io/badge/DuckDB-SQL%20Engine-orange.svg)](https://duckdb.org/)

**DE-Polars** is a comprehensive FinOps cost analytics platform that provides advanced SQL analysis of AWS Cost and Usage Reports (CUR) with local data caching for massive cost savings. Built with a modular architecture supporting FastAPI deployment for enterprise-grade cost optimization.

## 🚀 Key Features

- **🧠 Advanced SQL Analytics**: DuckDB-powered SQL engine with window functions, CTEs, and complex joins
- **💾 Local Data Caching**: Download S3 data locally to eliminate ongoing S3 query costs (90%+ cost reduction)
- **📊 Modular Architecture**: Independent analytics modules for flexible deployment
- **🔌 FastAPI Integration**: Production-ready REST API with auto-generated OpenAPI docs
- **🤖 AI-Powered Insights**: Machine learning-based anomaly detection and optimization
- **⚡ Real-time Analytics**: Comprehensive KPI dashboards and cost optimization recommendations
- **🔄 Backward Compatible**: Existing DataExportsPolars code continues to work unchanged
- **🔧 Rich Utilities**: Built-in formatters, validators, performance monitoring, and export tools

## 📁 Architecture Overview

```
de_polars/
├── engine/           # Core DuckDB SQL execution engine
├── data/             # S3 and local data management
├── analytics/        # Modular cost analytics components
│   ├── kpi_summary.py         # ⭐ Comprehensive KPI dashboard
│   ├── spend_analytics.py     # Spend visibility & trends
│   ├── optimization.py        # Cost optimization recommendations
│   ├── allocation.py          # Cost allocation & tagging
│   ├── discounts.py           # Discount tracking & negotiation
│   ├── ai_recommendations.py  # AI-powered insights
│   └── mcp_integration.py     # Model Context Protocol support
├── api/              # FastAPI REST API layer
├── utils/            # Shared utility functions
│   ├── formatters.py         # Currency, number, date formatting
│   ├── validators.py         # Data quality & config validation
│   ├── performance.py        # Query profiling & caching
│   └── exports.py            # Data export & report generation
└── finops_engine.py  # Unified interface
```

## 🛠️ Installation

### Prerequisites

- Python 3.8+
- AWS credentials configured
- S3 bucket with Cost and Usage Report data

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Install Package

```bash
pip install -e .
```

## 🎯 Quick Start

### Option 1: New Modular Interface (Recommended)

```python
from de_polars import FinOpsEngine, DataConfig, DataExportType

# Configure your data source
config = DataConfig(
    s3_bucket='my-cost-data-bucket',
    s3_data_prefix='cur2/cur2/data',
    data_export_type=DataExportType.CUR_2_0,
    table_name='CUR',
    local_data_path='./local_data',  # Enable cost-saving local cache
    prefer_local_data=True
)

# Initialize FinOps engine
engine = FinOpsEngine(config)

# One-time: Download data locally (eliminates future S3 costs)
engine.download_data_locally()

# Access any analytics module independently
kpi_summary = engine.kpi.get_comprehensive_summary()
spend_analysis = engine.spend.get_invoice_summary()
optimization = engine.optimization.get_idle_resources()
```

### Option 2: Backward Compatible Interface

```python
from de_polars import DataExportsPolars

# Existing code works unchanged!
data = DataExportsPolars(
    s3_bucket='my-cost-data-bucket',
    s3_data_prefix='cur2/cur2/data',
    data_export_type='CUR2.0',
    local_data_path='./local_data'
)

# Download data locally for cost savings
data.download_data_locally()

# Execute SQL queries
result = data.query("""
    SELECT
        product_servicecode,
        SUM(line_item_unblended_cost) as total_cost
FROM CUR
    WHERE line_item_unblended_cost > 0
    GROUP BY 1
    ORDER BY 2 DESC
LIMIT 10
""")
```

## 🌐 FastAPI Deployment

### Create FastAPI Application

Create `main.py`:

```python
from de_polars.api import create_finops_app

# Method 1: Direct configuration
app = create_finops_app(
    s3_bucket='my-cost-data-bucket',
    s3_data_prefix='cur2/cur2/data',
    data_export_type='CUR2.0',
    local_data_path='./local_data'
)

# Method 2: Environment variables
# app = create_finops_app_from_env()
```

### Environment Configuration

Create `.env` file:

```bash
FINOPS_S3_BUCKET=my-cost-data-bucket
FINOPS_S3_PREFIX=cur2/cur2/data
FINOPS_DATA_TYPE=CUR2.0
FINOPS_LOCAL_PATH=./local_data
FINOPS_TABLE_NAME=CUR
AWS_REGION=us-east-1
```

### Run FastAPI Server

```bash
# Development server
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# Production server
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
```

### Access API Documentation

- **Interactive Docs**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **Health Check**: http://localhost:8000/health

## 📊 Available API Endpoints

### ⭐ KPI Summary Dashboard

```http
GET /api/v1/finops/kpi/summary
GET /api/v1/finops/kpi/health-check
GET /api/v1/finops/kpi/executive-summary
GET /api/v1/finops/kpi/dashboard-data
```

### 💰 Spend Analytics

```http
GET /api/v1/finops/spend/invoice/summary
GET /api/v1/finops/spend/regions/top
GET /api/v1/finops/spend/services/top
GET /api/v1/finops/spend/breakdown
POST /api/v1/finops/spend/export
```

### ⚡ Cost Optimization

```http
GET /api/v1/finops/optimization/idle-resources
GET /api/v1/finops/optimization/rightsizing
GET /api/v1/finops/optimization/cross-service-migration
GET /api/v1/finops/optimization/vpc-charges
POST /api/v1/finops/optimization/implement-recommendation
```

### 🏷️ Cost Allocation & Tagging

```http
GET /api/v1/finops/allocation/account-hierarchy
GET /api/v1/finops/allocation/tagging-compliance
GET /api/v1/finops/allocation/cost-center-breakdown
POST /api/v1/finops/allocation/tagging-rules
GET /api/v1/finops/allocation/third-party-integration
```

### 💳 Discount Tracking

```http
GET /api/v1/finops/discounts/current-agreements
GET /api/v1/finops/discounts/negotiation-opportunities
GET /api/v1/finops/discounts/usage-forecasting
POST /api/v1/finops/discounts/commitment-planning
```

### 🤖 AI Recommendations

```http
GET /api/v1/finops/ai/anomaly-detection
GET /api/v1/finops/ai/optimization-insights
POST /api/v1/finops/ai/custom-analysis
GET /api/v1/finops/ai/forecasting
```

### 🔌 MCP Integration

```http
GET /api/v1/finops/mcp/resources
GET /api/v1/finops/mcp/tools
POST /api/v1/finops/mcp/query
WebSocket /api/v1/finops/mcp/stream
```

## 🔧 Creating Custom API Endpoints

### Step 1: Create Analytics Module

Create `de_polars/analytics/custom_analytics.py`:

```python
"""
Custom Analytics Module - Your specialized cost analysis
"""
import polars as pl
from typing import Dict, Any
from ..engine.duckdb_engine import DuckDBEngine

class CustomAnalytics:
    """Custom cost analytics functionality."""

    def __init__(self, engine: DuckDBEngine):
        self.engine = engine
        self.config = engine.config

    def get_custom_metrics(self) -> Dict[str, Any]:
        """Your custom metric calculation."""
        sql = f"""
        SELECT
            product_servicecode,
            DATE_TRUNC('month', line_item_usage_start_date) as month,
            SUM(line_item_unblended_cost) as monthly_cost
        FROM {self.config.table_name}
    WHERE line_item_unblended_cost > 0
        GROUP BY 1, 2
        ORDER BY 3 DESC
        """

        result = self.engine.query(sql)

        # Process results
        metrics = []
        for row in result.iter_rows(named=True):
            metrics.append({
                "service": row["product_servicecode"],
                "month": str(row["month"]),
                "cost": float(row["monthly_cost"])
            })

        return {"custom_metrics": metrics}
```

### Step 2: Add to FinOpsEngine

Update `de_polars/finops_engine.py`:

```python
from .analytics.custom_analytics import CustomAnalytics

class FinOpsEngine:
    def __init__(self, config: DataConfig):
        # ... existing code ...
        self._custom = None

    @property
    def custom(self) -> CustomAnalytics:
        """Access Custom Analytics module."""
        if self._custom is None:
            self._custom = CustomAnalytics(self.engine)
        return self._custom
```

### Step 3: Create API Endpoints

Create `de_polars/api/endpoints/custom_endpoints.py`:

```python
"""
Custom API endpoints
"""
from fastapi import APIRouter, Depends, HTTPException
from ...finops_engine import FinOpsEngine

router = APIRouter()

@router.get("/custom/metrics")
async def get_custom_metrics(engine: FinOpsEngine = Depends()):
    """Get custom cost metrics."""
    try:
        result = engine.custom.get_custom_metrics()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
```

### Step 4: Register Router

Update `de_polars/api/fastapi_app.py`:

```python
from .endpoints.custom_endpoints import router as custom_router

def _create_app(self) -> FastAPI:
    # ... existing code ...
    app.include_router(custom_router, prefix="/api/v1/finops", tags=["Custom"])
```

## 📈 Example Use Cases

### 1. Comprehensive Cost Dashboard

```python
# Get complete dashboard data
dashboard = engine.get_dashboard_data()

print(f"Monthly Spend: ${dashboard['spend_summary']['invoice_total']:,.2f}")
print(f"Optimization Potential: ${dashboard['kpi_summary']['savings_summary']['total_potential_savings']:,.2f}")
```

### 2. Cost Health Assessment

```python
# Run comprehensive cost health check
health = engine.run_cost_health_check()

print(f"Overall Health Score: {health['overall_score']}/100")
for category, score in health['category_scores'].items():
    print(f"  {category}: {score}/100")
```

### 3. AI-Powered Analysis

```python
# Detect cost anomalies
anomalies = engine.ai.detect_anomalies(lookback_days=30)
print(f"Found {len(anomalies['anomalies'])} cost anomalies")

# Natural language queries
analysis = engine.ai.analyze_custom_query("What are my top 5 most expensive services?")
print(f"Query: {analysis['query']}")
print(f"Results: {analysis['narrative_insights']}")
```

### 4. Utility Functions

```python
from de_polars import CurrencyFormatter, NumberFormatter, DataValidator

# Format currency values
formatted_cost = CurrencyFormatter.format_currency(125432.50)  # "$125,432.50"
large_cost = CurrencyFormatter.format_large_currency(1250000)  # "$1.25M"

# Format percentages and numbers
growth_rate = NumberFormatter.format_percentage(15.7)  # "+15.7%"
resource_count = NumberFormatter.format_large_number(1500000)  # "1.5M"

# Validate data quality
validation = DataValidator.validate_cost_data(your_dataframe)
print(f"Data quality score: {validation['data_quality_score']}/100")
```

### 5. Advanced SQL Analytics

```python
# Complex analytical query
result = engine.query("""
WITH monthly_trends AS (
    SELECT
        DATE_TRUNC('month', line_item_usage_start_date) as month,
        product_servicecode,
        SUM(line_item_unblended_cost) as cost,
        LAG(SUM(line_item_unblended_cost)) OVER (
            PARTITION BY product_servicecode
            ORDER BY DATE_TRUNC('month', line_item_usage_start_date)
        ) as prev_month_cost
    FROM CUR
    WHERE line_item_unblended_cost > 0
    GROUP BY 1, 2
)
SELECT
    product_servicecode,
    month,
    cost,
    (cost - prev_month_cost) / prev_month_cost * 100 as growth_rate
FROM monthly_trends
WHERE prev_month_cost > 0
ORDER BY ABS(growth_rate) DESC
""")
```

## 🐳 Docker Deployment

Create `Dockerfile`:

```dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
RUN pip install -e .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

Create `docker-compose.yml`:

```yaml
version: "3.8"
services:
  finops-api:
    build: .
    ports:
      - "8000:8000"
    environment:
      - FINOPS_S3_BUCKET=my-cost-data-bucket
      - FINOPS_S3_PREFIX=cur2/cur2/data
      - FINOPS_DATA_TYPE=CUR2.0
      - FINOPS_LOCAL_PATH=/app/data
    volumes:
      - ./data:/app/data
```

Run with:

```bash
docker-compose up -d
```

## 💾 Cost Optimization with Local Caching

### Initial Setup (One-time)

```python
engine = FinOpsEngine(config)

# Download S3 data to local storage (one-time cost)
engine.download_data_locally()
```

### Ongoing Usage (Zero S3 costs)

```python
# All future queries use local data automatically
result = engine.query("SELECT * FROM CUR WHERE ...")  # No S3 charges!

# Check data status
status = engine.check_local_data_status()
print(f"Local data: {status['total_files']} files, {status['total_size_mb']:.1f} MB")
```

## 🔒 Security & Authentication

### API Key Authentication

Update `main.py`:

```python
from fastapi import Depends, HTTPException, Header

async def get_api_key(x_api_key: str = Header()):
    if x_api_key != "your-secret-api-key":
        raise HTTPException(status_code=401, detail="Invalid API key")
    return x_api_key

# Add dependency to routes
@app.get("/api/v1/finops/kpi/summary", dependencies=[Depends(get_api_key)])
async def protected_endpoint():
    # ... endpoint logic
```

### AWS IAM Roles

For production, use IAM roles instead of access keys:

```python
config = DataConfig(
    s3_bucket='my-cost-data-bucket',
    s3_data_prefix='cur2/cur2/data',
    data_export_type=DataExportType.CUR_2_0,
    # No AWS credentials needed - uses IAM role
)
```

## 📋 Supported Data Export Types

- **CUR 2.0**: `DataExportType.CUR_2_0` - AWS Cost and Usage Report v2.0
- **FOCUS 1.0**: `DataExportType.FOCUS_1_0` - FinOps Open Cost and Usage Specification
- **COH**: `DataExportType.COH` - AWS Cost Optimization Hub
- **Carbon Emissions**: `DataExportType.CARBON_EMISSION` - AWS Carbon Footprint

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add your analytics module or API endpoint
4. Test thoroughly
5. Submit a pull request

## 📝 License

MIT License - see LICENSE file for details.

## 🆘 Support

- **Documentation**: Check the `/docs` endpoint when running the API
- **Issues**: GitHub Issues
- **Examples**: See `example_new_modular_usage.py`

---

**Ready to optimize your AWS costs?** 🚀

Start with local data caching to eliminate S3 query costs, then build your custom analytics on top of the modular architecture!
