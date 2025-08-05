# Infralyzer: Modern FinOps Cost Analytics Platform

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104+-green.svg)](https://fastapi.tiangolo.com/)
[![DuckDB](https://img.shields.io/badge/DuckDB-SQL%20Engine-orange.svg)](https://duckdb.org/)
[![Polars](https://img.shields.io/badge/Polars-DataFrame-blue.svg)](https://pola.rs/)
[![AWS Athena](https://img.shields.io/badge/AWS-Athena-orange.svg)](https://aws.amazon.com/athena/)

**Modern AWS Cost Analytics Platform** with multi-engine SQL support, local data caching, and comprehensive FinOps insights.

## 🚀 Key Features

- **🔍 Multi-Engine Query Support**: DuckDB, Polars, and AWS Athena engines
- **📊 Comprehensive Analytics**: KPI dashboards, spend analysis, optimization insights
- **💾 Smart Data Caching**: Download S3 data locally to reduce costs by 90%+
- **🌐 Modern FastAPI**: Clean REST API with OpenAPI documentation
- **📄 SQL File Execution**: Direct execution of .sql files and parquet queries
- **🤖 AI-Powered Insights**: Natural language queries and ML-based recommendations
- **🧠 AWS Bedrock Integration**: Advanced chatbot with knowledge base support
- **💬 Structured Query Generation**: Convert natural language to optimized SQL
- **📚 Knowledge Base Management**: CUR-specific AI context and citations
- **⚡ High Performance**: Optimized for large-scale cost data analysis
- **🎯 Production Ready**: Comprehensive error handling, logging, and monitoring

## 🏗️ Architecture Overview

### System Design

Infralyzer follows a layered, modern architecture with comprehensive AI integration:

**📈 Complete Flow:** AWS S3 → Data Management → Query Engines → FinOps Engine → Analytics → AI/API → Frontend

### 🏛️ Detailed Architecture

The system is built with clear separation of concerns across multiple layers:

```mermaid
graph LR
    subgraph "Data Sources"
        A[AWS S3<br/>CUR Data]
        B[Local Cache<br/>Parquet Files]
        C[AWS APIs<br/>Pricing Data]
    end

    subgraph "Data Layer"
        D[Data Manager]
        E[S3 Data Manager]
        F[Local Data Manager]
        G[API Data Manager]
    end

    subgraph "Processing Layer"
        H[Query Engine Factory]
        I[DuckDB Engine<br/>⚡ Fast Analytics]
        J[Polars Engine<br/>🔄 Modern DataFrames]
        K[Athena Engine<br/>☁️ Serverless Scale]
    end

    subgraph "Core Layer"
        L[FinOps Engine<br/>🎯 Main Interface]
        M[Configuration<br/>⚙️ DataConfig]
    end

    subgraph "Analytics Layer"
        N[KPI Analytics<br/>📊 Dashboards]
        O[Spend Analytics<br/>💰 Cost Tracking]
        P[Optimization<br/>🔍 Recommendations]
        Q[AI Recommendations<br/>🤖 ML Insights]
    end

    subgraph "AI Layer"
        R[Bedrock Handler<br/>🧠 AI Integration]
        S[Knowledge Base<br/>📚 CUR Context]
        T[Query Generation<br/>💬 NL to SQL]
    end

    subgraph "API Layer"
        U[FastAPI App<br/>🌐 REST API]
        V[Authentication<br/>🔐 Security]
        W[Endpoints<br/>📡 Routes]
    end

    subgraph "Output Layer"
        X[JSON/CSV/Arrow<br/>📄 Data Formats]
        Y[Visualizations<br/>📈 Charts]
        Z[AI Chat<br/>💬 Conversations]
    end

    A --> D
    B --> D
    C --> D

    D --> E
    D --> F
    D --> G

    E --> H
    F --> H
    G --> H

    H --> I
    H --> J
    H --> K

    I --> L
    J --> L
    K --> L
    M --> L

    L --> N
    L --> O
    L --> P
    L --> Q
    L --> R

    R --> S
    R --> T

    N --> U
    O --> U
    P --> U
    Q --> U
    R --> U

    U --> V
    U --> W

    W --> X
    W --> Y
    W --> Z

    classDef sourceLayer fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    classDef dataLayer fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    classDef processLayer fill:#e8f5e8,stroke:#1b5e20,stroke-width:2px
    classDef coreLayer fill:#fff3e0,stroke:#e65100,stroke-width:2px
    classDef analyticsLayer fill:#fce4ec,stroke:#880e4f,stroke-width:2px
    classDef aiLayer fill:#f1f8e9,stroke:#33691e,stroke-width:2px
    classDef apiLayer fill:#e0f2f1,stroke:#00695c,stroke-width:2px
    classDef outputLayer fill:#f8e0f8,stroke:#6a1b9a,stroke-width:2px

    class A,B,C sourceLayer
    class D,E,F,G dataLayer
    class H,I,J,K processLayer
    class L,M coreLayer
    class N,O,P,Q analyticsLayer
    class R,S,T aiLayer
    class U,V,W apiLayer
    class X,Y,Z outputLayer
```

### 🧠 AI-Powered Query Flow

The new Bedrock integration provides sophisticated AI capabilities:

```mermaid
graph TB
    subgraph "Request Flow"
        Client[Client Application]
        API[FastAPI Router]
        Auth[Authentication Layer]
    end

    subgraph "Bedrock Integration Layer"
        BH[Bedrock Handler]
        MC[Model Configuration]
        KB[Knowledge Base Manager]
    end

    subgraph "AI Model Services"
        Claude[Claude 3.5 Sonnet<br/>Complex Reasoning]
        Haiku[Claude 3 Haiku<br/>Fast Queries]
        Titan[Amazon Titan<br/>Cost Effective]
        Cohere[Cohere Command<br/>Conversations]
        Llama[Meta Llama<br/>Open Source]
    end

    subgraph "Knowledge Base System"
        S3KB[S3 Knowledge Data]
        Embed[Vector Embeddings]
        Search[Vector Search]
        Citation[Source Citations]
    end

    subgraph "Query Generation"
        NLP[Natural Language<br/>Processing]
        SQLGen[SQL Generation]
        Validation[Query Validation]
        Optimize[Query Optimization]
    end

    subgraph "Execution Engine"
        QueryEngine[Query Engines<br/>DuckDB/Polars/Athena]
        CURData[CUR Data]
        Results[Query Results]
    end

    subgraph "Response Processing"
        Format[Result Formatting]
        Viz[Visualization Metadata]
        Struct[Structured Response]
    end

    Client --> API
    API --> Auth
    Auth --> BH

    BH --> MC
    BH --> KB
    BH --> NLP

    MC --> Claude
    MC --> Haiku
    MC --> Titan
    MC --> Cohere
    MC --> Llama

    KB --> S3KB
    KB --> Embed
    KB --> Search
    KB --> Citation

    NLP --> SQLGen
    SQLGen --> Validation
    Validation --> Optimize

    Optimize --> QueryEngine
    QueryEngine --> CURData
    CURData --> Results

    Results --> Format
    Format --> Viz
    Viz --> Struct

    Struct --> Client

    Claude --> SQLGen
    Haiku --> SQLGen
    Titan --> SQLGen

    Search --> Citation
    Citation --> Struct

    classDef requestLayer fill:#e3f2fd
    classDef bedrockLayer fill:#f3e5f5
    classDef modelLayer fill:#fff3e0
    classDef kbLayer fill:#e8f5e8
    classDef queryLayer fill:#fce4ec
    classDef engineLayer fill:#f1f8e9
    classDef responseLayer fill:#e0f2f1

    class Client,API,Auth requestLayer
    class BH,MC,KB bedrockLayer
    class Claude,Haiku,Titan,Cohere,Llama modelLayer
    class S3KB,Embed,Search,Citation kbLayer
    class NLP,SQLGen,Validation,Optimize queryLayer
    class QueryEngine,CURData,Results engineLayer
    class Format,Viz,Struct responseLayer
```

### 🎯 Key Components

#### Core Infrastructure

- **🔧 DataConfig**: Central configuration for S3, local paths, and AWS settings
- **💾 DataManager**: Handles S3 discovery, local caching, and data synchronization
- **🧠 Query Engines**: Choose between DuckDB (fast), Polars (modern), or Athena (scale)
- **🎯 FinOpsEngine**: Your main interface - handles all queries and analytics

#### Analytics Modules

- **📊 KPI Analytics**: Real-time cost metrics and dashboard data
- **💰 Spend Analytics**: Cost tracking, trends, and invoice analysis
- **🔍 Optimization Analytics**: Resource optimization and rightsizing
- **🏷️ Allocation Analytics**: Tag-based cost allocation and chargeback
- **💳 Discount Analytics**: Savings plans, reserved instances, and discounts
- **🤖 AI Analytics**: Machine learning insights and anomaly detection

#### AI & Chatbot Layer

- **🧠 Bedrock Handler**: AWS Bedrock integration and model management
- **📚 Knowledge Base**: CUR-specific context with vector search capabilities
- **💬 Query Generation**: Natural language to SQL conversion
- **🎯 Model Configuration**: Support for Claude, Titan, Cohere, and Llama models

#### API & Integration

- **🌐 FastAPI**: Production REST API with OpenAPI documentation
- **🔐 Authentication**: JWT and role-based access control
- **📡 Endpoints**: Comprehensive API coverage for all features

### 🚀 Usage Flow

```mermaid
graph LR
    A["1️⃣ Configure<br/>DataConfig"] --> B["2️⃣ Initialize<br/>FinOpsEngine"]
    B --> C["3️⃣ Query<br/>engine.query()"]
    C --> D["4️⃣ Analyze<br/>Results"]

    style A fill:#e1f5fe
    style B fill:#f3e5f5
    style C fill:#e8f5e8
    style D fill:#fff3e0
```

## 🚀 Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/jasonwu001t/infralyzer.git
cd infralyzer

# Install dependencies
pip install -r requirements.txt

# Install the package
pip install -e .
```

### Option 1: Python Interface (Recommended)

```python
from infralyzer import FinOpsEngine, DataConfig, DataExportType

# Configure your data source
config = DataConfig(
    s3_bucket='your-cost-data-bucket',
    s3_data_prefix='cur2/data',
    data_export_type=DataExportType.CUR_2_0,
    local_data_path='./local_data',  # Optional: for caching
    table_name='CUR'
)

# Initialize the FinOps engine
engine = FinOpsEngine(config)

# Option: Download data locally (one-time setup for cost savings)
# engine.download_data_locally()

# Execute SQL queries (returns pandas DataFrame by default)
df = engine.query("SELECT * FROM CUR LIMIT 10")

# Use convenient output format methods
json_result = engine.query_json("SELECT product_servicecode, SUM(line_item_unblended_cost) as cost FROM CUR GROUP BY 1 LIMIT 5")
csv_result = engine.query_csv("SELECT * FROM CUR LIMIT 100")

# Execute SQL files directly
result = engine.query("cur2_analytics/cost_summary.sql")

# Query parquet files directly
result = engine.query("SELECT * FROM 'exports/monthly_costs.parquet' WHERE cost > 1000")

# Access specialized analytics
kpi_summary = engine.kpi.get_summary()
spend_analysis = engine.spend.get_invoice_summary()
optimization = engine.optimization.get_idle_resources()
```

### Option 2: FastAPI Server

```bash
# Start the development server
python main.py

# Or for production
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
```

```bash
# Query via REST API
curl -X POST "http://localhost:8000/api/v1/finops/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT * FROM CUR LIMIT 10",
    "engine": "duckdb",
    "output_format": "json"
  }'

# Natural language queries (MCP)
curl -X POST "http://localhost:8000/api/v1/finops/mcp/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "What are my top 5 services by cost this month?",
    "query_type": "natural_language"
  }'

# AI-powered query generation (NEW!)
curl -X POST "http://localhost:8000/api/v1/finops/bedrock/generate-query" \
  -H "Content-Type: application/json" \
  -d '{
    "user_query": "Show me EC2 costs by region for the last 6 months"
  }'

# AI chatbot with knowledge base (NEW!)
curl -X POST "http://localhost:8000/api/v1/finops/bedrock/chat" \
  -H "Content-Type: application/json" \
  -d '{
    "message": "Analyze my cost trends and suggest optimizations",
    "knowledge_base_id": "kb-12345",
    "include_sources": true
  }'
```

## 🎯 Core Capabilities

### 1. 🔍 Multi-Engine Query Support

Choose the best engine for your use case:

```python
# Fast analytics with DuckDB (default)
result = engine.query("SELECT * FROM CUR LIMIT 1000", engine_name="duckdb")

# Modern DataFrame processing with Polars
result = engine.query("SELECT * FROM CUR LIMIT 1000", engine_name="polars")

# Serverless cloud queries with Athena
result = engine.query("SELECT * FROM CUR LIMIT 1000", engine_name="athena")
```

### 2. 📄 Flexible Query Types

Execute various query types seamlessly:

```python
# SQL query strings
df = engine.query("SELECT product_servicecode, SUM(line_item_unblended_cost) as cost FROM CUR GROUP BY 1")

# SQL files
df = engine.query("cur2_analytics/monthly_summary.sql")

# Direct parquet file queries
df = engine.query("SELECT * FROM 'data/costs_2024.parquet' WHERE cost > 1000")

# Multi-format output
json_data = engine.query_json("SELECT * FROM CUR LIMIT 10")
csv_data = engine.query_csv("SELECT * FROM CUR LIMIT 10")
```

### 3. 🧠 AWS Bedrock AI Integration (NEW!)

Transform cost analysis with advanced AI capabilities:

```python
from infralyzer.utils.bedrock_handler import BedrockHandler, BedrockModel, ModelConfiguration

# Initialize Bedrock handler
bedrock = BedrockHandler(config, default_model=BedrockModel.CLAUDE_3_5_SONNET)

# List available AI models
models = bedrock.list_available_models()
print(f"Available models: {len(models)}")

# Generate structured queries from natural language
query_result = bedrock.generate_cur_structured_query(
    user_query="What are my top 5 most expensive services this month?",
    model_config=ModelConfiguration(
        model_id=BedrockModel.CLAUDE_3_5_SONNET,
        temperature=0.1,
        max_tokens=4096
    )
)

# Extract the generated SQL and metadata
structured_query = query_result['structured_query']
sql_query = structured_query['sql_query']
chart_type = structured_query['visualization_type']
title = structured_query['title']

print(f"Generated SQL: {sql_query}")
print(f"Visualization: {chart_type}")
print(f"Confidence: {query_result['confidence']}")

# Execute the AI-generated query
results = engine.query(sql_query)

# Create knowledge base for CUR data
kb_result = bedrock.create_cur_knowledge_base(
    s3_bucket="your-cur-bucket",
    s3_prefix="cur2/data",
    role_arn="arn:aws:iam::123456789012:role/BedrockRole",
    name="CUR-FinOps-Knowledge"
)

# Chat with AI using knowledge base context
chat_response = bedrock.chat_with_knowledge_base(
    message="Analyze my EC2 costs and suggest optimizations",
    knowledge_base_id=kb_result['knowledge_base_id'],
    conversation_id="cost-analysis-session"
)

print(f"AI Response: {chat_response['response']}")
print(f"Knowledge Sources: {len(chat_response['knowledge_sources'])}")
```

### 4. 📊 Comprehensive Analytics

Access specialized FinOps analytics:

```python
# KPI Dashboard
kpi_data = engine.kpi.get_summary()
# Returns: cost trends, service distribution, account metrics

# Spend Analysis
spend_data = engine.spend.get_invoice_summary()
# Returns: monthly costs, service breakdown, account analysis

# Cost Optimization
optimization_data = engine.optimization.get_idle_resources()
# Returns: unused resources, rightsizing opportunities

# Cost Allocation
allocation_data = engine.allocation.get_cost_allocation_overview()
# Returns: tag-based allocation, account hierarchy

# Discount Analysis
discount_data = engine.discounts.get_current_agreements()
# Returns: RI utilization, savings plans, negotiated discounts

# AI Insights
ai_insights = engine.ai.get_anomaly_detection()
# Returns: cost anomalies, forecasting, recommendations

# AWS Bedrock Chatbot (NEW!)
from infralyzer.utils.bedrock_handler import BedrockHandler, BedrockModel

bedrock = BedrockHandler(config, default_model=BedrockModel.CLAUDE_3_5_SONNET)

# Generate structured queries from natural language
query_result = bedrock.generate_cur_structured_query(
    "What are my top 5 most expensive services this month?"
)
# Returns: Optimized SQL, visualization metadata, confidence scores

# Chat with knowledge base
chat_response = bedrock.chat_with_knowledge_base(
    message="Analyze my EC2 costs and suggest optimizations",
    knowledge_base_id="kb-12345"
)
# Returns: AI response with CUR data context and citations
```

### 5. 🌐 Production-Ready API

Access everything via REST endpoints:

| Endpoint                                     | Method | Purpose                             |
| -------------------------------------------- | ------ | ----------------------------------- |
| `/api/v1/finops/query`                       | POST   | Execute SQL queries, files, parquet |
| `/api/v1/finops/mcp/query`                   | POST   | Natural language queries            |
| `/api/v1/finops/kpi/summary`                 | GET    | KPI dashboard data                  |
| `/api/v1/finops/spend/invoice/summary`       | GET    | Spend analysis                      |
| `/api/v1/finops/optimization/idle-resources` | GET    | Cost optimization                   |
| `/api/v1/finops/ai/anomaly-detection`        | GET    | AI insights                         |
| **AWS Bedrock Endpoints (NEW!)**             |        |                                     |
| `/api/v1/finops/bedrock/models`              | GET    | List available AI models            |
| `/api/v1/finops/bedrock/generate-query`      | POST   | Natural language to SQL conversion  |
| `/api/v1/finops/bedrock/chat`                | POST   | AI chatbot with knowledge base      |
| `/api/v1/finops/bedrock/knowledge-base`      | POST   | Create/manage knowledge bases       |
| `/api/v1/finops/bedrock/knowledge-bases`     | GET    | List knowledge bases                |
| `/docs`                                      | GET    | Interactive API documentation       |

## 🎯 Data Flow & Use Cases

### Real-World Usage Patterns

The system supports various data flow patterns for different use cases:

```mermaid
graph TB
    subgraph "Frontend Applications"
        Dashboard["Cost Dashboard<br/>📊 React/Vue"]
        Mobile["Mobile App<br/>📱 React Native"]
        CLI["CLI Tools<br/>⚡ Command Line"]
        BI["BI Tools<br/>📈 Tableau/PowerBI"]
    end

    subgraph "Use Case 1: Real-time Cost Analysis"
        UC1_Start["User Query:<br/>Show monthly trends"]
        UC1_API["FastAPI Endpoint"]
        UC1_Engine["DuckDB Engine<br/>Fast Processing"]
        UC1_Cache["Local Cache<br/>Instant Results"]
        UC1_Viz["Chart Generation<br/>Line Chart"]
    end

    subgraph "Use Case 2: AI-Powered Insights"
        UC2_Start["Natural Language:<br/>What is driving my costs"]
        UC2_Bedrock["Bedrock Handler"]
        UC2_Model["Claude 3.5 Sonnet<br/>Complex Reasoning"]
        UC2_KB["Knowledge Base<br/>CUR Context"]
        UC2_SQL["Generated SQL<br/>Optimized Query"]
        UC2_Results["Structured Response<br/>Insights + Citations"]
    end

    subgraph "Use Case 3: Enterprise Analytics"
        UC3_Start["Scheduled Report:<br/>Monthly Analysis"]
        UC3_Athena["Athena Engine<br/>Serverless Scale"]
        UC3_S3["S3 Data Source<br/>Massive Datasets"]
        UC3_Export["Export Manager<br/>CSV/JSON/Arrow"]
        UC3_Delivery["Report Delivery<br/>Email/Slack/S3"]
    end

    Dashboard --> UC1_Start
    Mobile --> UC2_Start
    CLI --> UC3_Start
    BI --> UC1_API

    UC1_Start --> UC1_API
    UC1_API --> UC1_Engine
    UC1_Engine --> UC1_Cache
    UC1_Cache --> UC1_Viz
    UC1_Viz --> Dashboard

    UC2_Start --> UC2_Bedrock
    UC2_Bedrock --> UC2_Model
    UC2_Bedrock --> UC2_KB
    UC2_Model --> UC2_SQL
    UC2_KB --> UC2_Results
    UC2_SQL --> UC2_Results
    UC2_Results --> Mobile

    UC3_Start --> UC3_Athena
    UC3_Athena --> UC3_S3
    UC3_S3 --> UC3_Export
    UC3_Export --> UC3_Delivery
    UC3_Delivery --> CLI

    classDef frontend fill:#e3f2fd,stroke:#1976d2,stroke-width:2px
    classDef usecase1 fill:#e8f5e8,stroke:#2e7d32,stroke-width:2px
    classDef usecase2 fill:#fff3e0,stroke:#f57c00,stroke-width:2px
    classDef usecase3 fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px

    class Dashboard,Mobile,CLI,BI frontend
    class UC1_Start,UC1_API,UC1_Engine,UC1_Cache,UC1_Viz usecase1
    class UC2_Start,UC2_Bedrock,UC2_Model,UC2_KB,UC2_SQL,UC2_Results usecase2
    class UC3_Start,UC3_Athena,UC3_S3,UC3_Export,UC3_Delivery usecase3
```

## 💾 Data Caching Strategy

Reduce S3 costs by 90%+ with smart local caching:

```python
# One-time setup: Download data locally
engine.download_data_locally()

# Automatic cost savings
# - First query: Downloads from S3 (~$0.10 per GB)
# - Subsequent queries: Uses local cache (~$0.00)
# - Result: 90%+ cost reduction for repeated analytics

# Check cache status
cache_status = engine.check_local_data_status()
print(f"Local files: {cache_status['local_file_count']}")
print(f"Total size: {cache_status['total_size_gb']:.2f} GB")
```

## 🔧 Configuration Options

### Environment Variables

Set these for automatic configuration:

```bash
export FINOPS_S3_BUCKET=your-cost-data-bucket
export FINOPS_S3_PREFIX=cur2/data
export FINOPS_DATA_TYPE=CUR2.0
export FINOPS_LOCAL_PATH=./local_data
export FINOPS_TABLE_NAME=CUR
```

### Manual Configuration

```python
from infralyzer import DataConfig, DataExportType

config = DataConfig(
    # Required
    s3_bucket='your-cost-data-bucket',
    s3_data_prefix='cur2/data',
    data_export_type=DataExportType.CUR_2_0,

    # Optional optimization
    local_data_path='./local_data',        # Enable local caching
    prefer_local_data=True,                # Use cache when available

    # Optional filtering
    date_start='2024-01-01',               # Filter data range
    date_end='2024-12-31',
    table_name='CUR',                      # Main table name

    # Optional AWS settings
    aws_region='us-west-2',                # AWS region
    aws_profile='finops',                  # AWS profile name
)
```

## 🎯 Use Cases & Examples

### Cost Analysis Dashboard

```python
# Monthly cost trends
monthly_costs = engine.query("""
    SELECT
        DATE_TRUNC('month', line_item_usage_start_date) as month,
        SUM(line_item_unblended_cost) as total_cost
    FROM CUR
    WHERE line_item_usage_start_date >= '2024-01-01'
    GROUP BY 1
    ORDER BY 1
""")

# Top services by cost
top_services = engine.query("""
    SELECT
        product_servicecode,
        SUM(line_item_unblended_cost) as total_cost,
        COUNT(DISTINCT line_item_resource_id) as resource_count
    FROM CUR
    WHERE line_item_usage_start_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 10
""")
```

### Cost Optimization Analysis

```python
# Idle resources detection
idle_resources = engine.query("""
    SELECT
        line_item_resource_id,
        product_servicecode,
        line_item_unblended_cost,
        line_item_usage_start_date
    FROM CUR
    WHERE line_item_unblended_cost > 0
    AND line_item_usage_amount = 0  -- No usage but incurring cost
    AND line_item_usage_start_date >= CURRENT_DATE - INTERVAL '7 days'
""")

# Rightsizing opportunities
rightsizing = engine.query("cur2_analytics/rightsizing_analysis.sql")
```

### Account & Tag Analysis

```python
# Cost by account
account_costs = engine.query("""
SELECT
        line_item_usage_account_id,
        SUM(line_item_unblended_cost) as account_cost,
        COUNT(DISTINCT product_servicecode) as service_count
FROM CUR
    GROUP BY 1
    ORDER BY 2 DESC
""")

# Tag-based allocation
tag_allocation = engine.query("""
SELECT
        resource_tags_user_environment as environment,
        resource_tags_user_team as team,
        SUM(line_item_unblended_cost) as allocated_cost
    FROM CUR
    WHERE resource_tags_user_environment IS NOT NULL
    GROUP BY 1, 2
""")
```

## 🚀 Advanced Features

### Multi-Engine Performance Comparison

```python
import time

queries = [
    "SELECT COUNT(*) FROM CUR",
    "SELECT product_servicecode, SUM(line_item_unblended_cost) FROM CUR GROUP BY 1",
    "SELECT * FROM CUR LIMIT 10000"
]

engines = ['duckdb', 'polars', 'athena']

for query in queries:
    print(f"Query: {query[:50]}...")
    for engine_name in engines:
        start = time.time()
        result = engine.query(query, engine_name=engine_name)
        duration = time.time() - start
        print(f"  {engine_name}: {duration:.2f}s ({len(result)} rows)")
```

### Custom Analytics Pipelines

```python
# Create a custom analytics pipeline
class CustomCostAnalytics:
    def __init__(self, engine):
        self.engine = engine

    def get_monthly_service_trends(self, months=12):
        """Get monthly trends for top services"""
        sql = f"""
        WITH monthly_costs AS (
        SELECT
            DATE_TRUNC('month', line_item_usage_start_date) as month,
                product_servicecode,
            SUM(line_item_unblended_cost) as monthly_cost
            FROM CUR
            WHERE line_item_usage_start_date >= CURRENT_DATE - INTERVAL '{months} months'
        GROUP BY 1, 2
        ),
        top_services AS (
            SELECT product_servicecode
            FROM CUR
            GROUP BY 1
            ORDER BY SUM(line_item_unblended_cost) DESC
            LIMIT 10
        )
        SELECT mc.*
        FROM monthly_costs mc
        JOIN top_services ts ON mc.product_servicecode = ts.product_servicecode
        ORDER BY mc.month, mc.monthly_cost DESC
        """
        return self.engine.query(sql)

# Use custom analytics
analytics = CustomCostAnalytics(engine)
trends = analytics.get_monthly_service_trends(months=6)
```

## 📚 API Documentation

### Interactive Documentation

When running the FastAPI server, access comprehensive documentation:

- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`
- **Health Check**: `http://localhost:8000/health`

### Query API Reference

```python
# FinOpsEngine Query Methods
engine.query(sql, engine_name="duckdb", format=QueryResultFormat.DATAFRAME, force_s3=False)
engine.query_json(sql, force_s3=False)  # Returns JSON/dict format
engine.query_csv(sql, force_s3=False)   # Returns CSV string
engine.query_arrow(sql, force_s3=False) # Returns Arrow format

# Specialized Analytics
engine.kpi.get_summary(billing_period=None, filters=None)
engine.spend.get_invoice_summary(start_date=None, end_date=None)
engine.optimization.get_idle_resources(threshold_days=7)
engine.allocation.get_cost_allocation_overview(group_by="account")
engine.discounts.get_current_agreements()
engine.ai.get_anomaly_detection(sensitivity="medium")
```

## 🛠️ Development & Testing

### Running Tests

```bash
# Run all tests
python -m pytest tests/

# Run specific test categories
python tests/test_1_query_s3_multi_engine.py      # Multi-engine testing
python tests/test_3_query_local.py                # Local data testing
python tests/test_5_sql_file_execution.py         # SQL file execution
python tests/test_10_fastapi_endpoints.py         # API endpoint testing
```

### Development Setup

```bash
# Install in development mode
pip install -e .

# Start development server with auto-reload
python main.py

# Or with uvicorn
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

## 🏷️ Data Export Support

Infralyzer supports multiple AWS cost data formats:

| Format               | Description                     | Status          |
| -------------------- | ------------------------------- | --------------- |
| **CUR 2.0**          | AWS Cost and Usage Reports v2   | ✅ Full Support |
| **FOCUS 1.0**        | FinOps Open Cost and Usage Spec | ✅ Full Support |
| **COH**              | Cost Optimization Hub           | ✅ Beta Support |
| **Carbon Emissions** | AWS Carbon Footprint Reports    | 🔄 Coming Soon  |

## 📈 Performance & Scaling

### Query Performance Tips

1. **Use Local Caching**: Download data locally for 90%+ faster queries
2. **Choose Right Engine**: DuckDB for analytics, Athena for massive datasets
3. **Filter Early**: Use date ranges and WHERE clauses to reduce data
4. **Optimize SQL**: Use proper indexing and query patterns

### Scaling Considerations

```python
# For large datasets (>1TB)
config = DataConfig(
    s3_bucket='your-bucket',
    s3_data_prefix='cur2/data',
    data_export_type=DataExportType.CUR_2_0,
    # Use Athena for massive datasets
    prefer_athena_for_large_queries=True,
    # Partition filtering
    date_start='2024-01-01',
    date_end='2024-12-31'
)

# Production FastAPI deployment
# uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4 --worker-class uvicorn.workers.UvicornWorker
```

## 🔒 Security & Best Practices

### AWS Authentication

```python
# Recommended: Use IAM roles in production
config = DataConfig(
    s3_bucket='your-bucket',
    s3_data_prefix='cur2/data',
    data_export_type=DataExportType.CUR_2_0,
    # IAM role-based access (recommended)
    role_arn='arn:aws:iam::123456789012:role/FinOpsAnalyticsRole'
)

# Development: Use profiles
config = DataConfig(
    s3_bucket='your-bucket',
    s3_data_prefix='cur2/data',
    data_export_type=DataExportType.CUR_2_0,
    aws_profile='finops-dev'
)
```

### Production Deployment

```python
# production_config.py
import os
from infralyzer import DataConfig, DataExportType

def get_production_config():
    return DataConfig(
        s3_bucket=os.environ['FINOPS_S3_BUCKET'],
        s3_data_prefix=os.environ['FINOPS_S3_PREFIX'],
        data_export_type=DataExportType(os.environ.get('FINOPS_DATA_TYPE', 'CUR2.0')),
        local_data_path=os.environ.get('FINOPS_LOCAL_PATH', './cache'),
        aws_region=os.environ.get('AWS_REGION', 'us-west-2')
    )
```

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details.

### Development Workflow

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙋 Support & Community

- **Documentation**: [GitHub Wiki](https://github.com/jasonwu001t/infralyzer/wiki)
- **Issues**: [GitHub Issues](https://github.com/jasonwu001t/infralyzer/issues)
- **Discussions**: [GitHub Discussions](https://github.com/jasonwu001t/infralyzer/discussions)

---

**Built with ❤️ for the FinOps community**

Transform your AWS cost management with modern analytics, intelligent caching, and production-ready APIs.
