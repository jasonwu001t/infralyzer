# FinOps Test Suite

This directory contains 4 independent test files to validate each core function of the de-polars FinOps system.

## Test Files

### 🔍 Test 1: Query S3 Parquet Files

**File:** `test_1_query_s3.py`

- **Purpose:** Query S3 parquet files directly without downloading
- **Requirements:** AWS credentials, S3 access
- **Run:** `python test_1_query_s3.py`

### 💾 Test 2: Download Data Locally

**File:** `test_2_download_local.py`

- **Purpose:** Download S3 data to local storage for faster access
- **Requirements:** AWS credentials, S3 access, local disk space
- **Output:** Creates `./test_local_data/` directory with parquet files
- **Run:** `python test_2_download_local.py`

### ⚡ Test 3: Query Local Parquet Files

**File:** `test_3_query_local.py`

- **Purpose:** Query local parquet files for fast analytics
- **Requirements:** Local data (run Test 2 first)
- **Run:** `python test_3_query_local.py`

### 🌐 Test 4: FastAPI Endpoints

**File:** `test_4_fastapi_endpoints.py`

- **Purpose:** Create REST API endpoints serving data from local parquet files
- **Requirements:** Local data (run Test 2 first)
- **Output:** HTTP server at `http://localhost:8000`
- **Run:** `python test_4_fastapi_endpoints.py`

## Quick Start

1. **Run all tests in sequence:**

   ```bash
   python run_all_tests.py
   ```

2. **Run individual tests:**
   ```bash
   cd tests
   python test_1_query_s3.py          # Test S3 access
   python test_2_download_local.py    # Download data
   python test_3_query_local.py       # Test local queries
   python test_4_fastapi_endpoints.py # Start API server
   python test_5_sql_views.py         # Test SQL view dependencies
   python test_6_mcp_server.py        # Test MCP integration
   ```

## Test Configuration

All tests use the same S3 configuration:

- **Bucket:** `billing-data-exports-cur`
- **Prefix:** `cur2/cur2/data`
- **Export Type:** `CUR2.0`
- **Date Range:** `2025-07`

## API Endpoints (Test 4)

When running Test 4, the following endpoints become available:

- `GET /` - API information
- `GET /summary` - Overall cost summary
- `GET /services?limit=10` - Top services by cost
- `GET /daily-costs?limit=30` - Daily cost breakdown
- `GET /search-service/{service_name}` - Search specific service

**API Documentation:** http://localhost:8000/docs

## MCP Integration (Test 6)

Test 6 demonstrates AI assistant integration through Model Context Protocol:

- **Resources:** 5 cost data resources available
- **Tools:** 5 analysis tools for AI assistants
- **Queries:** Natural language cost analysis
- **Streaming:** Real-time cost alerts and events
- **Output:** Standard MCP response format

## Prerequisites

- AWS credentials configured (via AWS CLI, environment variables, or IAM role)
- Python packages: `polars`, `duckdb`, `fastapi`, `uvicorn`
- S3 bucket access permissions

## Notes

- Tests 1 and 2 require AWS/S3 access
- Tests 3, 4, 5, and 6 work offline with local data
- Test 2 must be run before Tests 3, 4, 5, and 6
- Each test is independent and focuses on a specific function
- Local data is stored in `./test_local_data/` directory
- Test 5 creates additional view outputs in `./test_views_output/`
- Test 6 generates MCP response JSON for AI assistant integration
