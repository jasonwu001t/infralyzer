"""
SQL Query API endpoints - Custom SQL execution with pluggable query engines
"""
from fastapi import APIRouter, Depends, HTTPException, Body
from typing import Dict, Any, List, Optional, Union
from pydantic import BaseModel, Field
import re
import json
import time
import pandas as pd

from ...finops_engine import FinOpsEngine
from ...engine import QueryEngineFactory, QueryResultFormat


router = APIRouter()


class SQLQueryRequest(BaseModel):
    """Request model for SQL query execution with engine selection."""
    sql: str = Field(..., description="SQL query to execute", min_length=1, max_length=10000)
    limit: Optional[int] = Field(1000, description="Maximum number of rows to return (default: 1000, max: 10000)", le=10000, ge=1)
    force_s3: Optional[bool] = Field(False, description="Force querying from S3 even if local data is available")
    format: Optional[str] = Field("json", description="Output format: 'json' (default) or 'csv'")
    engine: Optional[str] = Field("duckdb", description="Query engine: 'duckdb' (default), 'polars', or 'athena'")


class SQLQueryResponse(BaseModel):
    """Response model for SQL query execution."""
    success: bool
    query_metadata: Dict[str, Any]
    data: Union[List[Dict[str, Any]], str]  # JSON data or CSV string
    schema: Dict[str, str]
    row_count: int
    execution_time_ms: float


class SQLQueryError(BaseModel):
    """Error response model for SQL query execution."""
    success: bool = False
    error: str
    error_type: str
    query_metadata: Dict[str, Any]


def _validate_sql_query(sql: str) -> bool:
    """
    Basic SQL validation to prevent dangerous operations.
    Returns True if query is safe, raises HTTPException if not.
    """
    sql_upper = sql.upper().strip()
    
    # Block dangerous operations
    dangerous_keywords = [
        'DROP', 'DELETE', 'UPDATE', 'INSERT', 'CREATE', 'ALTER', 
        'TRUNCATE', 'REPLACE', 'MERGE', 'EXEC', 'EXECUTE',
        'CALL', 'DECLARE', 'SET'
    ]
    
    for keyword in dangerous_keywords:
        if f' {keyword} ' in f' {sql_upper} ' or sql_upper.startswith(f'{keyword} '):
            raise HTTPException(
                status_code=400,
                detail=f"SQL operation '{keyword}' is not allowed. Only SELECT queries are permitted."
            )
    
    # Ensure it's a SELECT query
    if not sql_upper.startswith('SELECT') and not sql_upper.startswith('WITH'):
        raise HTTPException(
            status_code=400,
            detail="Only SELECT queries and CTEs (WITH clauses) are allowed."
        )
    
    return True


def _get_table_suggestions(engine) -> List[str]:
    """Get available table names for query suggestions."""
    try:
        available_tables = [engine.config.table_name]
        
        # Add API data tables if available
        try:
            # These are the standard API tables that might be available
            api_tables = ['aws_pricing', 'aws_savings_plans']
            available_tables.extend(api_tables)
        except:
            pass
            
        return available_tables
    except Exception:
        return []


def _detect_schema_from_result(data: Union[List[Dict], pd.DataFrame]) -> Dict[str, str]:
    """Detect schema from query result data."""
    if isinstance(data, pd.DataFrame):
        return {col: str(dtype) for col, dtype in zip(data.columns, data.dtypes)}
    elif isinstance(data, list) and data:
        # Infer from first record
        first_record = data[0]
        schema = {}
        for key, value in first_record.items():
            if isinstance(value, int):
                schema[key] = "int64"
            elif isinstance(value, float):
                schema[key] = "float64"
            elif isinstance(value, bool):
                schema[key] = "bool"
            else:
                schema[key] = "object"
        return schema
    else:
        return {}


@router.post("/sql/query", response_model=SQLQueryResponse)
async def execute_sql_query(
    query_request: SQLQueryRequest,
    finops_engine: FinOpsEngine = Depends()
):
    """
    Execute custom SQL query with pluggable query engines.
    
    **Supported Engines:**
    - **duckdb** (default): Fast analytics engine with S3 direct access
    - **polars**: Modern DataFrame library with SQL support  
    - **athena**: AWS managed serverless query service
    
    **Query Features:**
    - Full SQL support (SELECT, JOINs, CTEs, window functions)
    - Automatic data source optimization (local vs S3)
    - Multiple output formats (JSON, CSV)
    - Built-in safety validation
    
    **Example Queries:**
    ```sql
    -- Top services by cost
    SELECT product_servicecode, SUM(line_item_unblended_cost) as total_cost
    FROM CUR 
    WHERE bill_billing_period_start_date >= '2024-01-01'
    GROUP BY product_servicecode 
    ORDER BY total_cost DESC 
    LIMIT 10;
    
    -- Cost trend analysis
    WITH daily_costs AS (
      SELECT DATE(line_item_usage_start_date) as usage_date,
             SUM(line_item_unblended_cost) as daily_cost
      FROM CUR 
      GROUP BY DATE(line_item_usage_start_date)
    )
    SELECT usage_date, daily_cost,
           AVG(daily_cost) OVER (ORDER BY usage_date ROWS 6 PRECEDING) as rolling_avg
    FROM daily_costs 
    ORDER BY usage_date;
    ```
    """
    import time
    start_time = time.time()
    
    try:
        # Validate the SQL query
        _validate_sql_query(query_request.sql)
        
        # Add LIMIT clause if not present and limit is specified
        sql = query_request.sql.strip()
        if query_request.limit and not re.search(r'\bLIMIT\b', sql.upper()):
            sql = f"{sql} LIMIT {query_request.limit}"
        
        # Create the specified query engine
        try:
            query_engine = QueryEngineFactory.create_engine(
                query_request.engine.lower(), 
                finops_engine.config
            )
        except ValueError as e:
            available_engines = QueryEngineFactory.list_engines()
            raise HTTPException(
                status_code=400,
                detail=f"Invalid engine '{query_request.engine}'. Available engines: {', '.join(available_engines)}"
            )
        
        # Determine output format
        if query_request.format.lower() == "csv":
            result_format = QueryResultFormat.CSV
        else:
            result_format = QueryResultFormat.RECORDS
        
        # Execute the query with the selected engine
        result_data = query_engine.query(sql, format=result_format, force_s3=query_request.force_s3)
        
        # Calculate execution time
        execution_time = (time.time() - start_time) * 1000  # Convert to milliseconds
        
        # Handle different result formats
        if query_request.format.lower() == "csv":
            data = result_data  # Already CSV string
            schema = {}  # Schema not available for CSV
            row_count = len(result_data.split('\n')) - 2 if result_data else 0  # Rough estimate
        else:
            data = result_data  # Already list of dicts
            schema = _detect_schema_from_result(data)
            row_count = len(data) if data else 0
        
        # Prepare metadata
        table_suggestions = _get_table_suggestions(query_engine)
        query_metadata = {
            "query_timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
            "query_engine": query_engine.engine_name,
            "data_source": "local_parquet" if query_engine.config.prefer_local_data and query_engine.has_local_data() else "s3_parquet",
            "data_export_type": query_engine.config.data_export_type.value,
            "table_name": query_engine.config.table_name,
            "available_tables": table_suggestions,
            "query_length": len(sql),
            "execution_time_ms": execution_time,
            "supports_s3_direct": query_engine.supports_s3_direct,
            "supports_local_data": query_engine.supports_local_data,
            "has_local_data": query_engine.has_local_data()
        }
        
        return SQLQueryResponse(
            success=True,
            query_metadata=query_metadata,
            data=data,
            schema=schema,
            row_count=row_count,
            execution_time_ms=execution_time
        )
        
    except HTTPException:
        raise  # Re-raise HTTP exceptions as-is
        
    except Exception as e:
        execution_time = (time.time() - start_time) * 1000
        
        # Classify error type
        error_str = str(e).lower()
        if "column" in error_str and ("not found" in error_str or "does not exist" in error_str):
            error_type = "COLUMN_NOT_FOUND"
        elif "table" in error_str and ("not found" in error_str or "does not exist" in error_str):
            error_type = "TABLE_NOT_FOUND"
        elif "syntax" in error_str or "parse" in error_str:
            error_type = "SYNTAX_ERROR"
        elif "permission" in error_str or "access" in error_str:
            error_type = "ACCESS_ERROR"
        else:
            error_type = "EXECUTION_ERROR"
        
        # Create error metadata
        try:
            query_engine = QueryEngineFactory.create_engine(
                query_request.engine.lower(), 
                finops_engine.config
            )
            table_suggestions = _get_table_suggestions(query_engine)
        except:
            table_suggestions = []
        
        query_metadata = {
            "query_timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
            "query_engine": query_request.engine,
            "table_name": finops_engine.config.table_name,
            "available_tables": table_suggestions,
            "query_length": len(query_request.sql),
            "execution_time_ms": execution_time
        }
        
        raise HTTPException(
            status_code=500,
            detail=SQLQueryError(
                error=str(e),
                error_type=error_type,
                query_metadata=query_metadata
            ).dict()
        )