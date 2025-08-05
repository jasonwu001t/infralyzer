"""
Query API endpoints - Simple engine.query() support
===================================================

Supports:
- Regular SQL queries
- SQL file execution (.sql files)
- Direct parquet file reading
- Multiple output formats
- All query engines (DuckDB, Polars, Athena)

query sample:
curl -X POST "http://localhost:8000/api/v1/finops/query" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM CUR LIMIT 1", "engine": "duckdb"}'
"""
from fastapi import APIRouter, Depends, HTTPException, Body
from typing import Dict, Any, List, Optional, Union
from pydantic import BaseModel, Field
import os
import time
import json
from pathlib import Path

from ...finops_engine import FinOpsEngine
from ...engine import QueryResultFormat
from ..dependencies import get_finops_engine

router = APIRouter()


class QueryRequest(BaseModel):
    """Request model supporting SQL, SQL files, and parquet files."""
    
    query: str = Field(
        ..., 
        description="SQL query, path to .sql file, or parquet file query",
        min_length=1,
        max_length=50000,
        examples=[
            "SELECT * FROM CUR LIMIT 10",
            "cur2_analytics/monthly_summary.sql", 
            "SELECT * FROM 'data/costs.parquet' WHERE cost > 100"
        ]
    )
    
    engine: Optional[str] = Field(
        "duckdb", 
        description="Query engine: 'duckdb' (default), 'polars', or 'athena'",
        regex="^(duckdb|polars|athena)$"
    )
    
    output_format: Optional[str] = Field(
        "json", 
        description="Output format: 'json' (default), 'csv', 'dataframe_json'",
        regex="^(json|csv|dataframe_json)$"
    )
    
    limit: Optional[int] = Field(
        None, 
        description="Row limit (applied automatically for safety)",
        ge=1,
        le=50000
    )
    
    force_s3: Optional[bool] = Field(
        False, 
        description="Force S3 data access even if local data available"
    )


class QueryResponse(BaseModel):
    """Response model with query results and metadata."""
    
    success: bool
    query_type: str  # "sql_query", "sql_file", "parquet_file"
    data: Union[List[Dict[str, Any]], str]
    metadata: Dict[str, Any]
    row_count: int
    column_count: int
    execution_time_ms: float


class QueryError(BaseModel):
    """Error response model."""
    success: bool = False
    error: str
    error_type: str
    metadata: Dict[str, Any]


def _detect_query_type(query: str) -> tuple[str, str]:
    """
    Detect the type of query and return (query_type, processed_query).
    
    Returns:
        - ("sql_query", query) for regular SQL
        - ("sql_file", file_path) for .sql files  
        - ("parquet_file", query) for parquet file queries
    """
    query_stripped = query.strip()
    
    # Check if it's a SQL file path
    if query_stripped.endswith('.sql') and not query_stripped.upper().startswith('SELECT'):
        if os.path.exists(query_stripped):
            return ("sql_file", query_stripped)
        # Check in common SQL directories
        for sql_dir in ["cur2_analytics", "cur2_query_library", "sql"]:
            full_path = os.path.join(sql_dir, query_stripped)
            if os.path.exists(full_path):
                return ("sql_file", full_path)
        # If file not found, treat as regular SQL (will error later)
        return ("sql_query", query_stripped)
    
    # Check if it's a parquet file query
    if '.parquet' in query_stripped.lower():
        return ("parquet_file", query_stripped)
    
    # Default: regular SQL query
    return ("sql_query", query_stripped)


def _apply_safety_limit(query: str, limit: Optional[int]) -> str:
    """Apply safety limit to query if needed."""
    if not limit:
        return query
        
    query_upper = query.upper()
    if 'LIMIT' not in query_upper:
        # Add limit for safety
        if query.strip().endswith(';'):
            query = query.strip()[:-1]  # Remove trailing semicolon
        return f"{query} LIMIT {limit}"
    
    return query


def _get_output_format(format_str: str) -> QueryResultFormat:
    """Convert string format to QueryResultFormat enum."""
    format_map = {
        "json": QueryResultFormat.RECORDS,
        "csv": QueryResultFormat.CSV,
        "dataframe_json": QueryResultFormat.RECORDS  # Will convert DataFrame to JSON
    }
    return format_map.get(format_str.lower(), QueryResultFormat.RECORDS)


def _clean_json_data(data):
    """Clean data to ensure JSON compliance by handling NaN and infinite values."""
    import math
    
    def clean_value(value):
        """Clean individual value for JSON compliance."""
        if isinstance(value, float):
            if math.isnan(value):
                return None  # Convert NaN to null
            elif math.isinf(value):
                return None  # Convert infinity to null
        return value
    
    def clean_record(record):
        """Clean a single record (dictionary)."""
        if isinstance(record, dict):
            return {key: clean_value(value) for key, value in record.items()}
        return record
    
    if isinstance(data, list):
        return [clean_record(record) for record in data]
    elif isinstance(data, dict):
        return clean_record(data)
    else:
        return data


@router.post("/query", response_model=QueryResponse)
async def execute_query(
    request: QueryRequest,
    finops_engine: FinOpsEngine = Depends(get_finops_engine)
):
    """
    Execute SQL queries, SQL files, or query parquet files directly.
    
    **Query Types:**
    - SQL queries: `SELECT * FROM CUR LIMIT 10`
    - SQL files: `cur2_analytics/monthly_summary.sql`
    - Parquet files: `SELECT * FROM 'data/costs.parquet'`
    
    **Engines:** duckdb (default), polars, athena
    **Formats:** json (default), csv
    """
    start_time = time.time()
    
    try:
        # Detect query type and process accordingly
        query_type, processed_query = _detect_query_type(request.query)
        
        # Apply safety limit for regular queries
        if query_type == "sql_query" and request.limit:
            processed_query = _apply_safety_limit(processed_query, request.limit)
        
        # Execute query using FinOpsEngine
        if request.output_format == "json":
            # Use convenient JSON method
            result = finops_engine.query_json(processed_query, force_s3=request.force_s3)
            # Clean NaN and infinite values for JSON compliance
            output_data = _clean_json_data(result)
        elif request.output_format == "csv":
            # Use convenient CSV method
            result = finops_engine.query_csv(processed_query, force_s3=request.force_s3)
            output_data = result
        else:
            # Default to DataFrame then convert
            result = finops_engine.query(processed_query, force_s3=request.force_s3)
            if hasattr(result, 'to_dict'):
                output_data = _clean_json_data(result.to_dict('records'))
            else:
                output_data = _clean_json_data(result)
        
        # Calculate metrics
        execution_time = (time.time() - start_time) * 1000
        
        if request.output_format == "csv":
            row_count = len(result.split('\n')) - 1 if result else 0
            column_count = len(result.split('\n')[0].split(',')) if result else 0
        else:
            row_count = len(output_data) if isinstance(output_data, list) else 1
            column_count = len(output_data[0].keys()) if output_data and isinstance(output_data, list) else 0
        
        # Prepare metadata
        metadata = {
            "query_type": query_type,
            "original_query": request.query,
            "processed_query": processed_query if query_type != "sql_file" else f"<contents of {processed_query}>",
            "engine_used": finops_engine.engine_name,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
            "data_source": "local" if finops_engine.config.prefer_local_data else "s3",
            "execution_time_ms": execution_time,
            "output_format": request.output_format,
            "safety_limit_applied": request.limit is not None,
            "engine_capabilities": {
                "supports_s3": finops_engine.engine.supports_s3_direct,
                "supports_local": finops_engine.engine.supports_local_data,
                "has_local_data": finops_engine.engine.has_local_data()
            }
        }
        
        return QueryResponse(
            success=True,
            query_type=query_type,
            data=output_data,
            metadata=metadata,
            row_count=row_count,
            column_count=column_count,
            execution_time_ms=execution_time
        )
        
    except FileNotFoundError as e:
        return QueryError(
            error=f"SQL file not found: {str(e)}",
            error_type="FILE_NOT_FOUND",
            metadata={
                "query_type": query_type,
                "searched_paths": ["cur2_analytics/", "cur2_query_library/", "sql/"],
                "suggestion": "Check file path or place SQL files in cur2_analytics/ directory"
            }
        )
        
    except Exception as e:
        execution_time = (time.time() - start_time) * 1000
        error_str = str(e).lower()
        
        # Classify error types
        if "column" in error_str and "not found" in error_str:
            error_type = "COLUMN_NOT_FOUND"
        elif "table" in error_str and ("not found" in error_str or "does not exist" in error_str):
            error_type = "TABLE_NOT_FOUND"
        elif "syntax" in error_str or "parse" in error_str:
            error_type = "SQL_SYNTAX_ERROR"
        elif "permission" in error_str or "access" in error_str:
            error_type = "ACCESS_DENIED"
        else:
            error_type = "EXECUTION_ERROR"
        
        return QueryError(
            error=str(e),
            error_type=error_type,
            metadata={
                "query_type": query_type,
                "execution_time_ms": execution_time,
                "engine_used": request.engine,
                "suggestion": _get_error_suggestion(error_type)
            }
        )




def _get_error_suggestion(error_type: str) -> str:
    """Get helpful suggestion based on error type."""
    suggestions = {
        "COLUMN_NOT_FOUND": "Check column names with: SELECT * FROM CUR LIMIT 1",
        "TABLE_NOT_FOUND": "Available tables: CUR (main data), aws_pricing (pricing data), aws_savings_plans (savings plans)",
        "SQL_SYNTAX_ERROR": "Verify SQL syntax. Use simple SELECT statements for testing.",
        "ACCESS_DENIED": "Check AWS credentials and S3 bucket permissions.",
        "FILE_NOT_FOUND": "Ensure SQL file exists in cur2_analytics/ directory or provide full path.",
        "EXECUTION_ERROR": "Try a simpler query to isolate the issue."
    }
    return suggestions.get(error_type, "Check query syntax and try again.")