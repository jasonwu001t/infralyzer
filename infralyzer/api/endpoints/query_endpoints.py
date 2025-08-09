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
from ...utils.focus_schema import FocusSchema
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
            "SELECT * FROM FOCUS LIMIT 10",
            "cur2_analytics/monthly_summary.sql", 
            "SELECT * FROM 'data/costs.parquet' WHERE cost > 100"
        ]
    )
    
    engine: Optional[str] = Field(
        "duckdb", 
        description="Query engine: 'duckdb' (default), 'polars', or 'athena'",
        pattern="^(duckdb|polars|athena)$"
    )
    
    output_format: Optional[str] = Field(
        "json", 
        description="Output format: 'json' (default), 'csv', 'dataframe_json'",
        pattern="^(json|csv|dataframe_json)$"
    )
    
    table_name: Optional[str] = Field(
        None, 
        description="Target table name: 'CUR' (AWS Cost and Usage Report 2.0), 'FOCUS' (FinOps Open Cost and Usage Specification), or custom table name",
        examples=["CUR", "FOCUS", "cost_data"]
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


def _replace_table_placeholder(query: str, table_name: str) -> str:
    """Replace table placeholders in SQL query with actual table name."""
    if not table_name:
        return query
    
    import re
    
    # Replace standalone CUR or FOCUS with the specified table name
    # This handles cases like: SELECT * FROM CUR -> SELECT * FROM actual_table_name
    patterns = [
        # Match standalone CUR or FOCUS (word boundaries)
        (r'\bCUR\b', table_name),
        (r'\bFOCUS\b', table_name),
    ]
    
    result_query = query
    for pattern, replacement in patterns:
        result_query = re.sub(pattern, replacement, result_query, flags=re.IGNORECASE)
    
    return result_query


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
        
        # Replace table placeholders if table_name is specified
        if request.table_name:
            processed_query = _replace_table_placeholder(processed_query, request.table_name)
        
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
        original_error = str(e)
        
        # Classify error types and provide user-friendly messages
        if "column" in error_str and "not found" in error_str:
            error_type = "COLUMN_NOT_FOUND"
            user_friendly_error, helpful_suggestions = _get_column_error_guidance(original_error, request.query)
        elif "table" in error_str and ("not found" in error_str or "does not exist" in error_str):
            error_type = "TABLE_NOT_FOUND"
            user_friendly_error, helpful_suggestions = _get_table_error_guidance(original_error, request.query)
        elif "syntax" in error_str or "parse" in error_str:
            error_type = "SQL_SYNTAX_ERROR"
            user_friendly_error, helpful_suggestions = _get_syntax_error_guidance(original_error, request.query)
        elif "permission" in error_str or "access" in error_str:
            error_type = "ACCESS_DENIED"
            user_friendly_error, helpful_suggestions = _get_access_error_guidance(original_error, request.query)
        else:
            error_type = "EXECUTION_ERROR"
            user_friendly_error, helpful_suggestions = _get_general_error_guidance(original_error, request.query)
        
        # Return user-friendly error response instead of HTTP 500
        raise HTTPException(
            status_code=400,  # Bad Request instead of 500 Internal Server Error
            detail={
                "error": user_friendly_error,
                "error_type": error_type,
                "suggestions": helpful_suggestions,
                "metadata": {
                    "query_type": query_type,
                    "execution_time_ms": execution_time,
                    "engine_used": request.engine,
                    "original_query": request.query[:200] + "..." if len(request.query) > 200 else request.query,
                    "troubleshooting_steps": _get_troubleshooting_steps(error_type)
                }
            }
        )




def _get_column_error_guidance(original_error: str, query: str) -> tuple[str, list[str]]:
    """Get user-friendly guidance for column not found errors."""
    
    # Extract the relevant part of DuckDB error message
    import re
    
    # Extract the core error message (remove "DuckDB query error:" prefix)
    clean_error = re.sub(r'^.*?DuckDB query error:\s*', '', original_error, flags=re.IGNORECASE)
    
    # Extract column name from error if possible
    column_match = re.search(r'column "([^"]+)" not found', original_error, re.IGNORECASE)
    missing_column = column_match.group(1) if column_match else "unknown column"
    
    # Look for candidate columns from error message
    candidates_match = re.search(r'candidate bindings:?\s*(.+)', original_error, re.IGNORECASE)
    candidates = []
    if candidates_match:
        candidates_text = candidates_match.group(1)
        # Extract quoted column names
        candidates = re.findall(r'"([^"]+)"', candidates_text)
    
    # Use the actual DuckDB error as the primary message
    user_friendly_error = clean_error.strip()
    
    suggestions = [
        f"🎯 Column '{missing_column}' does not exist in the CUR table",
        "🔍 Run 'SELECT * FROM CUR LIMIT 1' to see all available columns"
    ]
    
    if candidates:
        suggestions.append(f"💡 Available similar columns: {', '.join(candidates[:5])}")
    
    suggestions.extend([
        "📊 Check if you're using correct CUR 2.0 column names (not legacy CUR 1.0)",
        "📋 Use /bedrock/finops-expert for help with CUR column selection"
    ])
    
    return user_friendly_error, suggestions


def _get_table_error_guidance(original_error: str, query: str) -> tuple[str, list[str]]:
    """Get user-friendly guidance for table not found errors."""
    
    import re
    
    # Extract the core error message (remove "DuckDB query error:" prefix)
    clean_error = re.sub(r'^.*?DuckDB query error:\s*', '', original_error, flags=re.IGNORECASE)
    user_friendly_error = clean_error.strip()
    
    suggestions = [
        "✅ Use 'CUR' as the main table name for Cost and Usage Report data",
        "📋 Available tables: CUR (main cost data), aws_pricing (pricing data), aws_savings_plans (savings plans)",
        "🔄 Check if your data has been properly loaded and configured",
        "📁 Verify your data source configuration (local vs S3)",
        "🔍 Try a simple query first: 'SELECT * FROM CUR LIMIT 1'"
    ]
    
    return user_friendly_error, suggestions


def _get_syntax_error_guidance(original_error: str, query: str) -> tuple[str, list[str]]:
    """Get user-friendly guidance for SQL syntax errors."""
    
    import re
    
    # Extract the core error message (remove "DuckDB query error:" prefix)
    clean_error = re.sub(r'^.*?DuckDB query error:\s*', '', original_error, flags=re.IGNORECASE)
    user_friendly_error = clean_error.strip()
    
    suggestions = [
        "📝 Check for missing commas, quotation marks, or parentheses",
        "🔤 Verify proper SQL keyword spelling (SELECT, FROM, WHERE, etc.)",
        "⚠️ Ensure string values are properly quoted with single quotes ('value')",
        "🔍 Test with a simple query first: 'SELECT * FROM CUR LIMIT 1'",
        "🎯 Use /bedrock/generate-query endpoint to get AI-generated correct SQL"
    ]
    
    return user_friendly_error, suggestions


def _get_access_error_guidance(original_error: str, query: str) -> tuple[str, list[str]]:
    """Get user-friendly guidance for access/permission errors."""
    
    import re
    
    # Extract the core error message (remove "DuckDB query error:" prefix if present)
    clean_error = re.sub(r'^.*?DuckDB query error:\s*', '', original_error, flags=re.IGNORECASE)
    user_friendly_error = clean_error.strip()
    
    suggestions = [
        "🔐 Check your AWS credentials configuration",
        "🪣 Verify you have read access to the S3 bucket containing CUR data",
        "⚙️ Ensure your data source configuration is correct",
        "🔄 Try switching to local data if available (set force_s3=false)",
        "📞 Contact your administrator to verify your permissions",
        "🛠️ Check the Infralyzer configuration for data source settings"
    ]
    
    return user_friendly_error, suggestions


def _get_general_error_guidance(original_error: str, query: str) -> tuple[str, list[str]]:
    """Get user-friendly guidance for general execution errors."""
    
    import re
    
    # Extract the core error message (remove "DuckDB query error:" prefix if present)
    clean_error = re.sub(r'^.*?DuckDB query error:\s*', '', original_error, flags=re.IGNORECASE)
    user_friendly_error = clean_error.strip()
    
    suggestions = [
        "📝 Double-check your query syntax and column names",
        "📊 Verify that your data is properly loaded and accessible",
        "🔍 Try a simpler query to test: 'SELECT COUNT(*) FROM CUR'",
        "📅 Check if you're querying data for time periods that exist in your dataset",
        "💰 Ensure you're using correct filter conditions (dates, service names, etc.)",
        "🚀 Use /bedrock/finops-expert for query assistance"
    ]
    
    return user_friendly_error, suggestions


def _get_troubleshooting_steps(error_type: str) -> list[str]:
    """Get specific troubleshooting steps based on error type."""
    
    steps = {
        "COLUMN_NOT_FOUND": [
            "1. Run 'SELECT * FROM CUR LIMIT 1' to see available columns",
            "2. Check CUR 2.0 schema documentation for correct column names",
            "3. Verify you're not using legacy CUR 1.0 column names",
            "4. Use case-sensitive column names exactly as they appear"
        ],
        "TABLE_NOT_FOUND": [
            "1. Verify data source configuration (local vs S3)",
            "2. Check if CUR data has been loaded properly",
            "3. Test with 'SELECT 1' to verify basic engine functionality",
            "4. Contact administrator if data access issues persist"
        ],
        "SQL_SYNTAX_ERROR": [
            "1. Copy your query to a SQL editor for syntax highlighting",
            "2. Break down complex queries into smaller parts",
            "3. Test each part of the query individually",
            "4. Use the AI query generator for help with complex queries"
        ],
        "ACCESS_DENIED": [
            "1. Verify AWS credentials are configured correctly",
            "2. Check S3 bucket permissions for CUR data",
            "3. Test with local data if available",
            "4. Contact your cloud administrator for permission review"
        ],
        "EXECUTION_ERROR": [
            "1. Start with a simple 'SELECT COUNT(*) FROM CUR' query",
            "2. Add filters and columns gradually to isolate the issue",
            "3. Check date ranges and filter values for validity",
            "4. Verify data exists for the time period you're querying"
        ]
    }
    
    return steps.get(error_type, [
        "1. Review your query for any obvious errors",
        "2. Test with a simpler version of your query",
        "3. Check data availability and configuration",
        "4. Contact support if the issue persists"
    ])


@router.get("/schema/{table_name}")
async def get_table_schema(
    table_name: str,
    finops_engine: FinOpsEngine = Depends(get_finops_engine)
):
    """
    Get schema information for a specific table.
    
    **Supported Tables:**
    - `CUR`: AWS Cost and Usage Report 2.0 schema
    - `FOCUS`: FinOps Open Cost and Usage Specification schema
    
    **Returns:** Column groups, column details, and sample queries
    """
    table_name = table_name.upper()
    
    if table_name == "FOCUS":
        return {
            "success": True,
            "table_name": "FOCUS",
            "table_type": "FinOps Open Cost and Usage Specification",
            "version": "v1.0",
            "column_groups": FocusSchema.get_column_groups_for_frontend(),
            "total_columns": len(FocusSchema.get_all_columns()),
            "schema": FocusSchema.get_schema_dict(),
            "sample_queries": FocusSchema.get_sample_queries(),
            "metadata": {
                "specification": "https://focus.finops.org",
                "supported_versions": ["v1.0", "v1.1", "v1.2"],
                "description": "Cloud vendor-neutral specification for cloud billing data"
            }
        }
    elif table_name == "CUR":
        # Get CUR schema from the engine
        try:
            cur_schema = finops_engine.schema()
            # This would return the existing CUR column structure
            # For now, return basic info pointing to existing implementation
            return {
                "success": True,
                "table_name": "CUR",
                "table_type": "AWS Cost and Usage Report 2.0",
                "version": "2.0",
                "total_columns": len(cur_schema),
                "schema": cur_schema,
                "metadata": {
                    "specification": "https://docs.aws.amazon.com/cur/",
                    "description": "AWS Cost and Usage Report with detailed billing information",
                    "note": "Use existing CUR column browser in SQL Lab for detailed column groups"
                }
            }
        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to retrieve CUR schema: {str(e)}",
                "table_name": table_name
            }
    else:
        return {
            "success": False,
            "error": f"Unsupported table: {table_name}. Supported tables: CUR, FOCUS",
            "supported_tables": ["CUR", "FOCUS"]
        }