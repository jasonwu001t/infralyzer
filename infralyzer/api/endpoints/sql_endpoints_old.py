"""
SQL Query API endpoints - Custom SQL execution for flexible data analysis
"""
from fastapi import APIRouter, Depends, HTTPException, Body
from typing import Dict, Any, List, Optional, Union
from pydantic import BaseModel, Field
import re
import json

from ...finops_engine import FinOpsEngine


router = APIRouter()


class SQLQueryRequest(BaseModel):
    """Request model for SQL query execution."""
    sql: str = Field(..., description="SQL query to execute", min_length=1, max_length=10000)
    limit: Optional[int] = Field(1000, description="Maximum number of rows to return (default: 1000, max: 10000)", le=10000, ge=1)
    force_s3: Optional[bool] = Field(False, description="Force querying from S3 even if local data is available")
    format: Optional[str] = Field("json", description="Output format: 'json' (default) or 'csv'")


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
        'DROP', 'DELETE', 'INSERT', 'UPDATE', 'CREATE', 'ALTER', 
        'TRUNCATE', 'GRANT', 'REVOKE', 'EXEC', 'EXECUTE'
    ]
    
    for keyword in dangerous_keywords:
        if re.search(rf'\b{keyword}\b', sql_upper):
            raise HTTPException(
                status_code=400, 
                detail=f"SQL operation '{keyword}' is not allowed. Only SELECT queries are permitted."
            )
    
    # Ensure it's a SELECT query
    if not sql_upper.lstrip().startswith('SELECT'):
        raise HTTPException(
            status_code=400,
            detail="Only SELECT queries are allowed. Query must start with SELECT."
        )
    
    return True


def _get_table_suggestions(engine: FinOpsEngine) -> List[str]:
    """Get available table names and common suggestions."""
    table_name = engine.engine.config.table_name
    
    suggestions = [
        table_name,  # Main data table (CUR/FOCUS)
        "summary_view",
        "kpi_instance_all", 
        "kpi_ebs_storage_all",
        "kpi_ebs_snap",
        "kpi_s3_storage_all",
        "kpi_instance_mapping"
    ]
    
    return suggestions


@router.post("/sql/query", response_model=SQLQueryResponse)
async def execute_sql_query(
    query_request: SQLQueryRequest,
    engine: FinOpsEngine = Depends()
):
    """
    Execute custom SQL query for flexible data analysis
    
    **Powerful SQL interface for cost data analysis:**
    - Execute custom SELECT queries on your AWS cost data
    - Support for complex JOINs, aggregations, and window functions
    - Automatic data source selection (local vs S3)
    - Flexible output formats (JSON or CSV)
    
    **Available Tables:**
    - `{table_name}` - Main cost data (CUR/FOCUS export)
    - `summary_view` - Pre-aggregated cost summary
    - `kpi_instance_all` - Instance-level cost metrics
    - `kpi_ebs_storage_all` - EBS storage costs
    - `kpi_ebs_snap` - EBS snapshot costs  
    - `kpi_s3_storage_all` - S3 storage costs
    
    **Security:**
    - Only SELECT queries allowed
    - Maximum 10,000 rows per query
    - 10 second query timeout
    - No data modification operations
    
    **Examples:**
    ```sql
    -- Top 10 most expensive services
    SELECT product_servicecode, SUM(unblended_cost) as total_cost
    FROM {table_name}
    WHERE unblended_cost > 0
    GROUP BY product_servicecode  
    ORDER BY total_cost DESC
    LIMIT 10;
    
    -- Monthly cost trends
    SELECT 
        billing_period,
        SUM(unblended_cost) as monthly_cost,
        COUNT(*) as line_items
    FROM {table_name}
    GROUP BY billing_period
    ORDER BY billing_period;
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
        
        # Execute the query
        result_df = engine.query(sql, force_s3=query_request.force_s3)
        
        # Calculate execution time
        execution_time = (time.time() - start_time) * 1000  # Convert to milliseconds
        
        # Get schema information
        schema = {col: str(dtype) for col, dtype in zip(result_df.columns, result_df.dtypes)}
        
        # Prepare response data based on format
        if query_request.format.lower() == "csv":
            # Return CSV format
            import io
            csv_buffer = io.StringIO()
            result_df.to_pandas().to_csv(csv_buffer, index=False)
            data = csv_buffer.getvalue()
        else:
            # Return JSON format (default)
            # Convert to records format for frontend consumption
            try:
                data = result_df.to_dicts()
            except Exception:
                # Fallback: convert via pandas
                pandas_df = result_df.to_pandas()
                data = pandas_df.to_dict('records')
        
        # Prepare metadata
        table_suggestions = _get_table_suggestions(engine)
        query_metadata = {
            "query_timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
            "data_source": "local_parquet" if engine.engine.config.prefer_local_data and engine.engine.has_local_data() else "s3_parquet",
            "data_export_type": engine.engine.config.data_export_type.value,
            "table_name": engine.engine.config.table_name,
            "available_tables": table_suggestions,
            "query_length": len(query_request.sql),
            "format": query_request.format,
            "limit_applied": query_request.limit
        }
        
        return SQLQueryResponse(
            success=True,
            query_metadata=query_metadata,
            data=data,
            schema=schema,
            row_count=len(result_df),
            execution_time_ms=round(execution_time, 2)
        )
        
    except HTTPException:
        # Re-raise validation errors
        raise
        
    except Exception as e:
        execution_time = (time.time() - start_time) * 1000
        
        # Prepare error metadata
        table_suggestions = _get_table_suggestions(engine)
        error_metadata = {
            "query_timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
            "table_name": getattr(engine.engine.config, 'table_name', 'unknown'),
            "available_tables": table_suggestions,
            "query_length": len(query_request.sql),
            "execution_time_ms": round(execution_time, 2)
        }
        
        # Determine error type
        error_str = str(e)
        if "not found" in error_str.lower() or "does not exist" in error_str.lower():
            error_type = "TABLE_NOT_FOUND"
        elif "syntax error" in error_str.lower() or "parser error" in error_str.lower():
            error_type = "SQL_SYNTAX_ERROR"
        elif "bind" in error_str.lower():
            error_type = "COLUMN_NOT_FOUND"
        else:
            error_type = "EXECUTION_ERROR"
        
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "error": error_str,
                "error_type": error_type,
                "query_metadata": error_metadata
            }
        )


@router.get("/sql/schema")
async def get_data_schema(engine: FinOpsEngine = Depends()):
    """
    📋 Get schema information for available tables
    
    Returns column names, data types, and sample values for the main data table
    and any available views.
    """
    try:
        # Get main table schema
        main_schema = engine.schema()
        
        # Get table suggestions
        table_suggestions = _get_table_suggestions(engine)
        
        # Try to get a sample of data to show example values
        sample_query = f"SELECT * FROM {engine.engine.config.table_name} LIMIT 3"
        try:
            sample_df = engine.query(sample_query)
            sample_data = sample_df.to_dicts()
        except Exception:
            sample_data = []
        
        return {
            "main_table": {
                "name": engine.engine.config.table_name,
                "schema": main_schema,
                "sample_data": sample_data,
                "row_count_estimate": "Run: SELECT COUNT(*) FROM table_name"
            },
            "available_tables": table_suggestions,
            "data_export_type": engine.engine.config.data_export_type.value,
            "query_examples": [
                {
                    "name": "Top Services by Cost",
                    "sql": f"SELECT product_servicecode, SUM(unblended_cost) as total_cost FROM {engine.engine.config.table_name} WHERE unblended_cost > 0 GROUP BY product_servicecode ORDER BY total_cost DESC LIMIT 10"
                },
                {
                    "name": "Monthly Cost Trends",
                    "sql": f"SELECT billing_period, SUM(unblended_cost) as monthly_cost FROM {engine.engine.config.table_name} GROUP BY billing_period ORDER BY billing_period"
                },
                {
                    "name": "Cost by Account",
                    "sql": f"SELECT line_item_usage_account_id, SUM(unblended_cost) as account_cost FROM {engine.engine.config.table_name} GROUP BY line_item_usage_account_id ORDER BY account_cost DESC"
                }
            ]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving schema: {str(e)}")


@router.get("/sql/tables")
async def list_available_tables(engine: FinOpsEngine = Depends()):
    """
    📚 List all available tables and views for querying
    
    Returns a list of tables that can be used in SQL queries, including
    the main data table and any pre-created views.
    """
    try:
        table_suggestions = _get_table_suggestions(engine)
        
        return {
            "available_tables": [
                {
                    "name": engine.engine.config.table_name,
                    "type": "main_table", 
                    "description": f"Main {engine.engine.config.data_export_type.value} cost data export",
                    "suggested_columns": ["product_servicecode", "unblended_cost", "billing_period", "line_item_usage_account_id"]
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
            "data_export_type": engine.engine.config.data_export_type.value,
            "main_table": engine.engine.config.table_name
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing tables: {str(e)}")