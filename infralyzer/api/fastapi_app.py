"""
FastAPI application factory for FinOps cost analytics API
"""
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Any, Optional
import os

from ..finops_engine import FinOpsEngine
from ..engine import DataConfig, DataExportType
from .endpoints import (
    kpi_router,
    spend_router,
    optimization_router,
    allocation_router,
    discounts_router,
    mcp_router,
    ai_router,
    sql_router
)


class FinOpsAPI:
    """
    FastAPI application wrapper for FinOps cost analytics.
    
    Provides a complete REST API for all cost analytics functionality
    with automatic OpenAPI documentation and validation.
    """
    
    def __init__(self, engine: FinOpsEngine):
        """Initialize FastAPI app with FinOps engine."""
        self.engine = engine
        self.app = self._create_app()
    
    def _create_app(self) -> FastAPI:
        """Create FastAPI application with all endpoints."""
        app = FastAPI(
            title="FinOps Cost Analytics API",
            description="""
            Comprehensive AWS cost analytics and optimization platform.
            
            ## Features
            - **KPI Summary**: Comprehensive cost metrics dashboard
            - **Spend Analytics**: Real-time spend visibility and trends  
            - **Optimization**: Cost optimization recommendations
            - **Allocation**: Cost allocation and tagging management
            - **Discounts**: Enterprise discount tracking
            - **AI Insights**: AI-powered cost recommendations
            - **MCP Integration**: Model Context Protocol support
            - **SQL Queries**: Execute custom SQL queries on cost data
            
            ## Data Sources
            - Local parquet files (preferred for cost savings)
            - S3 parquet files (fallback)
            - Powered by DuckDB SQL engine
            
            ## SQL Query Interface
            - Execute custom SELECT queries on your AWS cost data
            - Support for complex JOINs, aggregations, and analytics
            - Access to main data tables and pre-built cost views
            - JSON and CSV output formats
            
            ## Authentication
            JWT/API key based authentication with role-based access control.
            """,
            version="1.0.0",
            docs_url="/docs",
            redoc_url="/redoc"
        )
        
        # Add CORS middleware
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],  # Configure appropriately for production
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        # Add engine dependency
        app.dependency_overrides[get_finops_engine] = lambda: self.engine
        
        # Include all routers
        app.include_router(kpi_router, prefix="/api/v1/finops", tags=["KPI Summary"])
        app.include_router(spend_router, prefix="/api/v1/finops", tags=["Spend Analytics"])
        app.include_router(optimization_router, prefix="/api/v1/finops", tags=["Optimization"])
        app.include_router(allocation_router, prefix="/api/v1/finops", tags=["Allocation"])
        app.include_router(discounts_router, prefix="/api/v1/finops", tags=["Discounts"])
        app.include_router(mcp_router, prefix="/api/v1/finops", tags=["MCP Integration"])
        app.include_router(ai_router, prefix="/api/v1/finops", tags=["AI Recommendations"])
        app.include_router(sql_router, prefix="/api/v1/finops", tags=["SQL Queries"])
        
        # Add health check endpoint
        @app.get("/health", tags=["Health"])
        async def health_check():
            """Health check endpoint."""
            return {
                "status": "healthy",
                "version": "1.0.0",
                "engine_status": "operational",
                "data_source": "local" if self.engine.has_local_data() else "s3"
            }
        
        # Add root endpoint
        @app.get("/", tags=["Root"])
        async def root():
            """Root endpoint with API information."""
            return {
                "message": "FinOps Cost Analytics API",
                "version": "1.0.0",
                "docs": "/docs",
                "health": "/health",
                "api_base": "/api/v1/finops"
            }
        
        return app


# Dependency injection for FinOps engine
def get_finops_engine() -> FinOpsEngine:
    """Dependency to get FinOps engine instance."""
    # This will be overridden by the app instance
    raise HTTPException(status_code=500, detail="FinOps engine not configured")


def create_finops_app(
    s3_bucket: str,
    s3_data_prefix: str,
    data_export_type: str,
    local_data_path: Optional[str] = None,
    **config_kwargs
) -> FastAPI:
    """
    Factory function to create FinOps FastAPI application.
    
    Args:
        s3_bucket: S3 bucket containing data export files
        s3_data_prefix: S3 prefix path to data directory
        data_export_type: Type of AWS data export
        local_data_path: Optional local data cache directory
        **config_kwargs: Additional configuration parameters
    
    Returns:
        Configured FastAPI application
    
    Example:
        app = create_finops_app(
            s3_bucket="my-cost-data",
            s3_data_prefix="cur2/cur2/data",
            data_export_type="CUR2.0",
            local_data_path="./local_data"
        )
        
        # Run with uvicorn
        # uvicorn main:app --host 0.0.0.0 --port 8000
    """
    # Create configuration
    config = DataConfig(
        s3_bucket=s3_bucket,
        s3_data_prefix=s3_data_prefix,
        data_export_type=DataExportType(data_export_type),
        local_data_path=local_data_path,
        prefer_local_data=True if local_data_path else False,
        **config_kwargs
    )
    
    # Create FinOps engine
    engine = FinOpsEngine(config)
    
    # Create and return FastAPI app
    finops_api = FinOpsAPI(engine)
    return finops_api.app


def create_finops_app_from_env() -> FastAPI:
    """
    Create FinOps FastAPI application from environment variables.
    
    Environment variables:
        FINOPS_S3_BUCKET: S3 bucket name
        FINOPS_S3_PREFIX: S3 data prefix
        FINOPS_DATA_TYPE: Data export type (CUR2.0, FOCUS1.0, etc.)
        FINOPS_LOCAL_PATH: Local data cache path (optional)
        FINOPS_AWS_REGION: AWS region (optional)
        FINOPS_TABLE_NAME: Table name for queries (optional, default: CUR)
    
    Returns:
        Configured FastAPI application
    """
    # Get required environment variables
    s3_bucket = os.getenv("FINOPS_S3_BUCKET")
    s3_prefix = os.getenv("FINOPS_S3_PREFIX") 
    data_type = os.getenv("FINOPS_DATA_TYPE", "CUR2.0")
    
    if not s3_bucket or not s3_prefix:
        raise ValueError("FINOPS_S3_BUCKET and FINOPS_S3_PREFIX environment variables are required")
    
    # Get optional environment variables
    config_kwargs = {}
    
    if local_path := os.getenv("FINOPS_LOCAL_PATH"):
        config_kwargs["local_data_path"] = local_path
    
    if aws_region := os.getenv("FINOPS_AWS_REGION"):
        config_kwargs["aws_region"] = aws_region
    
    if table_name := os.getenv("FINOPS_TABLE_NAME"):
        config_kwargs["table_name"] = table_name
    
    # Add AWS credentials if provided
    if aws_access_key := os.getenv("AWS_ACCESS_KEY_ID"):
        config_kwargs["aws_access_key_id"] = aws_access_key
    
    if aws_secret_key := os.getenv("AWS_SECRET_ACCESS_KEY"):
        config_kwargs["aws_secret_access_key"] = aws_secret_key
    
    if aws_session_token := os.getenv("AWS_SESSION_TOKEN"):
        config_kwargs["aws_session_token"] = aws_session_token
    
    return create_finops_app(
        s3_bucket=s3_bucket,
        s3_data_prefix=s3_prefix,
        data_export_type=data_type,
        **config_kwargs
    )