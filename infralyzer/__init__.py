"""
Infralyzer - Multi-Engine FinOps Analytics Platform for AWS Cost Optimization

This package provides a comprehensive cost analytics platform with:
- Pluggable Query Engines (DuckDB, Polars, AWS Athena)
- Modular analytics components for cost optimization  
- Local data caching for cost savings
- FastAPI-ready architecture
- Enterprise-grade authentication
- No wasteful data conversions - optimized performance

Quick Start:
    # New multi-engine architecture
    from infralyzer import FinOpsEngine, DataConfig, DataExportType
    from infralyzer.engine import QueryEngineFactory, QueryResultFormat
    
    # Create config
    config = DataConfig(
        s3_bucket='my-bucket',
        s3_data_prefix='cur2/data',
        data_export_type=DataExportType.CUR_2_0
    )
    
    # Use different engines
    duckdb_engine = FinOpsEngine(config, engine_name="duckdb")    # Fast analytics (default)
    polars_engine = FinOpsEngine(config, engine_name="polars")    # Modern DataFrame API
    athena_engine = FinOpsEngine(config, engine_name="athena")    # Serverless AWS service
    
    # Query with different output formats
    records = engine.query("SELECT * FROM CUR LIMIT 10")  # List[Dict] - default
    df = engine.query("SELECT * FROM CUR LIMIT 10", format=QueryResultFormat.DATAFRAME)  # pandas.DataFrame
    csv = engine.query("SELECT * FROM CUR LIMIT 10", format=QueryResultFormat.CSV)  # CSV string
"""

# Core engine and configuration
from .engine import DuckDBEngine, DataConfig, DataExportType

# Data management
from .data import S3DataManager, LocalDataManager, DataDownloader

# Analytics modules
from .analytics import (
    KPISummaryAnalytics,
    SpendAnalytics,
    OptimizationAnalytics,
    AllocationAnalytics,
    DiscountAnalytics,
    AIRecommendationAnalytics
)

# Include MCP integration
from .analytics.mcp_integration import MCPIntegrationAnalytics

# Authentication utilities
from .auth import check_credential_expiration, get_boto3_client, get_storage_options

# Utility functions
from .utils import (
    CurrencyFormatter,
    NumberFormatter,
    DateFormatter,
    DataValidator,
    ConfigValidator,
    QueryProfiler,
    CacheManager,
    DataExporter,
    ReportGenerator
)

# Data partitioner (existing functionality)
from .data_partitioner import DataPartitioner

# Backward compatibility - recreate DataExportsPolars using new architecture
from .client import DataExportsPolars

# New unified engine interface
from .finops_engine import FinOpsEngine

__version__ = "1.0.0"
__all__ = [
    # Core components
    "DuckDBEngine",
    "DataConfig", 
    "DataExportType",
    "FinOpsEngine",
    
    # Data management
    "S3DataManager",
    "LocalDataManager", 
    "DataDownloader",
    
    # Analytics modules
    "KPISummaryAnalytics",
    "SpendAnalytics",
    "OptimizationAnalytics",
    "AllocationAnalytics",
    "DiscountAnalytics",
    "AIRecommendationAnalytics",
    "MCPIntegrationAnalytics",
    
    # Authentication
    "check_credential_expiration",
    "get_boto3_client",
    "get_storage_options",
    
    # Utilities
    "CurrencyFormatter",
    "NumberFormatter",
    "DateFormatter", 
    "DataValidator",
    "ConfigValidator",
    "QueryProfiler",
    "CacheManager",
    "DataExporter",
    "ReportGenerator",
    
    # Existing components (backward compatibility)
    "DataExportsPolars",
    "DataPartitioner"
]