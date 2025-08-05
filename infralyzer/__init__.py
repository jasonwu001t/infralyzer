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
# Core engine and configuration
from .engine import (
    BaseQueryEngine,
    QueryEngineFactory,
    QueryResultFormat,
    DuckDBEngine,
    PolarsEngine,
    AthenaEngine,
    DataConfig,
    DataExportType
)

# Data management
from .data import (
    S3DataManager,
    LocalDataManager,
    DataDownloader
)

# Analytics modules
from .analytics import (
    KPISummaryAnalytics,
    SpendAnalytics,
    OptimizationAnalytics,
    AllocationAnalytics,
    DiscountAnalytics,
    AIRecommendationAnalytics
)

# MCP integration
from .analytics.mcp_integration import MCPIntegrationAnalytics

# Authentication utilities
from .auth import (
    check_credential_expiration,
    get_boto3_client,
    get_storage_options
)

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
    ReportGenerator,
    handle_exception,
    log_and_raise,
    safe_execute
)

# SQL patterns and query utilities
from .utils.sql_patterns import SQLPatterns, QueryBuilder

# Configuration validation
from .config_validator import ConfigValidator as EnhancedConfigValidator

# Exception handling
from .exceptions import (
    InfralyzerError,
    ConfigurationError,
    DataSourceError,
    QueryExecutionError,
    AuthenticationError,
    ValidationError,
    EngineError,
    DataProcessingError,
    APIError,
    CacheError,
    ExportError,
    ErrorCodes
)

# Logging utilities
from .logging_config import get_logger, InfralyzerLogger

# Backward compatibility and utilities removed - use FinOpsEngine instead

# Unified engine interface
from .finops_engine import FinOpsEngine

from .constants import VERSION
__version__ = VERSION
__all__ = [
    # Core components
    "BaseQueryEngine",
    "QueryEngineFactory", 
    "QueryResultFormat",
    "DuckDBEngine",
    "PolarsEngine",
    "AthenaEngine",
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
    "handle_exception",
    "log_and_raise",
    "safe_execute",
    
    # SQL utilities
    "SQLPatterns",
    "QueryBuilder",
    
    # Enhanced configuration
    "EnhancedConfigValidator",
    
    # Exception classes
    "InfralyzerError",
    "ConfigurationError", 
    "DataSourceError",
    "QueryExecutionError",
    "AuthenticationError",
    "ValidationError",
    "EngineError",
    "DataProcessingError",
    "APIError",
    "CacheError",
    "ExportError",
    "ErrorCodes",
    
    # Logging
    "get_logger",
    "InfralyzerLogger"
]