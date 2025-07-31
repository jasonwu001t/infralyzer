"""
DE Polars - Advanced SQL interface for AWS Data Exports with modular analytics

This package provides a comprehensive cost analytics platform with:
- 🧠 DuckDB SQL Engine for advanced analytics
- 📊 Modular analytics components for cost optimization
- 💾 Local data caching for cost savings
- 🚀 FastAPI-ready architecture
- 🔒 Enterprise-grade authentication

Quick Start:
    # Traditional usage (backward compatible)
    from de_polars import DataExportsPolars
    
    # New modular usage
    from de_polars import FinOpsEngine
    from de_polars.analytics import KPISummaryAnalytics
    
    # Engine with comprehensive analytics
    engine = FinOpsEngine(config)
    kpi_analytics = KPISummaryAnalytics(engine)
    summary = kpi_analytics.get_comprehensive_summary()
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

__version__ = "0.4.0"
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