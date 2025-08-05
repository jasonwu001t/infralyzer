"""
Infralyzer Engine - Core data processing and query execution with pluggable engines
"""

from .base_engine import BaseQueryEngine, QueryResultFormat, QueryEngineFactory
from .data_config import DataConfig, DataExportType
from .duckdb_engine import DuckDBEngine
from .polars_engine import PolarsEngine
from .athena_engine import AthenaEngine

# Register engines with factory
QueryEngineFactory.register_engine("duckdb", DuckDBEngine)
QueryEngineFactory.register_engine("polars", PolarsEngine)
QueryEngineFactory.register_engine("athena", AthenaEngine)

__all__ = [
    "BaseQueryEngine", 
    "QueryResultFormat", 
    "QueryEngineFactory",
    "DuckDBEngine",
    "PolarsEngine", 
    "AthenaEngine",
    "DataConfig", 
    "DataExportType"
]