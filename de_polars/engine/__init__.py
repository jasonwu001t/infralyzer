"""
DE Polars Engine - Core data processing and query execution
"""

from .duckdb_engine import DuckDBEngine
from .data_config import DataConfig, DataExportType

__all__ = ["DuckDBEngine", "DataConfig", "DataExportType"]