"""
DE Polars - Simple SQL interface for AWS Data Exports
"""

from .client import DataExportsPolars
from .data_partitioner import DataPartitioner

__version__ = "0.3.0"
__all__ = ["DataExportsPolars", "DataPartitioner"] 