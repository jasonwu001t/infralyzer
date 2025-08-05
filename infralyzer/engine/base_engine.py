"""
Abstract base class for query engines in Infralyzer
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Union, Optional
import pandas as pd
from enum import Enum

from .data_config import DataConfig


class QueryResultFormat(Enum):
    """Supported query result formats."""
    RECORDS = "records"  # List[Dict] - JSON records
    DATAFRAME = "dataframe"  # pandas.DataFrame
    CSV = "csv"  # CSV string
    ARROW = "arrow"  # PyArrow Table
    RAW = "raw"  # Engine-specific raw format


class BaseQueryEngine(ABC):
    """
    Abstract base class for query engines.
    
    All query engines (DuckDB, Polars, Athena, etc.) must implement this interface.
    This allows for pluggable query engines while maintaining a consistent API.
    """
    
    def __init__(self, config: DataConfig):
        """
        Initialize the query engine with configuration.
        
        Args:
            config: DataConfig object with all necessary configuration
        """
        self.config = config
        self._schema_cache = None
    
    @property
    @abstractmethod
    def engine_name(self) -> str:
        """Return the name of this query engine."""
        pass
    
    @property
    @abstractmethod
    def supports_s3_direct(self) -> bool:
        """Return True if engine can query S3 data directly."""
        pass
    
    @property
    @abstractmethod
    def supports_local_data(self) -> bool:
        """Return True if engine can query local data files."""
        pass
    
    @abstractmethod
    def query(self, 
              sql: str, 
              format: QueryResultFormat = QueryResultFormat.RECORDS,
              force_s3: bool = False) -> Union[List[Dict[str, Any]], pd.DataFrame, str, Any]:
        """
        Execute SQL query and return results in specified format.
        
        Args:
            sql: SQL query to execute
            format: Desired output format
            force_s3: Force using S3 data even if local data is available
            
        Returns:
            Query results in the specified format
        """
        pass
    
    @abstractmethod
    def schema(self) -> Dict[str, str]:
        """
        Get schema information for the data.
        
        Returns:
            Dictionary mapping column names to data types
        """
        pass
    
    @abstractmethod
    def catalog(self) -> Dict[str, Any]:
        """
        Get data catalog information.
        
        Returns:
            Dictionary with catalog metadata
        """
        pass
    
    def sample(self, n: int = 10, format: QueryResultFormat = QueryResultFormat.RECORDS) -> Any:
        """
        Get a sample of the data.
        
        Args:
            n: Number of rows to sample
            format: Desired output format
            
        Returns:
            Sample data in specified format
        """
        return self.query(f"SELECT * FROM {self.config.table_name} LIMIT {n}", format=format)
    
    @abstractmethod
    def has_local_data(self) -> bool:
        """Check if local data is available."""
        pass
    
    def download_data_locally(self, overwrite: bool = False, show_progress: bool = True) -> None:
        """
        Download S3 data to local storage for optimization.
        
        Default implementation - engines can override if they have specific needs.
        """
        if not self.supports_local_data:
            raise NotImplementedError(f"{self.engine_name} does not support local data caching")
            
        from ..data.data_downloader import DataDownloader
        downloader = DataDownloader(self.config)
        downloader.download_data_locally(overwrite=overwrite, show_progress=show_progress)
    
    def info(self) -> None:
        """Print information about the data source and configuration."""
        print(f"Query Engine: {self.engine_name}")
        print(f"Data Export Type: {self.config.data_export_type.value}")
        
        # Show appropriate date format based on export type
        if self.config.data_export_type.value == 'COH':
            print(f"Partition Format: {self.config.partition_format}=YYYY-MM-DD (daily)")
            print(f"Date Format Required: YYYY-MM-DD (e.g., '2025-07-15')")
        else:
            print(f"Partition Format: {self.config.partition_format}=YYYY-MM (monthly)")
            print(f"Date Format Required: YYYY-MM (e.g., '2025-07')")
            
        print(f"Data Path: s3://{self.config.s3_bucket}/{self.config.s3_data_prefix}")
        
        if self.config.local_data_path:
            print(f"Local Cache: {self.config.local_bucket_path}")
            print(f"Prefer Local: {self.config.prefer_local_data}")
        
        print(f"S3 Direct Support: {self.supports_s3_direct}")
        print(f"Local Data Support: {self.supports_local_data}")
        print(f"Has Local Data: {self.has_local_data()}")


class QueryEngineFactory:
    """Factory for creating query engines."""
    
    _engines = {}
    
    @classmethod
    def register_engine(cls, name: str, engine_class: type):
        """Register a query engine implementation."""
        cls._engines[name.lower()] = engine_class
    
    @classmethod
    def create_engine(cls, engine_name: str, config: DataConfig) -> BaseQueryEngine:
        """
        Create a query engine instance.
        
        Args:
            engine_name: Name of the engine (duckdb, polars, athena)
            config: DataConfig object
            
        Returns:
            Query engine instance
        """
        engine_name = engine_name.lower()
        if engine_name not in cls._engines:
            available = ", ".join(cls._engines.keys())
            raise ValueError(f"Unknown query engine: {engine_name}. Available: {available}")
        
        engine_class = cls._engines[engine_name]
        return engine_class(config)
    
    @classmethod
    def list_engines(cls) -> List[str]:
        """List all registered query engines."""
        return list(cls._engines.keys())