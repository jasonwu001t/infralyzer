"""
Abstract base class for query engines in Infralyzer
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Union, Optional
import pandas as pd
from enum import Enum

from .data_config import DataConfig
from ..exceptions import EngineError, QueryExecutionError, ErrorCodes
from ..logging_config import get_logger
from ..constants import DEFAULT_SAMPLE_SIZE, DEFAULT_TIMEOUT_SECONDS


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
            
        Raises:
            EngineError: If engine initialization fails
        """
        self.config = config
        self._schema_cache = None
        self.logger = get_logger(f"infralyzer.{self.__class__.__name__}")
        self.logger.info(f"Initializing {self.engine_name} engine")
    
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
              format: QueryResultFormat = QueryResultFormat.DATAFRAME,
              force_s3: bool = False) -> Union[List[Dict[str, Any]], pd.DataFrame, str, Any]:
        """
        Execute SQL query and return results in specified format.
        
        Args:
            sql: SQL query to execute
            format: Desired output format (default: DATAFRAME for easy data analysis)
            force_s3: Force using S3 data even if local data is available
            
        Returns:
            Query results in the specified format:
            - DATAFRAME (default): pandas.DataFrame for analysis
            - RECORDS: List[Dict] for JSON/API use
            - CSV: String for file export
            - ARROW: PyArrow table for performance
            - RAW: Engine-specific format
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
    
    def sample(self, n: int = DEFAULT_SAMPLE_SIZE, format: QueryResultFormat = QueryResultFormat.DATAFRAME) -> Any:
        """
        Get a sample of the data.
        
        Args:
            n: Number of rows to sample
            format: Desired output format
            
        Returns:
            Sample data in specified format
            
        Raises:
            QueryExecutionError: If sample query fails
        """
        try:
            self.logger.debug(f"Sampling {n} rows from {self.config.table_name}")
            return self.query(f"SELECT * FROM {self.config.table_name} LIMIT {n}", format=format)
        except Exception as e:
            error_msg = f"Failed to sample data: {str(e)}"
            self.logger.error(error_msg)
            raise QueryExecutionError(
                error_msg,
                error_code=ErrorCodes.QUERY_TIMEOUT,
                context={"table_name": self.config.table_name, "sample_size": n}
            ) from e
    
    @abstractmethod
    def has_local_data(self) -> bool:
        """Check if local data is available."""
        pass
    
    def download_data_locally(self, overwrite: bool = False, show_progress: bool = True) -> None:
        """
        Download S3 data to local storage for optimization.
        
        Args:
            overwrite: Whether to overwrite existing local data
            show_progress: Whether to show download progress
            
        Raises:
            EngineError: If engine doesn't support local data or download fails
        """
        if not self.supports_local_data:
            error_msg = f"{self.engine_name} does not support local data caching"
            self.logger.error(error_msg)
            raise EngineError(
                error_msg,
                error_code=ErrorCodes.UNSUPPORTED_OPERATION,
                context={"engine": self.engine_name, "operation": "local_data_caching"}
            )
            
        try:
            self.logger.info(f"Downloading data locally for {self.engine_name}")
            from ..data.data_downloader import DataDownloader
            downloader = DataDownloader(self.config)
            downloader.download_data_locally(overwrite=overwrite, show_progress=show_progress)
            self.logger.info("Data download completed successfully")
        except Exception as e:
            error_msg = f"Failed to download data locally: {str(e)}"
            self.logger.error(error_msg)
            raise EngineError(
                error_msg,
                error_code=ErrorCodes.ENGINE_INITIALIZATION_FAILED,
                context={"engine": self.engine_name, "operation": "data_download"}
            ) from e
    
    def info(self) -> Dict[str, Any]:
        """
        Get information about the data source and configuration.
        
        Returns:
            Dictionary with engine configuration information
        """
        info_data = {
            "query_engine": self.engine_name,
            "data_export_type": self.config.data_export_type.value,
            "data_path": f"s3://{self.config.s3_bucket}/{self.config.s3_data_prefix}",
            "s3_direct_support": self.supports_s3_direct,
            "local_data_support": self.supports_local_data,
            "has_local_data": self.has_local_data()
        }
        
        # Add date format information
        if self.config.data_export_type.value == 'COH':
            info_data["partition_format"] = f"{self.config.partition_format}=YYYY-MM-DD (daily)"
            info_data["date_format_required"] = "YYYY-MM-DD (e.g., '2025-07-15')"
        else:
            info_data["partition_format"] = f"{self.config.partition_format}=YYYY-MM (monthly)"
            info_data["date_format_required"] = "YYYY-MM (e.g., '2025-07')"
        
        # Add local cache information if available
        if self.config.local_data_path:
            info_data["local_cache"] = self.config.local_bucket_path
            info_data["prefer_local"] = self.config.prefer_local_data
        
        self.logger.debug(f"Engine info retrieved for {self.engine_name}")
        return info_data

    # Convenience methods for easy format switching
    def query_json(self, sql: str, force_s3: bool = False) -> List[Dict[str, Any]]:
        """
        Execute SQL query and return JSON-like records (List[Dict]).
        
        Perfect for API responses, JSON export, and programmatic processing.
        
        Args:
            sql: SQL query to execute
            force_s3: Force using S3 data even if local data is available
            
        Returns:
            List[Dict]: JSON-serializable records
            
        Example:
            >>> records = engine.query_json("SELECT * FROM CUR LIMIT 5")
            >>> print(json.dumps(records, indent=2))
        """
        return self.query(sql, format=QueryResultFormat.RECORDS, force_s3=force_s3)
    
    def query_csv(self, sql: str, force_s3: bool = False) -> str:
        """
        Execute SQL query and return CSV string.
        
        Perfect for file export, data transfer, and spreadsheet import.
        
        Args:
            sql: SQL query to execute
            force_s3: Force using S3 data even if local data is available
            
        Returns:
            str: CSV-formatted string
            
        Example:
            >>> csv_data = engine.query_csv("SELECT * FROM CUR LIMIT 100")
            >>> with open("cost_data.csv", "w") as f:
            ...     f.write(csv_data)
        """
        return self.query(sql, format=QueryResultFormat.CSV, force_s3=force_s3)
    
    def query_arrow(self, sql: str, force_s3: bool = False):
        """
        Execute SQL query and return PyArrow table.
        
        Perfect for high-performance analytics and cross-language compatibility.
        
        Args:
            sql: SQL query to execute
            force_s3: Force using S3 data even if local data is available
            
        Returns:
            pyarrow.Table: High-performance columnar data
            
        Example:
            >>> arrow_table = engine.query_arrow("SELECT * FROM CUR")
            >>> # Convert to pandas for analysis
            >>> df = arrow_table.to_pandas()
        """
        return self.query(sql, format=QueryResultFormat.ARROW, force_s3=force_s3)
    
    def query_raw(self, sql: str, force_s3: bool = False):
        """
        Execute SQL query and return engine-specific raw format.
        
        Perfect for maximum performance and engine-specific operations.
        
        Args:
            sql: SQL query to execute
            force_s3: Force using S3 data even if local data is available
            
        Returns:
            Engine-specific format (DuckDB result, Polars DataFrame, etc.)
            
        Example:
            >>> raw_result = engine.query_raw("SELECT * FROM CUR")
            >>> # Engine-specific operations on raw result
        """
        return self.query(sql, format=QueryResultFormat.RAW, force_s3=force_s3)


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
            
        Raises:
            EngineError: If engine name is invalid or creation fails
        """
        logger = get_logger("infralyzer.QueryEngineFactory")
        engine_name = engine_name.lower()
        
        if engine_name not in cls._engines:
            available = ", ".join(cls._engines.keys())
            error_msg = f"Unknown query engine: {engine_name}. Available: {available}"
            logger.error(error_msg)
            raise EngineError(
                error_msg,
                error_code=ErrorCodes.INVALID_ENGINE_NAME,
                context={"requested_engine": engine_name, "available_engines": list(cls._engines.keys())}
            )
        
        try:
            logger.info(f"Creating {engine_name} engine")
            engine_class = cls._engines[engine_name]
            engine = engine_class(config)
            logger.info(f"{engine_name} engine created successfully")
            return engine
        except Exception as e:
            error_msg = f"Failed to create {engine_name} engine: {str(e)}"
            logger.error(error_msg)
            raise EngineError(
                error_msg,
                error_code=ErrorCodes.ENGINE_INITIALIZATION_FAILED,
                context={"engine_name": engine_name}
            ) from e
    
    @classmethod
    def list_engines(cls) -> List[str]:
        """List all registered query engines."""
        return list(cls._engines.keys())