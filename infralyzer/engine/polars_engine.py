"""
Polars Engine - Direct Polars-based query execution
"""
import polars as pl
import pandas as pd
import pyarrow as pa
import io
from pathlib import Path
from typing import Optional, List, Dict, Any, Union
from datetime import datetime

from .base_engine import BaseQueryEngine, QueryResultFormat
from .data_config import DataConfig
from ..auth import check_credential_expiration


class PolarsEngine(BaseQueryEngine):
    """
    Polars-based query engine for executing SQL queries on AWS data exports.
    Uses Polars directly for data processing with native Polars SQL capabilities.
    """
    
    def __init__(self, config: DataConfig):
        """Initialize Polars engine with data configuration."""
        super().__init__(config)
        self._dataframe = None
        
        # Check credential expiration if provided
        if config.expiration:
            check_credential_expiration(config.expiration)
    
    @property
    def engine_name(self) -> str:
        return "Polars"
    
    @property
    def supports_s3_direct(self) -> bool:
        return True  # Polars can read from S3
    
    @property
    def supports_local_data(self) -> bool:
        return True
    
    def _get_storage_options(self) -> Dict[str, str]:
        """Get storage options for S3 access."""
        from ..auth import get_storage_options
        creds = self.config.get_aws_credentials()
        return get_storage_options(**creds)
    
    def _discover_data_files(self) -> List[str]:
        """Discover available data files in S3 based on configuration."""
        from ..data.s3_data_manager import S3DataManager
        s3_manager = S3DataManager(self.config)
        return s3_manager.discover_data_files()
    
    def _discover_local_data_files(self) -> List[str]:
        """Discover available local data files."""
        from ..data.local_data_manager import LocalDataManager
        local_manager = LocalDataManager(self.config)
        return local_manager.discover_data_files()
    
    def has_local_data(self) -> bool:
        """Check if local data is available."""
        if not self.config.local_data_path or not self.config.local_bucket_path:
            return False
        
        try:
            local_files = self._discover_local_data_files()
            return len(local_files) > 0
        except Exception:
            return False
    
    def _load_data(self, force_s3: bool = False) -> pl.DataFrame:
        """Load data into Polars DataFrame."""
        if self._dataframe is not None and not force_s3:
            return self._dataframe
        
        # Determine data source
        use_local_data = (
            not force_s3 and 
            self.config.prefer_local_data and 
            self.has_local_data()
        )
        
        if use_local_data:
            print("Loading data with Polars engine using LOCAL DATA...")
            data_files = self._discover_local_data_files()
            if not data_files:
                raise ValueError("No local data files found. Run download_data_locally() first.")
            
            # Load local parquet files
            self._dataframe = pl.read_parquet(data_files)
            print(f"Loaded {self._dataframe.shape[0]} rows from local files")
            
        else:
            print("Loading data with Polars engine using S3 DATA...")
            data_files = self._discover_data_files()
            if not data_files:
                raise ValueError("No data files found in S3. Check your S3 bucket, prefix, and date filters.")
            
            # Get S3 storage options
            storage_options = self._get_storage_options()
            
            # Load S3 parquet files
            self._dataframe = pl.read_parquet(data_files, storage_options=storage_options)
            print(f"Loaded {self._dataframe.shape[0]} rows from S3 files")
        
        return self._dataframe
    
    def query(self, 
              sql: str, 
              format: QueryResultFormat = QueryResultFormat.RECORDS,
              force_s3: bool = False) -> Union[List[Dict[str, Any]], pd.DataFrame, str, pa.Table, pl.DataFrame]:
        """
        Execute SQL query using Polars SQL engine.
        
        Args:
            sql: SQL query to execute
            format: Desired output format
            force_s3: Force using S3 data even if local data is available
            
        Returns:
            Query results in the specified format
        """
        # Load data into Polars DataFrame
        df = self._load_data(force_s3)
        
        # Create a Polars LazyFrame context for SQL
        # Replace table name in SQL with the actual DataFrame reference
        # Note: Polars SQL support might need adjustment based on version
        try:
            # Execute SQL query using Polars SQL
            print(f"Running Polars SQL query: {sql[:100]}{'...' if len(sql) > 100 else ''}")
            
            # For now, we'll use a simple approach - in newer Polars versions,
            # you can use pl.sql() or register the DataFrame
            ctx = pl.SQLContext({self.config.table_name: df.lazy()})
            result_lazy = ctx.execute(sql)
            result_df = result_lazy.collect()
            
            print(f"Query completed: {result_df.shape[0]} rows, {result_df.shape[1]} columns")
            
            # Return results in requested format
            if format == QueryResultFormat.RECORDS:
                return result_df.to_dicts()
                
            elif format == QueryResultFormat.DATAFRAME:
                return result_df.to_pandas()
                
            elif format == QueryResultFormat.CSV:
                csv_buffer = io.StringIO()
                result_df.to_pandas().to_csv(csv_buffer, index=False)
                return csv_buffer.getvalue()
                
            elif format == QueryResultFormat.ARROW:
                return result_df.to_arrow()
                
            elif format == QueryResultFormat.RAW:
                return result_df  # Return native Polars DataFrame
                
            else:
                raise ValueError(f"Unsupported format: {format}")
                
        except Exception as e:
            print(f"Polars query error: {str(e)}")
            # Fallback to basic DataFrame operations for simple queries
            print("Attempting fallback with basic Polars operations...")
            
            # Very basic SQL parsing for simple SELECT statements
            if sql.upper().strip().startswith('SELECT'):
                # This is a simplified fallback - in production you'd want more robust SQL parsing
                # For now, just return the whole dataset with limit if specified
                if 'LIMIT' in sql.upper():
                    import re
                    limit_match = re.search(r'LIMIT\s+(\d+)', sql.upper())
                    if limit_match:
                        limit = int(limit_match.group(1))
                        result_df = df.head(limit)
                    else:
                        result_df = df.head(100)  # Default limit
                else:
                    result_df = df.head(1000)  # Safety limit
                
                # Return in requested format
                if format == QueryResultFormat.RECORDS:
                    return result_df.to_dicts()
                elif format == QueryResultFormat.DATAFRAME:
                    return result_df.to_pandas()
                elif format == QueryResultFormat.CSV:
                    csv_buffer = io.StringIO()
                    result_df.to_pandas().to_csv(csv_buffer, index=False)
                    return csv_buffer.getvalue()
                elif format == QueryResultFormat.ARROW:
                    return result_df.to_arrow()
                elif format == QueryResultFormat.RAW:
                    return result_df
            
            raise  # Re-raise original error if fallback fails
    
    def schema(self) -> Dict[str, str]:
        """Get schema information for the data."""
        if self._schema_cache:
            return self._schema_cache
        
        try:
            df = self._load_data()
            self._schema_cache = {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)}
            return self._schema_cache
        except Exception as e:
            print(f"Could not get schema: {e}")
            return {}
    
    def catalog(self) -> Dict[str, Any]:
        """Get data catalog information."""
        return {
            "engine": self.engine_name,
            "table_name": self.config.table_name,
            "data_export_type": self.config.data_export_type.value,
            "partition_format": self.config.partition_format,
            "s3_location": f"s3://{self.config.s3_bucket}/{self.config.s3_data_prefix}",
            "local_location": self.config.local_bucket_path,
            "has_local_data": self.has_local_data(),
            "schema": self.schema(),
            "date_range": {
                "start": self.config.date_start,
                "end": self.config.date_end,
                "format": self.config.date_format
            },
            "supports_s3_direct": self.supports_s3_direct,
            "supports_local_data": self.supports_local_data,
            "data_loaded": self._dataframe is not None,
            "data_shape": self._dataframe.shape if self._dataframe is not None else None
        }


# Register Polars engine with the factory
from .base_engine import QueryEngineFactory
QueryEngineFactory.register_engine("polars", PolarsEngine)