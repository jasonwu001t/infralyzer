"""
Infralyzer Exceptions - Custom exception classes for better error handling.
"""
from typing import Optional, Dict, Any


class InfralyzerError(Exception):
    """Base exception class for Infralyzer package."""
    
    def __init__(self, message: str, error_code: Optional[str] = None, context: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.context = context or {}
    
    def __str__(self) -> str:
        if self.error_code:
            return f"[{self.error_code}] {self.message}"
        return self.message


class ConfigurationError(InfralyzerError):
    """Raised when there's a configuration error."""
    pass


class DataSourceError(InfralyzerError):
    """Raised when there's an issue with data source access."""
    pass


class QueryExecutionError(InfralyzerError):
    """Raised when query execution fails."""
    pass


class AuthenticationError(InfralyzerError):
    """Raised when authentication fails."""
    pass


class ValidationError(InfralyzerError):
    """Raised when data validation fails."""
    pass


class EngineError(InfralyzerError):
    """Raised when query engine encounters an error."""
    pass


class DataProcessingError(InfralyzerError):
    """Raised when data processing fails."""
    pass


class APIError(InfralyzerError):
    """Raised when API operations fail."""
    pass


class CacheError(InfralyzerError):
    """Raised when cache operations fail."""
    pass


class ExportError(InfralyzerError):
    """Raised when data export operations fail."""
    pass


# Error code constants for better error tracking
class ErrorCodes:
    """Standard error codes for consistent error handling."""
    
    # Configuration errors
    INVALID_CONFIG = "CONFIG_001"
    MISSING_REQUIRED_PARAM = "CONFIG_002"
    INVALID_ENGINE_NAME = "CONFIG_003"
    
    # Data source errors
    S3_ACCESS_DENIED = "DATA_001"
    S3_BUCKET_NOT_FOUND = "DATA_002"
    DATA_NOT_FOUND = "DATA_003"
    INVALID_DATA_FORMAT = "DATA_004"
    
    # Query execution errors
    SQL_SYNTAX_ERROR = "QUERY_001"
    QUERY_TIMEOUT = "QUERY_002"
    INSUFFICIENT_MEMORY = "QUERY_003"
    CONNECTION_FAILED = "QUERY_004"
    
    # Authentication errors
    INVALID_CREDENTIALS = "AUTH_001"
    CREDENTIALS_EXPIRED = "AUTH_002"
    INSUFFICIENT_PERMISSIONS = "AUTH_003"
    
    # Validation errors
    INVALID_DATE_FORMAT = "VALID_001"
    DATA_QUALITY_ISSUE = "VALID_002"
    SCHEMA_MISMATCH = "VALID_003"
    
    # Engine errors
    ENGINE_INITIALIZATION_FAILED = "ENGINE_001"
    UNSUPPORTED_OPERATION = "ENGINE_002"
    ENGINE_NOT_AVAILABLE = "ENGINE_003"
    
    # API errors
    INVALID_REQUEST = "API_001"
    RATE_LIMIT_EXCEEDED = "API_002"
    ENDPOINT_NOT_FOUND = "API_003"