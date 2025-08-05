"""
Base Pydantic models for consistent API responses across Infralyzer.
"""
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from pydantic import BaseModel, Field
from enum import Enum


class QueryFormat(str, Enum):
    """Supported query result formats."""
    JSON = "json"
    CSV = "csv"
    DATAFRAME = "dataframe"


class ResponseStatus(str, Enum):
    """Standard response status values."""
    SUCCESS = "success"
    ERROR = "error"
    WARNING = "warning"


class BaseResponse(BaseModel):
    """Base response model for all API endpoints."""
    success: bool = Field(description="Whether the request was successful")
    status: ResponseStatus = Field(description="Response status")
    message: Optional[str] = Field(None, description="Optional status message")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")
    execution_time_ms: Optional[float] = Field(None, description="Query execution time in milliseconds")


class DataResponse(BaseResponse):
    """Response model for data-returning endpoints."""
    data: Union[List[Dict[str, Any]], str] = Field(description="Response data")
    row_count: Optional[int] = Field(None, description="Number of rows returned")
    columns: Optional[List[str]] = Field(None, description="Column names")


class ErrorResponse(BaseResponse):
    """Response model for error cases."""
    error_code: Optional[str] = Field(None, description="Error code for tracking")
    error_details: Optional[Dict[str, Any]] = Field(None, description="Additional error context")


class MetricsResponse(BaseResponse):
    """Response model for analytics metrics."""
    metrics: Dict[str, Any] = Field(description="Analytics metrics data")
    summary: Optional[Dict[str, Any]] = Field(None, description="Summary statistics")
    filters_applied: Optional[Dict[str, Any]] = Field(None, description="Filters that were applied")


class QueryRequest(BaseModel):
    """Request model for SQL query endpoints."""
    sql: str = Field(description="SQL query to execute", max_length=10000)
    limit: Optional[int] = Field(1000, description="Maximum rows to return", ge=1, le=10000)
    force_s3: Optional[bool] = Field(False, description="Force S3 querying even if local data available")
    format: Optional[QueryFormat] = Field(QueryFormat.JSON, description="Output format")


class FilterParams(BaseModel):
    """Common filter parameters for analytics endpoints."""
    billing_period: Optional[str] = Field(None, description="Filter by billing period (YYYY-MM format)")
    payer_account_id: Optional[str] = Field(None, description="Filter by payer account ID")
    linked_account_id: Optional[str] = Field(None, description="Filter by linked account ID")
    service_code: Optional[str] = Field(None, description="Filter by AWS service code")
    start_date: Optional[str] = Field(None, description="Start date filter (YYYY-MM-DD)")
    end_date: Optional[str] = Field(None, description="End date filter (YYYY-MM-DD)")


class PaginationParams(BaseModel):
    """Pagination parameters for list endpoints."""
    page: int = Field(1, description="Page number", ge=1)
    page_size: int = Field(100, description="Items per page", ge=1, le=1000)
    
    @property
    def offset(self) -> int:
        """Calculate offset from page and page_size."""
        return (self.page - 1) * self.page_size


class PaginatedResponse(BaseResponse):
    """Response model for paginated endpoints."""
    data: List[Dict[str, Any]] = Field(description="Page data")
    pagination: Dict[str, Any] = Field(description="Pagination metadata")
    
    @classmethod
    def create(
        cls,
        data: List[Dict[str, Any]],
        page: int,
        page_size: int,
        total_count: int,
        **kwargs
    ) -> 'PaginatedResponse':
        """Create a paginated response."""
        total_pages = (total_count + page_size - 1) // page_size
        
        pagination = {
            "current_page": page,
            "page_size": page_size,
            "total_count": total_count,
            "total_pages": total_pages,
            "has_next": page < total_pages,
            "has_previous": page > 1,
            "next_page": page + 1 if page < total_pages else None,
            "previous_page": page - 1 if page > 1 else None
        }
        
        return cls(
            data=data,
            pagination=pagination,
            success=True,
            status=ResponseStatus.SUCCESS,
            **kwargs
        )


class HealthCheckResponse(BaseResponse):
    """Response model for health check endpoints."""
    services: Dict[str, Dict[str, Any]] = Field(description="Service health status")
    overall_status: str = Field(description="Overall system health")
    version: str = Field(description="Application version")


class ConfigValidationResponse(BaseResponse):
    """Response model for configuration validation."""
    validation_results: Dict[str, Any] = Field(description="Validation results")
    recommendations: List[str] = Field(description="Configuration recommendations")
    warnings: List[str] = Field(description="Configuration warnings")


# Common field definitions for reuse
class CommonFields:
    """Common field definitions for consistent API documentation."""
    
    BILLING_PERIOD = Field(
        None,
        description="Billing period in YYYY-MM format (e.g., '2024-01')",
        example="2024-01"
    )
    
    ACCOUNT_ID = Field(
        None,
        description="AWS account ID",
        example="123456789012"
    )
    
    SERVICE_CODE = Field(
        None,
        description="AWS service code",
        example="AmazonEC2"
    )
    
    COST_AMOUNT = Field(
        description="Cost amount in USD",
        example=123.45
    )
    
    RESOURCE_ID = Field(
        description="AWS resource identifier",
        example="i-1234567890abcdef0"
    )
    
    DATE_STRING = Field(
        description="Date in YYYY-MM-DD format",
        example="2024-01-15"
    )