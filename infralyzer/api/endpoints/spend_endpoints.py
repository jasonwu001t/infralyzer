"""
Spend Analytics API endpoints - View 1: Actual Spend Analysis
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Dict, Any, Optional, List
from pydantic import BaseModel

from ...finops_engine import FinOpsEngine


router = APIRouter()


class SpendSummaryResponse(BaseModel):
    """Response model for spend summary."""
    invoice_total: float
    mom_change: float
    yoy_change: float
    trend_data: List[Dict[str, Any]]
    forecast: Dict[str, Any]


class RegionsResponse(BaseModel):
    """Response model for top regions."""
    regions: List[Dict[str, Any]]


class ServicesResponse(BaseModel):
    """Response model for top services."""
    services: List[Dict[str, Any]]


@router.get("/spend/invoice/summary", response_model=SpendSummaryResponse)
async def get_invoice_summary(
    months_back: int = Query(12, description="Number of months to include in trend analysis"),
    engine: FinOpsEngine = Depends()
):
    """
    Last month invoice total, MoM change %, YoY change %.
    
    **Returns:**
    - Monthly trendline data (12 months)
    - Forecasted next month spend based on trends
    - Month-over-month and year-over-year comparisons
    """
    try:
        result = engine.spend.get_invoice_summary(months_back=months_back)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving invoice summary: {str(e)}")


@router.get("/spend/regions/top", response_model=RegionsResponse)
async def get_top_regions(
    limit: int = Query(10, description="Number of top regions to return"),
    engine: FinOpsEngine = Depends()
):
    """
    Top regions by spend with cost breakdown.
    
    **Returns:**
    - Percentage of total spend per region
    - Month-over-month change per region
    - Regional spend distribution
    """
    try:
        result = engine.spend.get_top_regions(limit=limit)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving top regions: {str(e)}")


@router.get("/spend/services/top", response_model=ServicesResponse)
async def get_top_services(
    limit: int = Query(10, description="Number of top services to return"),
    engine: FinOpsEngine = Depends()
):
    """
    Top AWS services by spend.
    
    **Returns:**
    - Cost trend analysis per service
    - Drill-down capability to resource level
    - Service spend distribution
    """
    try:
        result = engine.spend.get_top_services(limit=limit)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving top services: {str(e)}")


@router.get("/spend/breakdown")
async def get_spend_breakdown(
    dimensions: List[str] = Query(["region", "service"], description="Dimensions to break down by"),
    engine: FinOpsEngine = Depends()
):
    """
    Multi-dimensional spend analysis (region × service matrix).
    
    **Features:**
    - Interactive drill-down from region → AZ → service → resource
    - Support for custom time ranges and filters
    - Flexible dimension analysis
    """
    try:
        result = engine.spend.get_spend_breakdown(dimensions=dimensions)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving spend breakdown: {str(e)}")


class ExportRequest(BaseModel):
    """Request model for spend data export."""
    format: str = "csv"
    date_range: Optional[Dict[str, str]] = None


@router.post("/spend/export")
async def export_spend_data(
    export_request: ExportRequest,
    engine: FinOpsEngine = Depends()
):
    """
    Export spend data in multiple formats (CSV, Excel, PDF).
    
    **Features:**
    - Custom date ranges and dimension filtering
    - Scheduled export capabilities
    - Multiple output formats
    """
    try:
        result = engine.spend.export_spend_data(
            format=export_request.format,
            date_range=export_request.date_range
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error exporting spend data: {str(e)}")