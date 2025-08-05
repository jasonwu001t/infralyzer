"""
KPI Summary API endpoints - Comprehensive cost metrics dashboard
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Dict, Any, Optional
from pydantic import BaseModel

from ...finops_engine import FinOpsEngine


router = APIRouter()


class KPISummaryResponse(BaseModel):
    """Response model for KPI summary endpoint."""
    summary_metadata: Dict[str, Any]
    overall_spend: Dict[str, Any]
    ec2_metrics: Dict[str, Any]
    rds_metrics: Dict[str, Any]
    storage_metrics: Dict[str, Any]
    compute_services: Dict[str, Any]
    savings_summary: Dict[str, Any]


@router.get("/kpi/summary", response_model=KPISummaryResponse)
async def get_kpi_summary(
    billing_period: Optional[str] = Query(None, description="Filter by specific billing period (YYYY-MM format)"),
    payer_account_id: Optional[str] = Query(None, description="Filter by specific payer account"),
    linked_account_id: Optional[str] = Query(None, description="Filter by specific linked account"),
    tags_filter: Optional[str] = Query(None, description="Filter by tag key-value pairs (JSON format)"),
    engine: FinOpsEngine = Depends()
):
    """
    Comprehensive cost metrics dashboard powered by kpi_tracker.sql
    
    This is the primary endpoint providing comprehensive cost metrics aggregated from 
    all specialized KPI views for complete cost optimization insights.
    
    **Features:**
    - Complete KPI metrics for last 3 months
    - All major AWS service costs and optimization opportunities  
    - Potential savings calculations across all services
    - Account-level and tag-based cost breakdowns
    
    **Data Sources:**
    - summary_view, kpi_instance_all, kpi_ebs_storage_all, kpi_ebs_snap, kpi_s3_storage_all
    """
    try:
        # Parse tags filter if provided
        tags_dict = None
        if tags_filter:
            import json
            try:
                tags_dict = json.loads(tags_filter)
            except json.JSONDecodeError:
                raise HTTPException(status_code=400, detail="Invalid JSON format for tags_filter")
        
        # Get comprehensive summary
        result = engine.kpi.get_comprehensive_summary(
            billing_period=billing_period,
            payer_account_id=payer_account_id,
            linked_account_id=linked_account_id,
            tags_filter=tags_dict
        )
        
        if "error" in result:
            raise HTTPException(status_code=500, detail=result["message"])
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving KPI summary: {str(e)}")


@router.get("/kpi/health-check")
async def get_cost_health_check(engine: FinOpsEngine = Depends()):
    """
    Run comprehensive cost health check across all modules.
    
    Returns health scores, findings, and recommendations for cost optimization.
    """
    try:
        health_check = engine.run_cost_health_check()
        return health_check
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error running health check: {str(e)}")


@router.get("/kpi/executive-summary")
async def get_executive_summary(engine: FinOpsEngine = Depends()):
    """
    Generate executive summary for leadership reporting.
    
    Provides key metrics, insights, and priority actions for executives.
    """
    try:
        summary = engine.generate_executive_summary()
        return summary
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating executive summary: {str(e)}")


@router.get("/kpi/dashboard-data")
async def get_dashboard_data(engine: FinOpsEngine = Depends()):
    """
    Get comprehensive dashboard data combining multiple analytics modules.
    
    Returns data for all dashboard components in a single API call.
    """
    try:
        dashboard_data = engine.get_dashboard_data()
        return dashboard_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving dashboard data: {str(e)}")