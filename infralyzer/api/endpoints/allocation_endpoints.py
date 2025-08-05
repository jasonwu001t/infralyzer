"""
Allocation Analytics API endpoints - View 3: Cost Allocation & Tagging Management
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Dict, Any, Optional, List
from pydantic import BaseModel

from ...finops_engine import FinOpsEngine


router = APIRouter()


@router.get("/allocation/account-hierarchy")
async def get_account_hierarchy(engine: FinOpsEngine = Depends()):
    """
    Multi-account cost allocation and chargeback.
    
    **Features:**
    - Organizational unit breakdown with cost centers
    - Shared services cost allocation methodologies
    - Cross-account cost visibility
    """
    try:
        result = engine.allocation.get_account_hierarchy()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving account hierarchy: {str(e)}")


@router.get("/allocation/tagging-compliance")
async def get_tagging_compliance(engine: FinOpsEngine = Depends()):
    """
    Analyze tagging compliance across all resources.
    
    **Features:**
    - Identify untagged resources and their cost impact
    - Tag coverage reports by service and region
    - Compliance scoring and recommendations
    """
    try:
        result = engine.allocation.get_tagging_compliance()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error analyzing tagging compliance: {str(e)}")


@router.get("/allocation/cost-center-breakdown")
async def get_cost_center_breakdown(
    period: Optional[str] = Query(None, description="Optional billing period filter"),
    engine: FinOpsEngine = Depends()
):
    """
    Cost allocation by business unit, project, environment.
    
    **Features:**
    - Chargeback calculations and showback reports
    - Budget tracking and variance analysis
    - Tag-based cost allocation
    """
    try:
        result = engine.allocation.get_cost_center_breakdown(period=period)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving cost center breakdown: {str(e)}")


class TaggingRule(BaseModel):
    """Model for tagging rule definition."""
    name: str
    tag_key: str
    tag_value: str
    criteria: Dict[str, Any] = {}
    enforcement: str = "monitor"


class TaggingRulesRequest(BaseModel):
    """Request model for creating tagging rules."""
    rules: List[TaggingRule]


@router.post("/allocation/tagging-rules")
async def create_tagging_rules(
    request: TaggingRulesRequest,
    engine: FinOpsEngine = Depends()
):
    """
    Define automated tagging rules and policies.
    
    **Features:**
    - Integration with AWS Organizations for tag enforcement
    - Custom tagging strategies for different resource types
    - Policy compliance monitoring
    """
    try:
        rules_data = [rule.dict() for rule in request.rules]
        result = engine.allocation.create_tagging_rules(rules_data)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating tagging rules: {str(e)}")


@router.get("/allocation/third-party-integration")
async def get_third_party_integration_status(engine: FinOpsEngine = Depends()):
    """
    Support for external tagging tools (Terraform, CloudFormation).
    
    **Features:**
    - Integration with ITSM systems for automated cost allocation
    - API connectors for business systems (ERP, HRMS)
    - Data quality monitoring and sync status
    """
    try:
        result = engine.allocation.get_third_party_integration_status()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving integration status: {str(e)}")