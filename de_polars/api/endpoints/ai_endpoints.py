"""
AI Recommendations API endpoints - View 6: AI-Powered Cost Recommendations
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Dict, Any, Optional
from pydantic import BaseModel

from ...finops_engine import FinOpsEngine


router = APIRouter()


@router.get("/ai/anomaly-detection")
async def detect_anomalies(
    lookback_days: int = Query(30, description="Number of days to analyze for anomalies"),
    sensitivity: float = Query(2.0, description="Sensitivity threshold for anomaly detection"),
    engine: FinOpsEngine = Depends()
):
    """
    Machine learning-based spend anomaly detection.
    
    **Features:**
    - Root cause analysis for cost spikes
    - Predictive alerts for budget overruns
    - Confidence scoring for anomalies
    """
    try:
        result = engine.ai.detect_anomalies(
            lookback_days=lookback_days,
            sensitivity=sensitivity
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error detecting anomalies: {str(e)}")


@router.get("/ai/optimization-insights")
async def get_optimization_insights(engine: FinOpsEngine = Depends()):
    """
    AI-generated cost optimization recommendations.
    
    **Features:**
    - Pattern recognition across historical data
    - Industry benchmark comparisons
    - ML-powered optimization scoring
    """
    try:
        result = engine.ai.get_optimization_insights()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating optimization insights: {str(e)}")


class CustomAnalysisRequest(BaseModel):
    """Model for custom AI analysis requests."""
    query_text: str
    analysis_type: str = "cost_analysis"


@router.post("/ai/custom-analysis")
async def analyze_custom_query(
    request: CustomAnalysisRequest,
    engine: FinOpsEngine = Depends()
):
    """
    Natural language interface for cost analysis.
    
    **Features:**
    - AI-powered data exploration and insights
    - Automated report generation
    - Natural language to SQL conversion
    """
    try:
        result = engine.ai.analyze_custom_query(
            query_text=request.query_text,
            analysis_type=request.analysis_type
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error analyzing custom query: {str(e)}")


@router.get("/ai/forecasting")
async def get_forecasting(
    forecast_months: int = Query(6, description="Number of months to forecast"),
    engine: FinOpsEngine = Depends()
):
    """
    Machine learning cost forecasting models.
    
    **Features:**
    - Scenario planning and what-if analysis
    - Business impact predictions
    - ML-powered trend analysis
    """
    try:
        result = engine.ai.get_forecasting(forecast_months=forecast_months)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating forecasting: {str(e)}")