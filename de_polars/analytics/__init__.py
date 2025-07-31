"""
DE Polars Analytics - Cost analytics and optimization modules
"""

from .kpi_summary import KPISummaryAnalytics
from .spend_analytics import SpendAnalytics  
from .optimization import OptimizationAnalytics
from .allocation import AllocationAnalytics
from .discounts import DiscountAnalytics
from .ai_recommendations import AIRecommendationAnalytics

__all__ = [
    "KPISummaryAnalytics",
    "SpendAnalytics",
    "OptimizationAnalytics", 
    "AllocationAnalytics",
    "DiscountAnalytics",
    "AIRecommendationAnalytics"
]