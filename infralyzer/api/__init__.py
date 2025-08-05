"""
Infralyzer API - FastAPI integration for cost analytics endpoints
"""

from .fastapi_app import create_finops_app, FinOpsAPI
from .endpoints import (
    kpi_router,
    spend_router,
    optimization_router,
    allocation_router,
    discounts_router,
    mcp_router,
    ai_router
)

__all__ = [
    "create_finops_app",
    "FinOpsAPI",
    "kpi_router",
    "spend_router", 
    "optimization_router",
    "allocation_router",
    "discounts_router",
    "mcp_router",
    "ai_router"
]