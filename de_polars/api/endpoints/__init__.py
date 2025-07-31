"""
FastAPI endpoint routers for all FinOps API modules
"""

from .kpi_endpoints import router as kpi_router
from .spend_endpoints import router as spend_router
from .optimization_endpoints import router as optimization_router
from .allocation_endpoints import router as allocation_router
from .discounts_endpoints import router as discounts_router
from .mcp_endpoints import router as mcp_router
from .ai_endpoints import router as ai_router

__all__ = [
    "kpi_router",
    "spend_router",
    "optimization_router", 
    "allocation_router",
    "discounts_router",
    "mcp_router",
    "ai_router"
]