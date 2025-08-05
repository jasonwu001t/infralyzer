"""
FastAPI dependencies for the FinOps API
"""
from fastapi import HTTPException
from ..finops_engine import FinOpsEngine


def get_finops_engine() -> FinOpsEngine:
    """Dependency to get FinOps engine instance."""
    # This will be overridden by the app instance
    raise HTTPException(status_code=500, detail="FinOps engine not configured")