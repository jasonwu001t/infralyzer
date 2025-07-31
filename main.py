"""
FastAPI FinOps Cost Analytics Server

Quick start script for running the DE-Polars FinOps API server.
This creates a production-ready REST API with all cost analytics endpoints.

Usage:
    # Development
    uvicorn main:app --reload --host 0.0.0.0 --port 8000
    
    # Production  
    uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4

API Documentation:
    - Interactive docs: http://localhost:8000/docs
    - ReDoc: http://localhost:8000/redoc
    - Health check: http://localhost:8000/health
"""

import os
from de_polars.api import create_finops_app, create_finops_app_from_env

# Option 1: Create app from environment variables
# Set these environment variables before running:
# export FINOPS_S3_BUCKET=my-cost-data-bucket
# export FINOPS_S3_PREFIX=cur2/cur2/data  
# export FINOPS_DATA_TYPE=CUR2.0
# export FINOPS_LOCAL_PATH=./local_data

try:
    # Try to create from environment first
    app = create_finops_app_from_env()
    print("✅ FastAPI app created from environment variables")
except ValueError as e:
    print(f"⚠️  Environment variables not set: {e}")
    print("🔧 Creating app with default configuration...")
    
    # Option 2: Fallback to direct configuration
    app = create_finops_app(
        s3_bucket=os.getenv('FINOPS_S3_BUCKET', 'your-cost-data-bucket'),
        s3_data_prefix=os.getenv('FINOPS_S3_PREFIX', 'cur2/cur2/data'),
        data_export_type=os.getenv('FINOPS_DATA_TYPE', 'CUR2.0'),
        local_data_path=os.getenv('FINOPS_LOCAL_PATH', './local_data'),
        table_name=os.getenv('FINOPS_TABLE_NAME', 'CUR')
    )
    print("✅ FastAPI app created with default configuration")

# Add startup message
@app.on_event("startup")
async def startup_event():
    print("\n" + "="*70)
    print("🚀 DE-POLARS FINOPS API SERVER STARTED")
    print("="*70)
    print("📊 Comprehensive Cost Analytics Platform")
    print("💾 Local data caching for cost optimization")
    print("🤖 AI-powered insights and recommendations")
    print("📈 Real-time cost monitoring and alerts")
    print("\n🌐 API Endpoints Available:")
    print("   📖 Interactive Docs: http://localhost:8000/docs")
    print("   📝 ReDoc: http://localhost:8000/redoc") 
    print("   🏥 Health Check: http://localhost:8000/health")
    print("   ⭐ KPI Summary: http://localhost:8000/api/v1/finops/kpi/summary")
    print("   💰 Spend Analytics: http://localhost:8000/api/v1/finops/spend/invoice/summary")
    print("   ⚡ Optimization: http://localhost:8000/api/v1/finops/optimization/idle-resources")
    print("="*70)

if __name__ == "__main__":
    import uvicorn
    
    print("🔧 Starting development server...")
    print("💡 For production, use: uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4")
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0", 
        port=8000,
        reload=True,
        log_level="info"
    )