"""
Test 10: FastAPI Endpoints with Local Parquet Files
===================================================

This test demonstrates how to create FastAPI endpoints that serve data from local parquet files.
Requires local data to be downloaded first (run test_2_download_local.py).
"""

import sys
import os
# Add parent directory to path to import local de_polars module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from infralyzer import FinOpsEngine, DataConfig, DataExportType
import uvicorn
from typing import Optional, Dict, Any

# Global engine instance
engine = None

def initialize_engine():
    """Initialize the FinOps engine with local data"""
    global engine
    
    local_path = "./test_local_data"
    
    # Check if local data exists
    if not os.path.exists(local_path):
        raise RuntimeError(f"Local data not found at {local_path}. Please run test_2_download_local.py first.")
    
    config = DataConfig(
        s3_bucket='billing-data-exports-cur',          
        s3_data_prefix='cur2/cur2/data',          
        data_export_type=DataExportType.CUR_2_0,               
        table_name='CUR',                        
        date_start='2025-07',                    
        date_end='2025-07',
        local_data_path=local_path,
        prefer_local_data=True
    )
    
    engine = FinOpsEngine(config)
    print(f"Engine initialized with local data at {local_path}")

# Create FastAPI app
app = FastAPI(
    title="FinOps Cost Analytics API",
    description="Simple API endpoints for AWS cost data using local parquet files",
    version="1.0.0"
)

@app.on_event("startup")
async def startup_event():
    """Initialize engine on startup"""
    try:
        initialize_engine()
        print("FastAPI server started with local parquet data")
    except Exception as e:
        print(f"Failed to initialize engine: {str(e)}")

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "FinOps Cost Analytics API",
        "status": "active",
        "data_source": "local_parquet_files",
        "endpoints": [
            "/summary",
            "/services",
            "/daily-costs",
            "/search-service/{service_name}"
        ]
    }

@app.get("/summary")
async def get_cost_summary():
    """Get overall cost summary"""
    try:
        if not engine:
            raise HTTPException(status_code=500, detail="Engine not initialized")
        
        result = engine.query("""
            SELECT 
                COUNT(*) as total_line_items,
                SUM(line_item_unblended_cost) as total_cost,
                COUNT(DISTINCT product_servicecode) as unique_services,
                COUNT(DISTINCT line_item_usage_start_date) as unique_days,
                AVG(line_item_unblended_cost) as avg_line_item_cost
            FROM CUR
        """)
        
        row = result.row(0, named=True)
        return {
            "summary": {
                "total_line_items": row["total_line_items"],
                "total_cost": round(row["total_cost"], 2),
                "unique_services": row["unique_services"],
                "unique_days": row["unique_days"],
                "avg_line_item_cost": round(row["avg_line_item_cost"], 4)
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")

@app.get("/services")
async def get_top_services(limit: int = 10):
    """Get top services by cost"""
    try:
        if not engine:
            raise HTTPException(status_code=500, detail="Engine not initialized")
        
        result = engine.query(f"""
            SELECT 
                product_servicecode as service,
                SUM(line_item_unblended_cost) as total_cost,
                COUNT(*) as line_items,
                AVG(line_item_unblended_cost) as avg_cost
            FROM CUR 
            GROUP BY product_servicecode 
            ORDER BY total_cost DESC 
            LIMIT {limit}
        """)
        
        services = []
        for row in result.iter_rows(named=True):
            services.append({
                "service": row["service"],
                "total_cost": round(row["total_cost"], 2),
                "line_items": row["line_items"],
                "avg_cost": round(row["avg_cost"], 4)
            })
        
        return {"services": services}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")

@app.get("/daily-costs")
async def get_daily_costs(limit: int = 30):
    """Get daily cost breakdown"""
    try:
        if not engine:
            raise HTTPException(status_code=500, detail="Engine not initialized")
        
        result = engine.query(f"""
            SELECT 
                line_item_usage_start_date as date,
                SUM(line_item_unblended_cost) as daily_cost,
                COUNT(*) as line_items,
                COUNT(DISTINCT product_servicecode) as unique_services
            FROM CUR 
            GROUP BY line_item_usage_start_date 
            ORDER BY line_item_usage_start_date
            LIMIT {limit}
        """)
        
        daily_costs = []
        for row in result.iter_rows(named=True):
            daily_costs.append({
                "date": str(row["date"]),
                "daily_cost": round(row["daily_cost"], 2),
                "line_items": row["line_items"],
                "unique_services": row["unique_services"]
            })
        
        return {"daily_costs": daily_costs}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")

@app.get("/search-service/{service_name}")
async def search_service(service_name: str):
    """Search for specific service details"""
    try:
        if not engine:
            raise HTTPException(status_code=500, detail="Engine not initialized")
        
        result = engine.query(f"""
            SELECT 
                product_servicecode as service,
                line_item_usage_type as usage_type,
                SUM(line_item_unblended_cost) as total_cost,
                COUNT(*) as line_items,
                MIN(line_item_usage_start_date) as first_usage,
                MAX(line_item_usage_start_date) as last_usage
            FROM CUR 
            WHERE LOWER(product_servicecode) LIKE '%{service_name.lower()}%'
            GROUP BY product_servicecode, line_item_usage_type
            ORDER BY total_cost DESC
            LIMIT 20
        """)
        
        if len(result) == 0:
            raise HTTPException(status_code=404, detail=f"No service found matching '{service_name}'")
        
        service_details = []
        for row in result.iter_rows(named=True):
            service_details.append({
                "service": row["service"],
                "usage_type": row["usage_type"],
                "total_cost": round(row["total_cost"], 2),
                "line_items": row["line_items"],
                "first_usage": str(row["first_usage"]),
                "last_usage": str(row["last_usage"])
            })
        
        return {
            "search_term": service_name,
            "results_count": len(service_details),
            "service_details": service_details
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")

def test_fastapi_endpoints():
    """Test function to start the FastAPI server"""
    
    print("Test 4: FastAPI Endpoints with Local Parquet Files")
    print("=" * 60)
    
    # Check if local data exists
    local_path = "./test_local_data"
    if not os.path.exists(local_path):
        print(f"Local data not found at {local_path}")
        print("Please run test_2_download_local.py first to download data")
        return False
    
    print("Starting FastAPI server...")
    print("📍 Server will be available at: http://localhost:8000")
    print("\n🔗 Available endpoints:")
    print("   • GET /                     - API information")
    print("   • GET /summary              - Overall cost summary")
    print("   • GET /services?limit=10    - Top services by cost")
    print("   • GET /daily-costs?limit=30 - Daily cost breakdown")
    print("   • GET /search-service/{service_name} - Search specific service")
    print("\n📖 API documentation available at: http://localhost:8000/docs")
    print("\nPress Ctrl+C to stop the server")
    print("=" * 60)
    
    try:
        # Start the server
        uvicorn.run(app, host="127.0.0.1", port=8000, log_level="info")
        return True
        
    except KeyboardInterrupt:
        print("\n🛑 Server stopped by user")
        return True
    except Exception as e:
        print(f"Test 4 FAILED: {str(e)}")
        return False

if __name__ == "__main__":
    test_fastapi_endpoints()