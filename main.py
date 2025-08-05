#!/usr/bin/env python3
"""
Infralyzer FinOps API Server - Unified Startup Script
====================================================

Comprehensive AWS cost analytics platform with query engine support.

Usage:
    # Development (with auto-reload)
    python main.py
    
    # Production 
    uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4

Environment Variables (optional):
    FINOPS_S3_BUCKET       - S3 bucket containing cost data
    FINOPS_S3_PREFIX       - S3 prefix path to data files
    FINOPS_DATA_TYPE       - Data export type (default: CUR2.0)
    FINOPS_LOCAL_PATH      - Local data cache directory
    FINOPS_TABLE_NAME      - Main table name (default: CUR)

API Documentation:
    - Interactive docs: http://localhost:8000/docs
    - ReDoc: http://localhost:8000/redoc
    - Health check: http://localhost:8000/health

Query Endpoint Sample:
curl -X POST "http://localhost:8000/api/v1/finops/query" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM CUR LIMIT 1", "engine": "duckdb"}'
"""

import os
import sys
from infralyzer.api.fastapi_app import create_finops_app, create_finops_app_from_env


def check_dependencies():
    """Check if required packages are installed."""
    try:
        import fastapi
        import uvicorn
        import polars
        import duckdb
        print("✅ All dependencies installed")
        return True
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("Please run: pip install -r requirements.txt")
        return False


def get_app_config():
    """Get application configuration from environment or defaults."""
    # Try environment variables first
    env_vars = {
        'FINOPS_S3_BUCKET': os.getenv('FINOPS_S3_BUCKET'),
        'FINOPS_S3_PREFIX': os.getenv('FINOPS_S3_PREFIX'),
        'FINOPS_DATA_TYPE': os.getenv('FINOPS_DATA_TYPE', 'CUR2.0'),
        'FINOPS_LOCAL_PATH': os.getenv('FINOPS_LOCAL_PATH'),
        'FINOPS_TABLE_NAME': os.getenv('FINOPS_TABLE_NAME', 'CUR')
    }
    
    # Check if main required vars are set
    if env_vars['FINOPS_S3_BUCKET'] and env_vars['FINOPS_S3_PREFIX']:
        print("📋 Using environment configuration:")
        for key, value in env_vars.items():
            if value:
                print(f"   {key}: {value}")
        return 'env'
    
    # Use default configuration
    print("📋 Using default configuration:")
    print("   S3 Bucket: cid-014498620306-data-local")
    print("   S3 Prefix: cur2/014498620306/cid-cur2/data")
    print("   Data Type: CUR2.0")
    print("   Local Path: ./test_local_data")
    print("   Table Name: CUR")
    return 'default'


def create_app():
    """Create FastAPI application with appropriate configuration."""
    config_type = get_app_config()
    
    try:
        if config_type == 'env':
            # Create from environment variables
            app = create_finops_app_from_env()
            print("✅ FastAPI app created from environment variables")
        else:
            # Create with default configuration  
            app = create_finops_app(
                s3_bucket='cid-014498620306-data-local',
                s3_data_prefix='cur2/014498620306/cid-cur2/data',
                data_export_type='CUR2.0',
                local_data_path='./test_local_data',
                table_name='CUR'
            )
            print("✅ FastAPI app created with default configuration")
        
        return app
        
    except Exception as e:
        print(f"❌ Error creating FastAPI app: {e}")
        sys.exit(1)


def setup_startup_event(app):
    """Add startup event with comprehensive information."""
    @app.on_event("startup")
    async def startup_event():
        print("\n" + "="*70)
        print("🚀 INFRALYZER FINOPS API SERVER STARTED")
        print("="*70)
        print("📊 Comprehensive AWS Cost Analytics Platform")
        print("💾 Local data caching for cost optimization")
        print("🤖 AI-powered insights and recommendations")
        print("⚡ Multi-engine query support (DuckDB, Polars, Athena)")
        print("\n🌐 API Endpoints Available:")
        print("   📚 Interactive Docs: http://localhost:8000/docs")
        print("   📖 ReDoc: http://localhost:8000/redoc")
        print("   ❤️  Health Check: http://localhost:8000/health")
        print("\n🎯 Core Endpoints:")
        print("   🔍 Query Engine: POST /api/v1/finops/query")
        print("   🤖 Natural Language: POST /api/v1/finops/mcp/query")
        print("   📊 KPI Summary: GET /api/v1/finops/kpi/summary")
        print("   💰 Spend Analytics: GET /api/v1/finops/spend/invoice/summary")
        print("   🎯 Optimization: GET /api/v1/finops/optimization/idle-resources")
        print("="*70)


def start_development_server(app):
    """Start development server with auto-reload."""
    import uvicorn
    
    print("\n🏃 Starting development server...")
    print("   🔄 Auto-reload enabled")
    print("   🌐 Server: http://localhost:8000")
    print("   📚 Docs: http://localhost:8000/docs")
    print("   ⏹️  Press Ctrl+C to stop")
    print()
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )


def main():
    """Main application entry point."""
    print("🚀 INFRALYZER FINOPS API")
    print("=" * 50)
    
    # Check dependencies
    if not check_dependencies():
        sys.exit(1)
    
    # Create app
    global app
    app = create_app()
    
    # Setup startup event
    setup_startup_event(app)
    
    # Check if running directly (development) or via uvicorn (production)
    if __name__ == "__main__":
        start_development_server(app)


# Create the app instance for uvicorn
app = None

if __name__ == "__main__":
    main()
else:
    # When imported by uvicorn, create the app
    print("🚀 Creating Infralyzer FastAPI app...")
    if not check_dependencies():
        sys.exit(1)
    app = create_app()
    setup_startup_event(app)
    print("✅ App ready for production server")