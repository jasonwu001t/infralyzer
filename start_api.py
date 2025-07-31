#!/usr/bin/env python3
"""
Quick Start Script for DE-Polars FinOps API

This script helps you get started quickly with the FinOps API server.
It will guide you through configuration and start the server.

Usage:
    python start_api.py
"""

import os
import sys

def check_environment():
    """Check if required environment variables are set."""
    required_vars = ['FINOPS_S3_BUCKET', 'FINOPS_S3_PREFIX']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print("❌ Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\n🔧 Please set these environment variables or create a .env file")
        print("📋 Example:")
        print("   export FINOPS_S3_BUCKET=my-cost-data-bucket")
        print("   export FINOPS_S3_PREFIX=cur2/cur2/data")
        print("   export FINOPS_DATA_TYPE=CUR2.0")
        print("   export FINOPS_LOCAL_PATH=./local_data")
        return False
    
    return True

def setup_local_data():
    """Offer to set up local data caching."""
    local_path = os.getenv('FINOPS_LOCAL_PATH', './local_data')
    
    if not os.path.exists(local_path):
        print(f"\n💾 Local data directory '{local_path}' not found.")
        response = input("📥 Would you like to create it and download data locally? (y/N): ")
        
        if response.lower() == 'y':
            os.makedirs(local_path, exist_ok=True)
            print(f"✅ Created local data directory: {local_path}")
            
            print("\n🔄 To download S3 data locally (one-time setup):")
            print("   from de_polars import FinOpsEngine, DataConfig, DataExportType")
            print("   engine = FinOpsEngine.from_s3_config(...)")
            print("   engine.download_data_locally()")
            print("\n💡 This eliminates future S3 query costs!")
        else:
            print("ℹ️  Continuing with S3-only data access")

def start_server():
    """Start the FastAPI server."""
    print("\n🚀 Starting DE-Polars FinOps API Server...")
    
    try:
        import uvicorn
        from main import app
        
        print("📊 Server starting on http://localhost:8000")
        print("📖 API documentation: http://localhost:8000/docs")
        print("🏥 Health check: http://localhost:8000/health")
        print("\n⏹️  Press Ctrl+C to stop the server")
        
        uvicorn.run(
            "main:app",
            host="0.0.0.0",
            port=8000,
            reload=True,
            log_level="info"
        )
        
    except ImportError:
        print("❌ Required packages not installed.")
        print("🔧 Please run: pip install -r requirements.txt")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error starting server: {e}")
        sys.exit(1)

def main():
    """Main startup flow."""
    print("🎯 DE-POLARS FINOPS API QUICK START")
    print("=" * 50)
    
    # Check dependencies
    try:
        import fastapi
        import uvicorn
        import polars
        import duckdb
        print("✅ All dependencies installed")
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("🔧 Please run: pip install -r requirements.txt")
        sys.exit(1)
    
    # Check environment
    if not check_environment():
        print("\n📋 Example configuration:")
        print("   Create a .env file or set environment variables:")
        print("   FINOPS_S3_BUCKET=my-cost-data-bucket")
        print("   FINOPS_S3_PREFIX=cur2/cur2/data")
        print("   FINOPS_DATA_TYPE=CUR2.0")
        print("   FINOPS_LOCAL_PATH=./local_data")
        sys.exit(1)
    
    # Show configuration
    print("✅ Configuration detected:")
    print(f"   S3 Bucket: {os.getenv('FINOPS_S3_BUCKET')}")
    print(f"   S3 Prefix: {os.getenv('FINOPS_S3_PREFIX')}")
    print(f"   Data Type: {os.getenv('FINOPS_DATA_TYPE', 'CUR2.0')}")
    print(f"   Local Path: {os.getenv('FINOPS_LOCAL_PATH', './local_data')}")
    
    # Setup local data
    setup_local_data()
    
    # Start server
    start_server()

if __name__ == "__main__":
    main()