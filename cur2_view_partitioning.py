"""
CUR2 View Partitioning Script (This is used to partition the default cur2_view table to make sure dependent sql files are run in sequent)

Processes SQL view files from cur2_views folder in dependency order:
1. Level 1: Independent views (can run in parallel)
2. Level 2: Dependent views (require Level 1 results)  
3. Level 3: Final views (require Level 1 & 2 results)

Saves all results as partitioned parquet files using DataPartitioner.
"""

import os
import glob
from pathlib import Path
from typing import List, Dict
from de_polars.client import DataExportsPolars
from de_polars.data_partitioner import DataPartitioner


class CUR2ViewPartitioner:
    """Enhanced partitioner for CUR2 views with dependency management."""
    
    def __init__(self, 
                 source_client: DataExportsPolars,
                 views_base_dir: str = "cur2_views",
                 output_base_dir: str = "cur2_view"):
        """
        Initialize CUR2 view partitioner.
        
        Args:
            source_client: DataExportsPolars client with CUR table registered
            views_base_dir: Base directory containing SQL view files (default: "cur2_views")
            output_base_dir: Output directory for parquet files (default: "cur2_view")
        """
        self.source_client = source_client
        self.views_base_dir = Path(views_base_dir)
        self.output_base_dir = Path(output_base_dir)
        self.partitioner = DataPartitioner(
            source_client=source_client,
            output_base_dir=str(output_base_dir),
            query_library_path=str(views_base_dir)
        )
        
        # Ensure output directory exists
        self.output_base_dir.mkdir(exist_ok=True)
        
        # Track processed views for dependency resolution
        self.processed_views: Dict[str, str] = {}
    
    def discover_view_files_by_level(self) -> Dict[str, List[str]]:
        """Discover SQL view files organized by dependency level."""
        levels = {}
        
        # Level 1: Independent views
        level1_files = glob.glob(str(self.views_base_dir / "level_1_independent" / "*.sql"))
        levels["level_1_independent"] = [
            f"level_1_independent/{Path(f).name}" for f in sorted(level1_files)
        ]
        
        # Level 2: Dependent views
        level2_files = glob.glob(str(self.views_base_dir / "level_2_dependent" / "*.sql"))
        levels["level_2_dependent"] = [
            f"level_2_dependent/{Path(f).name}" for f in sorted(level2_files)
        ]
        
        # Level 3: Final views
        level3_files = glob.glob(str(self.views_base_dir / "level_3_final" / "*.sql"))
        levels["level_3_final"] = [
            f"level_3_final/{Path(f).name}" for f in sorted(level3_files)
        ]
        
        return levels
    
    def get_view_name_from_file(self, sql_file_path: str) -> str:
        """Extract view name from SQL file path."""
        return Path(sql_file_path).stem
    
    def register_parquet_as_view(self, view_name: str, parquet_path: str):
        """Register a parquet file as a view in DuckDB for subsequent queries."""
        print(f"📋 Registering {view_name} from {parquet_path}")
        
        # Create view that reads from parquet file
        create_view_sql = f"""
        CREATE OR REPLACE VIEW {view_name} AS 
        SELECT * FROM read_parquet('{parquet_path}')
        """
        
        try:
            self.source_client.query(create_view_sql)
            print(f"✅ View {view_name} registered successfully")
        except Exception as e:
            print(f"❌ Failed to register view {view_name}: {str(e)}")
            raise
    
    def process_view_file(self, sql_file_path: str, level: str) -> str:
        """Process a single view file and register result for subsequent use."""
        view_name = self.get_view_name_from_file(sql_file_path)
        
        print(f"🔨 Processing {level}: {view_name}")
        print("=" * 60)
        
        # Run SQL file and save as parquet
        parquet_path = self.partitioner.run_sql_file(sql_file_path)
        
        # Convert to absolute path for DuckDB
        abs_parquet_path = os.path.abspath(parquet_path)
        
        # Register parquet result as view for dependent queries
        self.register_parquet_as_view(view_name, abs_parquet_path)
        
        # Track processed view
        self.processed_views[view_name] = abs_parquet_path
        
        print(f"✅ {view_name} completed and registered")
        print()
        
        return parquet_path
    
    def process_level(self, level_name: str, sql_files: List[str]) -> Dict[str, str]:
        """Process all SQL files in a dependency level."""
        print(f"🏭 Processing {level_name.upper()}")
        print("=" * 80)
        print(f"📁 Files to process: {len(sql_files)}")
        print()
        
        results = {}
        
        for i, sql_file in enumerate(sql_files, 1):
            print(f"[{i}/{len(sql_files)}] {sql_file}")
            
            try:
                parquet_path = self.process_view_file(sql_file, level_name)
                results[sql_file] = parquet_path
                
            except Exception as e:
                print(f"❌ Failed to process {sql_file}: {str(e)}")
                raise  # Stop processing on error since views are dependent
        
        print(f"🎉 {level_name.upper()} COMPLETED")
        print(f"   ✅ Processed: {len(results)} views")
        print(f"   📊 Total views registered: {len(self.processed_views)}")
        print()
        
        return results
    
    def run_all_views(self) -> Dict[str, Dict[str, str]]:
        """Run all view files in dependency order."""
        print("🚀 STARTING CUR2 VIEW PARTITIONING")
        print("=" * 80)
        print(f"📂 Source: {self.views_base_dir}")
        print(f"📦 Output: {self.output_base_dir}")
        print()
        
        # Discover view files by level
        levels = self.discover_view_files_by_level()
        
        # Print processing plan
        total_files = sum(len(files) for files in levels.values())
        print(f"📋 PROCESSING PLAN ({total_files} total views)")
        print("-" * 40)
        for level_name, files in levels.items():
            print(f"  {level_name}: {len(files)} views")
        print()
        
        # Process each level in dependency order
        all_results = {}
        
        # Level 1: Independent views (can run in any order)
        if "level_1_independent" in levels:
            all_results["level_1_independent"] = self.process_level(
                "level_1_independent", 
                levels["level_1_independent"]
            )
        
        # Level 2: Dependent views (require Level 1 results)
        if "level_2_dependent" in levels:
            all_results["level_2_dependent"] = self.process_level(
                "level_2_dependent", 
                levels["level_2_dependent"]
            )
        
        # Level 3: Final views (require Level 1 & 2 results)
        if "level_3_final" in levels:
            all_results["level_3_final"] = self.process_level(
                "level_3_final", 
                levels["level_3_final"]
            )
        
        print("🎊 ALL LEVELS COMPLETED!")
        print("=" * 80)
        print(f"📈 Summary:")
        print(f"   📊 Total views processed: {len(self.processed_views)}")
        print(f"   📂 Output directory: {self.output_base_dir}")
        print(f"   🗃️  Parquet files created: {total_files}")
        print()
        
        # List all generated files
        print("📋 Generated Views:")
        for view_name, parquet_path in self.processed_views.items():
            print(f"   ✅ {view_name} → {parquet_path}")
        
        return all_results


def main():
    """Main function to run CUR2 view partitioning."""
    print("🏗️  CUR2 View Partitioning Pipeline")
    print("=" * 50)
    
    # Initialize client (assuming CUR table is already registered)
    try:
        client = DataExportsPolars()
        print("✅ DataExportsPolars client initialized")
    except Exception as e:
        print(f"❌ Failed to initialize client: {str(e)}")
        print("💡 Make sure CUR table is registered in the client")
        return
    
    # Initialize partitioner
    partitioner = CUR2ViewPartitioner(client)
    
    # Run all views
    try:
        results = partitioner.run_all_views()
        print("✅ CUR2 view partitioning completed successfully!")
        
    except Exception as e:
        print(f"❌ CUR2 view partitioning failed: {str(e)}")
        raise


if __name__ == "__main__":
    main() 