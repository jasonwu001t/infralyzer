"""
Enhanced Data Partitioner with SQL Query Library Support

Supports SQL query execution from cur2_query_library and saves results as local parquet files.
Can create partitioned folders and parquet files based on SQL queries.
"""

import polars as pl
import os
from pathlib import Path
from typing import Optional, Dict, List
from .client import DataExportsPolars


class DataPartitioner:
    """Enhanced data partitioner with SQL query library support."""
    
    def __init__(self, 
                 source_client: Optional[DataExportsPolars] = None,
                 output_base_dir: str = "cur2_data",
                 query_library_path: str = "cur2_query_library"):
        """
        Initialize the enhanced data partitioner.
        
        Args:
            source_client: DataExportsPolars client for source data (optional for file listing)
            output_base_dir: Base directory for storing parquet files (default: "cur2_data")
            query_library_path: Path to SQL query library (default: "cur2_query_library")
        """
        self.source_client = source_client
        self.output_base_dir = Path(output_base_dir)
        self.query_library_path = Path(query_library_path)
        
        # Ensure output directory exists (only if we have a real output dir)
        if output_base_dir != "temp":
            self.output_base_dir.mkdir(exist_ok=True)
    
    def _save_to_parquet(self, dataframe: pl.DataFrame, output_path: Path) -> str:
        """Save dataframe to local parquet file."""
        # Ensure parent directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write to parquet with snappy compression
        dataframe.write_parquet(output_path, compression='snappy')
        
        # Calculate file size
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        
        print(f"✅ Saved: {len(dataframe):,} rows, {file_size_mb:.1f}MB")
        print(f"   📂 Path: {output_path}")
        
        return str(output_path)
    
    def discover_sql_files(self) -> Dict[str, List[str]]:
        """Discover all SQL files in the query library organized by category."""
        import glob
        
        categories = {}
        
        if not self.query_library_path.exists():
            print(f"❌ Query library not found: {self.query_library_path}")
            return categories
        
        # Find all SQL files recursively
        sql_files = glob.glob(str(self.query_library_path / "**" / "*.sql"), recursive=True)
        
        for sql_file in sql_files:
            # Get relative path from query library root
            rel_path = Path(sql_file).relative_to(self.query_library_path)
            category = str(rel_path.parent)
            filename = str(rel_path)
            
            if category not in categories:
                categories[category] = []
            categories[category].append(filename)
        
        return categories
    
    def load_sql_query(self, query_path: str) -> str:
        """Load SQL query from file."""
        full_path = self.query_library_path / query_path
        
        if not full_path.exists():
            raise FileNotFoundError(f"SQL file not found: {full_path}")
        
        with open(full_path, 'r') as file:
            return file.read()
    
    def extract_query_metadata(self, sql_content: str) -> Dict[str, str]:
        """Extract metadata from SQL file comments."""
        lines = sql_content.split('\n')
        metadata = {}
        
        for line in lines:
            line = line.strip()
            if line.startswith('-- Description:'):
                metadata['description'] = line.replace('-- Description:', '').strip()
            elif line.startswith('-- Partitioning:'):
                metadata['partitioning'] = line.replace('-- Partitioning:', '').strip()
            elif line.startswith('-- Output:'):
                metadata['output'] = line.replace('-- Output:', '').strip()
        
        return metadata
    
    def run_sql_file(self, sql_file_path: str) -> str:
        """
        Run a SQL file and save results as parquet.
        
        Args:
            sql_file_path: Relative path to SQL file (e.g., 'analytics/amazon_athena.sql')
        
        Returns:
            Path to created parquet file
        """
        if self.source_client is None:
            raise ValueError("Source client is required for running SQL files")
            
        print(f"🔨 Running SQL File: {sql_file_path}")
        print("=" * 60)
        
        # Load and execute SQL query
        sql_content = self.load_sql_query(sql_file_path)
        metadata = self.extract_query_metadata(sql_content)
        
        if 'description' in metadata:
            print(f"📝 Description: {metadata['description']}")
        
        print(f"📄 Query: {len(sql_content)} characters")
        print()
        
        # Execute query
        print("⚡ Executing query...")
        result_df = self.source_client.query(sql_content)
        print(f"✅ Query completed: {len(result_df):,} rows × {len(result_df.columns)} columns")
        print()
        
        # Create output path maintaining directory structure
        sql_path = Path(sql_file_path)
        output_path = self.output_base_dir / sql_path.parent / f"{sql_path.stem}.parquet"
        
        # Save to parquet
        return self._save_to_parquet(result_df, output_path)
    
    def run_sql_files(self, sql_file_paths: List[str]) -> Dict[str, str]:
        """
        Run multiple SQL files and save results as parquet files.
        
        Args:
            sql_file_paths: List of relative paths to SQL files
        
        Returns:
            Dict mapping SQL file paths to output parquet paths
        """
        if self.source_client is None:
            raise ValueError("Source client is required for running SQL files")
            
        print(f"🏭 Running {len(sql_file_paths)} SQL Files")
        print("=" * 80)
        print()
        
        results = {}
        
        for i, sql_file_path in enumerate(sql_file_paths, 1):
            print(f"[{i}/{len(sql_file_paths)}] Processing: {sql_file_path}")
            
            try:
                output_path = self.run_sql_file(sql_file_path)
                results[sql_file_path] = output_path
                print()
                
            except Exception as e:
                print(f"❌ Failed to process {sql_file_path}: {str(e)}")
                print()
        
        print(f"🎉 Summary:")
        print(f"   ✅ Successful: {len(results)}")
        print(f"   ❌ Failed: {len(sql_file_paths) - len(results)}")
        print()
        
        return results
    
    def list_available_sql_files(self):
        """List all SQL files and their potential analytics tables."""
        categories = self.discover_sql_files()
        
        print(f"📊 Available SQL Files")
        print("=" * 60)
        
        if not categories:
            print("❌ No SQL queries found in cur2_query_library!")
            return
        
        total_queries = 0
        for category, files in sorted(categories.items()):
            print(f"\n📁 {category}/ ({len(files)} files)")
            
            for query_file in sorted(files):
                table_name = Path(query_file).stem
                
                print(f"   📊 {table_name}")
                print(f"      📄 Source: {query_file}")
                
                # Try to read description
                try:
                    sql_content = self.load_sql_query(query_file)
                    metadata = self.extract_query_metadata(sql_content)
                    if 'description' in metadata:
                        print(f"      💡 {metadata['description']}")
                except:
                    pass
                
                total_queries += 1
        
        print(f"\n📈 Total SQL Files Available: {total_queries} files across {len(categories)} categories") 