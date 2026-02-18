"""
Data Exploration Script
Examines the structure of Overture Maps metrics data
"""

import pandas as pd
import os
from pathlib import Path

def explore_metrics():
    """Explore all metrics files and document structure"""
    
    metrics_path = Path("data/metrics")
    
    print("=" * 60)
    print("OVERTURE MAPS METRICS DATA EXPLORATION")
    print("=" * 60)
    
    # Find all parquet files
    parquet_files = list(metrics_path.rglob("*.parquet"))
    
    print(f"\nFound {len(parquet_files)} parquet files\n")
    
    for file_path in parquet_files:
        print(f"\nFile: {file_path}")
        try:
            df = pd.read_parquet(file_path)
            print(f"Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
            print(f"Columns: {', '.join(df.columns.tolist())}")
            print(f"\nSample data:")
            print(df.head(3))
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    explore_metrics()
