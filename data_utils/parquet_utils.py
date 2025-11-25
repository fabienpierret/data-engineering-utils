"""
Parquet Utilities Module
========================

Utilities for optimizing Parquet file storage and reading.
"""

import polars as pl
from pathlib import Path
from typing import Optional, Union, List


def optimize_parquet(
    input_path: Union[str, Path],
    output_path: Union[str, Path],
    compression: str = "zstd",
    compression_level: int = 3,
    partition_by: Optional[str] = None,
    row_group_size: int = 100000
):
    """
    Optimize Parquet file for storage and performance.
    
    Args:
        input_path: Path to input Parquet file or directory
        output_path: Path to output Parquet file or directory
        compression: Compression codec ("zstd", "snappy", "gzip", "lz4")
        compression_level: Compression level (1-9 for zstd)
        partition_by: Column name for partitioning (optional)
        row_group_size: Row group size in rows
    """
    input_path = Path(input_path)
    output_path = Path(output_path)
    
    # Read data
    if input_path.is_dir():
        data = pl.read_parquet(input_path / "*.parquet")
    else:
        data = pl.read_parquet(input_path)
    
    # Write optimized Parquet
    if partition_by:
        # Partitioned write
        data.write_parquet(
            output_path,
            compression=compression,
            use_pyarrow=True,
            row_group_size=row_group_size
        )
    else:
        # Single file write
        data.write_parquet(
            output_path,
            compression=compression,
            row_group_size=row_group_size
        )


def read_partitioned_parquet(
    path: Union[str, Path],
    lazy: bool = True
) -> Union[pl.DataFrame, pl.LazyFrame]:
    """
    Read partitioned Parquet files efficiently.
    
    Args:
        path: Path to Parquet directory
        lazy: Use LazyFrame for memory efficiency
        
    Returns:
        DataFrame or LazyFrame
    """
    path = Path(path)
    
    if not path.exists():
        raise FileNotFoundError(f"Path not found: {path}")
    
    if lazy:
        return pl.scan_parquet(path / "*.parquet")
    else:
        return pl.read_parquet(path / "*.parquet")


def get_parquet_info(path: Union[str, Path]) -> dict:
    """
    Get information about Parquet file(s).
    
    Args:
        path: Path to Parquet file or directory
        
    Returns:
        Dictionary with file information
    """
    path = Path(path)
    
    if path.is_dir():
        files = list(path.glob("*.parquet"))
        total_size = sum(f.stat().st_size for f in files)
        
        # Read one file to get schema
        sample = pl.read_parquet(files[0])
        
        return {
            "type": "partitioned",
            "num_files": len(files),
            "total_size_mb": total_size / (1024 * 1024),
            "schema": sample.schema,
            "num_rows": sum(pl.read_parquet(f).height for f in files)
        }
    else:
        file_size = path.stat().st_size
        data = pl.read_parquet(path)
        
        return {
            "type": "single_file",
            "size_mb": file_size / (1024 * 1024),
            "schema": data.schema,
            "num_rows": data.height,
            "num_columns": data.width
        }

