"""
Data Engineering Utilities
==========================

A collection of production-ready utilities for data engineering workflows.
"""

__version__ = "1.0.0"

from .dollar_bars import DollarBars
from .resampling import Resampler
from .parquet_utils import optimize_parquet, read_partitioned_parquet
from .validation import validate_schema, check_data_quality

__all__ = [
    "DollarBars",
    "Resampler",
    "optimize_parquet",
    "read_partitioned_parquet",
    "validate_schema",
    "check_data_quality",
]

