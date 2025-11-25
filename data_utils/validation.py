"""
Data Validation Module
=======================

Utilities for validating data schemas and quality.
"""

import polars as pl
from typing import Dict, Optional, List
import numpy as np


def validate_schema(
    data: pl.DataFrame,
    expected_schema: Dict[str, pl.DataType]
) -> tuple[bool, Optional[str]]:
    """
    Validate DataFrame schema against expected schema.
    
    Args:
        data: DataFrame to validate
        expected_schema: Dictionary mapping column names to expected types
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    actual_schema = data.schema
    
    # Check all expected columns exist
    for col, expected_type in expected_schema.items():
        if col not in actual_schema:
            return False, f"Missing column: {col}"
        
        actual_type = actual_schema[col]
        if actual_type != expected_type:
            return False, f"Type mismatch for {col}: expected {expected_type}, got {actual_type}"
    
    return True, None


def check_data_quality(
    data: pl.DataFrame,
    check_missing: bool = True,
    check_duplicates: bool = True,
    check_outliers: bool = False
) -> Dict:
    """
    Perform data quality checks.
    
    Args:
        data: DataFrame to check
        check_missing: Check for missing values
        check_duplicates: Check for duplicate rows
        check_outliers: Check for outliers (using IQR method)
        
    Returns:
        Dictionary with quality report
    """
    report = {
        "num_rows": data.height,
        "num_columns": data.width,
        "columns": list(data.columns)
    }
    
    # Check missing values
    if check_missing:
        missing = data.null_count()
        report["missing_values"] = {
            col: count for col, count in zip(missing.columns, missing.row(0))
        }
        report["total_missing"] = sum(report["missing_values"].values())
    
    # Check duplicates
    if check_duplicates:
        num_duplicates = data.height - data.unique().height
        report["num_duplicates"] = num_duplicates
        report["duplicate_percentage"] = (num_duplicates / data.height * 100) if data.height > 0 else 0.0
    
    # Check outliers (for numeric columns)
    if check_outliers:
        outliers = {}
        for col in data.columns:
            if data[col].dtype in [pl.Float64, pl.Int64, pl.Float32, pl.Int32]:
                values = data[col].drop_nulls().to_numpy()
                if len(values) > 0:
                    q1 = np.percentile(values, 25)
                    q3 = np.percentile(values, 75)
                    iqr = q3 - q1
                    lower_bound = q1 - 1.5 * iqr
                    upper_bound = q3 + 1.5 * iqr
                    
                    num_outliers = np.sum((values < lower_bound) | (values > upper_bound))
                    outliers[col] = num_outliers
        
        report["outliers"] = outliers
    
    return report

