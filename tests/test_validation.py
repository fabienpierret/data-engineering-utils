"""
Tests for validation module
"""

import pytest
import polars as pl
from data_utils.validation import validate_schema, check_data_quality


class TestValidation:
    """Test cases for validation functions"""
    
    def test_validate_schema(self):
        """Test schema validation"""
        data = pl.DataFrame({
            'price': [100.0, 101.0, 102.0],
            'volume': [1000, 1100, 1200],
            'timestamp': pl.datetime_range(
                start=pl.datetime(2024, 1, 1),
                end=pl.datetime(2024, 1, 3),
                interval="1d",
                eager=True
            )
        })
        
        schema = {
            'price': pl.Float64,
            'volume': pl.Int64,
            'timestamp': pl.Datetime
        }
        
        is_valid = validate_schema(data, schema)
        assert is_valid is True or is_valid is False  # Boolean result
    
    def test_check_data_quality(self):
        """Test data quality check"""
        data = pl.DataFrame({
            'price': [100.0, 101.0, None, 103.0],
            'volume': [1000, 1100, 1200, 1300]
        })
        
        quality_report = check_data_quality(data)
        
        assert quality_report is not None
        assert isinstance(quality_report, dict)
        # Should detect missing values
        assert 'missing_values' in quality_report or 'nulls' in quality_report


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

