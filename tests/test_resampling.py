"""
Tests for resampling module
"""

import pytest
import polars as pl
import pandas as pd
from data_utils.resampling import Resampler


class TestResampler:
    """Test cases for Resampler"""
    
    def test_resample_hourly(self):
        """Test resampling to hourly bars"""
        data = pl.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=100, freq='5min'),
            'price': [100 + i * 0.1 for i in range(100)],
            'volume': [1000 + i * 10 for i in range(100)]
        })
        
        resampler = Resampler(interval="1h")
        hourly_data = resampler.resample(
            data,
            price_col="price",
            volume_col="volume"
        )
        
        assert hourly_data is not None
        assert isinstance(hourly_data, pl.DataFrame)
        # Should have fewer rows (hourly vs 5-min)
        assert len(hourly_data) <= len(data)
    
    def test_resample_daily(self):
        """Test resampling to daily bars"""
        data = pl.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=100, freq='1h'),
            'price': [100.0] * 100,
            'volume': [1000] * 100
        })
        
        resampler = Resampler(interval="1d")
        daily_data = resampler.resample(
            data,
            price_col="price",
            volume_col="volume"
        )
        
        assert daily_data is not None
        assert len(daily_data) <= len(data)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

