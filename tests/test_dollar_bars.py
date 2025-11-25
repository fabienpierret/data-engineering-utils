"""
Tests for dollar bars module
"""

import pytest
import polars as pl
import pandas as pd
from data_utils.dollar_bars import DollarBars


class TestDollarBars:
    """Test cases for DollarBars"""
    
    def test_create_dollar_bars(self):
        """Test creating dollar bars"""
        # Create sample tick data
        tick_data = pl.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=100, freq='1min'),
            'price': [100 + i * 0.1 for i in range(100)],
            'volume': [1000 + i * 10 for i in range(100)]
        })
        
        dollar_bars = DollarBars(threshold=50000)  # $50k per bar
        bars = dollar_bars.create(tick_data)
        
        assert bars is not None
        assert isinstance(bars, pl.DataFrame)
    
    def test_dollar_bars_threshold(self):
        """Test dollar bars with different threshold"""
        tick_data = pl.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=50, freq='1min'),
            'price': [100.0] * 50,
            'volume': [1000] * 50
        })
        
        dollar_bars = DollarBars(threshold=100000)  # $100k per bar
        bars = dollar_bars.create(tick_data)
        
        assert bars is not None
        # Should have fewer bars with higher threshold
        assert len(bars) <= len(tick_data)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

