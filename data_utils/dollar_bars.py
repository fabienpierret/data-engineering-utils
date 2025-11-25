"""
Dollar Bars Module
==================

Implements dollar bar sampling for financial data.
Dollar bars are time-independent and based on dollar volume.
"""

import polars as pl
import numpy as np
from typing import Optional


class DollarBars:
    """
    Create dollar bars from tick data.
    
    Dollar bars are sampled based on cumulative dollar volume,
    making them more robust than time-based bars.
    """
    
    def __init__(
        self,
        threshold: float = 100000.0,
        min_ticks: int = 1
    ):
        """
        Initialize dollar bars generator.
        
        Args:
            threshold: Dollar volume threshold per bar (e.g., $100k)
            min_ticks: Minimum number of ticks per bar
        """
        self.threshold = threshold
        self.min_ticks = min_ticks
    
    def create(
        self,
        data: pl.DataFrame,
        price_col: str = "price",
        volume_col: str = "volume",
        timestamp_col: str = "timestamp"
    ) -> pl.DataFrame:
        """
        Create dollar bars from tick data.
        
        Args:
            data: DataFrame with price, volume, timestamp columns
            price_col: Name of price column
            volume_col: Name of volume column
            timestamp_col: Name of timestamp column
            
        Returns:
            DataFrame with dollar bars (OHLCV format)
        """
        # Convert to numpy for efficient processing
        prices = data[price_col].to_numpy()
        volumes = data[volume_col].to_numpy()
        timestamps = data[timestamp_col].to_numpy()
        
        # Calculate dollar volume
        dollar_volumes = prices * volumes
        
        # Cumulative dollar volume
        cum_dollar_volume = np.cumsum(dollar_volumes)
        
        # Create bars
        bars = []
        current_bar_start = 0
        current_cum_dv = 0.0
        
        for i in range(len(data)):
            current_cum_dv = cum_dollar_volume[i] - cum_dollar_volume[current_bar_start]
            
            # Check if threshold reached
            if current_cum_dv >= self.threshold and (i - current_bar_start) >= self.min_ticks:
                # Create bar
                bar_data = data[current_bar_start:i+1]
                
                open_price = bar_data[price_col][0]
                high_price = bar_data[price_col].max()
                low_price = bar_data[price_col].min()
                close_price = bar_data[price_col][-1]
                volume = bar_data[volume_col].sum()
                dollar_volume = current_cum_dv
                timestamp = bar_data[timestamp_col][0]
                
                bars.append({
                    "timestamp": timestamp,
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "volume": volume,
                    "dollar_volume": dollar_volume
                })
                
                current_bar_start = i + 1
        
        # Create DataFrame
        if bars:
            return pl.DataFrame(bars)
        else:
            # Return empty DataFrame with correct schema
            return pl.DataFrame({
                "timestamp": pl.Datetime,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64,
                "dollar_volume": pl.Float64
            })
    
    def create_lazy(
        self,
        data: pl.LazyFrame,
        price_col: str = "price",
        volume_col: str = "volume"
    ) -> pl.LazyFrame:
        """
        Create dollar bars using LazyFrame (for large datasets).
        
        Args:
            data: LazyFrame with price and volume columns
            price_col: Name of price column
            volume_col: Name of volume column
            
        Returns:
            LazyFrame with dollar bars
        """
        # Calculate dollar volume
        data = data.with_columns([
            (pl.col(price_col) * pl.col(volume_col)).alias("dollar_volume")
        ])
        
        # Cumulative dollar volume
        data = data.with_columns([
            pl.col("dollar_volume").cumsum().alias("cum_dollar_volume")
        ])
        
        # Group by dollar volume threshold
        # Note: This is a simplified version. Full implementation would
        # require window functions and grouping logic.
        
        return data

