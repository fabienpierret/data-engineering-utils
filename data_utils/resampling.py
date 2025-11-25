"""
Data Resampling Module
======================

Efficient time-series resampling using Polars.
"""

import polars as pl
from typing import Optional, Union
from datetime import timedelta


class Resampler:
    """
    Resample time-series data to different intervals.
    
    Supports:
    - Downsampling (e.g., 1min → 1hour)
    - Upsampling (e.g., 1hour → 1min)
    - Aggregation (OHLCV, mean, sum, etc.)
    """
    
    def __init__(self, interval: Union[str, timedelta]):
        """
        Initialize resampler.
        
        Args:
            interval: Target interval (e.g., "1h", "1d", "5min")
        """
        self.interval = interval
    
    def resample(
        self,
        data: pl.DataFrame,
        timestamp_col: str = "timestamp",
        price_col: str = "price",
        volume_col: Optional[str] = None,
        method: str = "ohlcv"
    ) -> pl.DataFrame:
        """
        Resample data to target interval.
        
        Args:
            data: Input DataFrame
            timestamp_col: Name of timestamp column
            price_col: Name of price column
            volume_col: Name of volume column (optional)
            method: Resampling method ("ohlcv", "mean", "sum", "last")
            
        Returns:
            Resampled DataFrame
        """
        # Ensure timestamp is datetime
        if data[timestamp_col].dtype != pl.Datetime:
            data = data.with_columns([
                pl.col(timestamp_col).str.to_datetime()
            ])
        
        # Group by interval
        data = data.sort(timestamp_col)
        
        if method == "ohlcv":
            # OHLCV resampling
            result = data.group_by_dynamic(
                timestamp_col,
                every=self.interval
            ).agg([
                pl.col(price_col).first().alias("open"),
                pl.col(price_col).max().alias("high"),
                pl.col(price_col).min().alias("low"),
                pl.col(price_col).last().alias("close"),
                pl.col(volume_col).sum().alias("volume") if volume_col else pl.lit(None).alias("volume")
            ])
        elif method == "mean":
            result = data.group_by_dynamic(
                timestamp_col,
                every=self.interval
            ).agg([
                pl.col(price_col).mean().alias("mean_price")
            ])
        elif method == "sum":
            result = data.group_by_dynamic(
                timestamp_col,
                every=self.interval
            ).agg([
                pl.col(price_col).sum().alias("sum_price")
            ])
        elif method == "last":
            result = data.group_by_dynamic(
                timestamp_col,
                every=self.interval
            ).agg([
                pl.col(price_col).last().alias("last_price")
            ])
        else:
            raise ValueError(f"Unknown resampling method: {method}")
        
        return result
    
    def resample_lazy(
        self,
        data: pl.LazyFrame,
        timestamp_col: str = "timestamp",
        price_col: str = "price"
    ) -> pl.LazyFrame:
        """
        Resample using LazyFrame (for large datasets).
        
        Args:
            data: Input LazyFrame
            timestamp_col: Name of timestamp column
            price_col: Name of price column
            
        Returns:
            Resampled LazyFrame
        """
        return data.group_by_dynamic(
            timestamp_col,
            every=self.interval
        ).agg([
            pl.col(price_col).first().alias("open"),
            pl.col(price_col).max().alias("high"),
            pl.col(price_col).min().alias("low"),
            pl.col(price_col).last().alias("close")
        ])

