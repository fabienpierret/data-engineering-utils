"""
Dollar Bars Example
===================

Demonstrates how to create dollar bars from tick data.
"""

import sys
from pathlib import Path
import polars as pl
import numpy as np

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from data_utils.dollar_bars import DollarBars


def main():
    """Run dollar bars example."""
    
    # Generate sample tick data
    print("Generating sample tick data...")
    n_ticks = 10000
    
    np.random.seed(42)
    timestamps = pl.date_range(
        start=pl.datetime(2023, 1, 1, 0, 0, 0),
        end=pl.datetime(2023, 1, 1, 23, 59, 59),
        interval="1s",
        eager=True
    )[:n_ticks]
    
    # Generate synthetic price and volume data
    prices = 100 + np.cumsum(np.random.randn(n_ticks) * 0.1)
    volumes = np.random.randint(100, 1000, n_ticks)
    
    tick_data = pl.DataFrame({
        "timestamp": timestamps,
        "price": prices,
        "volume": volumes
    })
    
    print(f"Tick data shape: {tick_data.shape}")
    print(f"Tick data sample:\n{tick_data.head()}")
    
    # Create dollar bars
    print("\nCreating dollar bars (threshold: $100k)...")
    dollar_bars = DollarBars(threshold=100000.0, min_ticks=10)
    bars = dollar_bars.create(tick_data)
    
    print(f"\nCreated {len(bars)} dollar bars")
    print(f"Dollar bars sample:\n{bars.head()}")
    
    # Statistics
    print(f"\nStatistics:")
    print(f"  Average bar dollar volume: ${bars['dollar_volume'].mean():,.2f}")
    print(f"  Total dollar volume: ${bars['dollar_volume'].sum():,.2f}")
    print(f"  Number of bars: {len(bars)}")
    
    print("\n✅ Dollar bars example completed!")


if __name__ == "__main__":
    main()

