# Data Engineering Utilities

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/)
[![Tests](https://github.com/fabienpierret/data-engineering-utils/workflows/CI/badge.svg)](https://github.com/fabienpierret/data-engineering-utils/actions)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

A collection of production-ready utilities for data engineering workflows. This library provides efficient, scalable tools for data processing, transformation, and storage optimization using Polars and Parquet.

## 🎯 Objective

This library provides reusable utilities for common data engineering tasks:
- **Dollar Bars**: Time-independent bar sampling for financial data
- **Data Resampling**: Efficient time-series resampling
- **Parquet Optimization**: Compression, partitioning, and storage strategies
- **Data Validation**: Schema validation and data quality checks
- **Memory Management**: Efficient handling of large datasets

## 🛠️ Technologies

- **Python 3.11+**
- **Polars** (Lazy API, Rust backend for performance)
- **Parquet** (Columnar storage format)
- **NumPy** (Numerical operations)
- **PyArrow** (Parquet I/O)

## 📊 Features

### 1. Dollar Bars
Time-independent bar sampling based on dollar volume. More robust than time-based bars for financial data.

### 2. Data Resampling
Efficient resampling of time-series data (downsampling, upsampling, aggregation).

### 3. Parquet Utilities
- Compression optimization (zstd, snappy, gzip)
- Partitioning strategies (date-based, categorical)
- Schema evolution handling

### 4. Data Validation
- Schema validation
- Data quality checks
- Missing value detection

### 5. Memory Management
- Streaming processing for large datasets
- Lazy evaluation patterns
- Memory-efficient operations

## 🚀 Installation

```bash
# Clone the repository
git clone https://github.com/fabienpierret/data-engineering-utils.git
cd data-engineering-utils

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On macOS/Linux

# Install dependencies
pip install -r requirements.txt
```

## 💻 Usage

### Dollar Bars

```python
from data_utils.dollar_bars import DollarBars

# Create dollar bars from tick data
dollar_bars = DollarBars(threshold=100000)  # $100k per bar

# Process tick data
tick_data = pl.DataFrame({
    "timestamp": [...],
    "price": [...],
    "volume": [...]
})

bars = dollar_bars.create(tick_data)
print(f"Created {len(bars)} dollar bars")
```

### Data Resampling

```python
from data_utils.resampling import Resampler

# Resample to 1-hour bars
resampler = Resampler(interval="1h")
hourly_data = resampler.resample(
    data,
    price_col="price",
    volume_col="volume"
)
```

### Parquet Optimization

```python
from data_utils.parquet_utils import optimize_parquet

# Optimize Parquet file
optimize_parquet(
    input_path="data/raw.parquet",
    output_path="data/optimized.parquet",
    compression="zstd",
    partition_by="date"
)
```

### Data Validation

```python
from data_utils.validation import validate_schema, check_data_quality

# Validate schema
schema = {
    "price": pl.Float64,
    "volume": pl.Int64,
    "timestamp": pl.Datetime
}

is_valid = validate_schema(data, schema)

# Check data quality
quality_report = check_data_quality(data)
print(quality_report)
```

## 📁 Project Structure

```
data-engineering-utils/
├── data_utils/
│   ├── __init__.py
│   ├── dollar_bars.py      # Dollar bar sampling
│   ├── resampling.py        # Time-series resampling
│   ├── parquet_utils.py     # Parquet optimization
│   ├── validation.py        # Data validation
│   └── memory.py            # Memory management
├── examples/
│   ├── dollar_bars_example.py
│   ├── resampling_example.py
│   └── parquet_example.py
├── tests/
│   ├── test_dollar_bars.py
│   ├── test_resampling.py
│   └── test_validation.py
├── requirements.txt
├── .gitignore
└── README.md
```

## 🧪 Testing

```bash
# Install dependencies
pip install -r requirements.txt

# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=data_utils --cov-report=html

# Run specific test
pytest tests/test_dollar_bars.py -v
```

## 📊 Performance

- **Dollar Bars**: Processes 1M+ ticks in < 10 seconds
- **Resampling**: Handles Tera-bytes with Polars Lazy API
- **Parquet Compression**: 70-80% size reduction with zstd
- **Memory**: Streaming processing for datasets larger than RAM

## 🔧 Configuration

```python
# Example configuration
config = {
    "dollar_bars": {
        "threshold": 100000,  # $100k per bar
        "min_ticks": 10      # Minimum ticks per bar
    },
    "parquet": {
        "compression": "zstd",
        "compression_level": 3,
        "row_group_size": 100000
    },
    "validation": {
        "check_missing": True,
        "check_duplicates": True,
        "check_outliers": True
    }
}
```

## 🤝 Contributing

This is a utility library. Feel free to fork and adapt it to your needs.

## 📝 License

MIT License - See LICENSE file for details

## 👤 Author

**Fabien Pierret**
- GitHub: [@fabienpierret](https://github.com/fabienpierret)
- Portfolio: [fabienpierret.github.io](https://fabienpierret.github.io)

## 🙏 Acknowledgments

- Built on Polars (Rust backend for performance)
- Optimized for Apple Silicon (M4 Max)
- Designed for production data engineering workflows

