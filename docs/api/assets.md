# Assets

Documentation for Dagster assets (extract, transform, load).

## Extract Assets (Bronze Layer)

Extract assets fetch raw data from cryptocurrency exchanges and store it in MinIO.

### `extract_binance_ohlcv`

Extract OHLCV (Open, High, Low, Close, Volume) candles from Binance.

**Returns**: Dictionary with raw OHLCV data and metadata

**Example**:
```python
from dagster import build_asset_context
from dagster_crypto_data.assets.extract import extract_binance_ohlcv

context = build_asset_context()
result = extract_binance_ohlcv(context)
```

## Transform Assets (Silver Layer)

Transform assets clean, validate, and structure raw data.

### `transform_ohlcv`

Transform raw OHLCV data into validated Pydantic models.

**Parameters**:
- `raw_data`: Raw OHLCV data from extract asset

**Returns**: List of validated `OHLCVData` models

**Example**:
```python
from dagster_crypto_data.assets.transform import transform_ohlcv

transformed = transform_ohlcv(context, raw_data)
```

---

**Note**: For detailed implementation, see source code in `src/dagster_crypto_data/assets/`.
