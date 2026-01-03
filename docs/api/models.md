# Models

Documentation for Pydantic and SQLModel data models.

## Pydantic Models (Validation)

### TickerData

Ticker data model for exchange ticker information.

**Module**: `dagster_crypto_data.models.tickers`

**Fields**:
- `symbol`: Trading pair symbol (e.g., "BTC/USDT")
- `last`: Last traded price
- `bid`: Best bid price
- `ask`: Best ask price
- `volume`: 24h trading volume
- `timestamp`: Unix timestamp (milliseconds)

**Example**:
```python
from dagster_crypto_data.defs.models.tickers import TickerData

ticker = TickerData(
    symbol="BTC/USDT",
    last=50000.0,
    bid=49999.0,
    ask=50001.0,
    volume=1000.5,
    timestamp=1735689600000,
)
```

### ExtractConfig

Configuration model for extract assets.

**Module**: `dagster_crypto_data.models.pipeline_configs`

**Fields**:
- `exchange`: Exchange identifier (e.g., "binance")
- `symbols`: List of trading pairs
- `timeframe`: Candle timeframe (e.g., "1h")
- `limit`: Number of candles to fetch

**Example**:
```python
from dagster_crypto_data.defs.models.pipeline_configs import ExtractConfig

config = ExtractConfig(
    exchange="binance",
    symbols=["BTC/USDT", "ETH/USDT"],
    timeframe="1h",
    limit=100,
)
```

## SQLModel Models (Database)

### OHLCV

OHLCV table model for PostgreSQL.

**Table**: `ohlcv`

**Columns**:
- `id`: Primary key (auto-increment)
- `timestamp`: Unix timestamp (indexed)
- `open`: Opening price
- `high`: Highest price
- `low`: Lowest price
- `close`: Closing price
- `volume`: Trading volume
- `exchange`: Exchange identifier (indexed)
- `symbol`: Trading pair symbol (indexed)

**Example**:
```python
from sqlmodel import Session
from dagster_crypto_data.defs.models import OHLCV

with Session(engine) as session:
    candle = OHLCV(
        timestamp=1735689600000,
        open=50000.0,
        high=51000.0,
        low=49000.0,
        close=50500.0,
        volume=100.5,
        exchange="binance",
        symbol="BTC/USDT",
    )
    session.add(candle)
    session.commit()
```

---

**Note**: For detailed implementation, see source code in `src/dagster_crypto_data/models/`.
