# API Reference

This section provides detailed API documentation for the Dagster Crypto Data Pipeline codebase.

## Overview

The codebase is organized into the following modules:

- **[Assets](assets.md)**: Dagster assets for extract, transform, and load operations
- **[Resources](resources.md)**: Dagster resources (exchange clients, database connections)
- **[Models](models.md)**: Pydantic and SQLModel data models
- **Connectors**: Database and storage connectors
- **Utils**: Utility functions (logging, settings)

## Module Structure

```
src/dagster_crypto_data/
├── assets/
│   ├── extract.py       # Extract assets (Bronze layer)
│   └── transform.py     # Transform assets (Silver layer)
├── resources/
│   └── exchange.py      # CCXT exchange resource
├── models/
│   ├── pipeline_configs.py  # Pipeline configuration models
│   └── tickers.py       # Ticker data models
├── connectors/
│   └── database.py      # PostgreSQL connector
└── utils/
    ├── logger.py        # Structured logging
    └── settings.py      # Application settings
```

## Quick Reference

### Assets

```python
from dagster_crypto_data.defs.assets.extract import extract_binance_ohlcv
from dagster_crypto_data.defs.assets.transform import transform_ohlcv
```

### Resources

```python
from dagster_crypto_data.defs.resources.exchange import ExchangeResource

exchange = ExchangeResource(exchange_id="binance")
client = exchange.get_client()
```

### Models

```python
from dagster_crypto_data.defs.models.tickers import TickerData
from dagster_crypto_data.defs.models.pipeline_configs import ExtractConfig

ticker = TickerData(symbol="BTC/USDT", last=50000.0, ...)
config = ExtractConfig(exchange="binance", symbols=["BTC/USDT"])
```

### Database

```python
from dagster_crypto_data.defs.connectors.database import get_engine, get_session

engine = get_engine()
with get_session() as session:
    session.query(...)
```

### Logging

```python
from dagster_crypto_data.defs.utils.logger import get_logger

logger = get_logger(__name__)
logger.info("Processing data", exchange="binance", symbol="BTC/USDT")
```

## Type Annotations

All functions in the codebase have complete type annotations. Use your IDE's autocomplete and type checking features for the best development experience.

```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy import Engine
    from dagster import AssetExecutionContext
```

## Next Steps

- [Assets Documentation](assets.md)
- [Resources Documentation](resources.md)
- [Models Documentation](models.md)

---

**Note**: This is auto-generated documentation. For implementation details, refer to the source code.
