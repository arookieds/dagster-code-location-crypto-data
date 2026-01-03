# Resources

Documentation for Dagster resources.

## ExchangeResource

CCXT-based cryptocurrency exchange resource.

**Module**: `dagster_crypto_data.resources.exchange`

### Configuration

```python
from dagster_crypto_data.defs.resources.exchange import ExchangeResource

exchange = ExchangeResource(
    exchange_id="binance",
    api_key="your-api-key",  # Optional
    secret="your-secret",    # Optional
)
```

### Methods

#### `get_client() -> ccxt.Exchange`

Get CCXT exchange client instance.

**Returns**: Configured CCXT exchange client

**Example**:
```python
client = exchange.get_client()
ohlcv = client.fetch_ohlcv("BTC/USDT", "1h")
```

---

**Note**: For detailed implementation, see source code in `src/dagster_crypto_data/resources/`.
