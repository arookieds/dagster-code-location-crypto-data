# Development Guide

This guide covers the development workflow, coding standards, testing practices, and contribution guidelines for the Dagster Crypto Data Pipeline.

## Development Workflow

### 1. Create a Feature Branch

```bash
# Update main branch
git checkout main
git pull origin main

# Create feature branch
git checkout -b feature/your-feature-name
```

### 2. Make Changes

Follow the coding standards outlined in [AGENTS.md](../AGENTS.md).

### 3. Run Tests

```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/test_extract.py

# Run with coverage
uv run pytest --cov=dagster_crypto_data --cov-report=term-missing

# Run without coverage (faster)
uv run pytest --no-cov
```

### 4. Lint and Format

```bash
# Check linting
uv run ruff check .

# Auto-fix linting issues
uv run ruff check --fix .

# Format code
uv run ruff format .

# Type checking
uv run mypy src
uv run pyright src
```

### 5. Commit Changes

```bash
# Stage changes
git add .

# Commit with descriptive message
git commit -m "feat: add support for Gate.io exchange"

# Push to remote
git push origin feature/your-feature-name
```

### 6. Create Pull Request

- Go to GitHub and create a pull request
- Fill in the PR template with description and testing notes
- Request review from maintainers
- Address review feedback

## Coding Standards

### Code Style

- **Line Length**: Maximum 89 characters
- **Formatter**: Ruff (Black-compatible)
- **Linter**: Ruff with strict rules
- **Type Hints**: Required for all functions

### Naming Conventions

```python
# Variables and functions: snake_case
user_data = fetch_user_data()

# Classes: PascalCase
class ExchangeClient:
    pass

# Constants: UPPER_SNAKE_CASE
MAX_RETRIES = 3

# Dagster assets: snake_case
@asset
def extract_binance_ohlcv(context: AssetExecutionContext) -> dict[str, Any]:
    pass

# Private methods: leading underscore
def _internal_helper() -> None:
    pass
```

### Type Annotations

**All functions must have complete type annotations**:

```python
# ✅ CORRECT
def process_data(
    context: AssetExecutionContext,
    raw_data: dict[str, Any],
    timestamp: int | None = None,
) -> list[OHLCVData]:
    """Process raw OHLCV data."""
    ...

# ❌ INCORRECT - Missing type annotations
def process_data(context, raw_data, timestamp=None):
    ...
```

### Imports

```python
# Use __future__ imports for forward references
from __future__ import annotations

# Standard library
import os
from typing import TYPE_CHECKING, Any

# Third-party
from dagster import asset, AssetExecutionContext
from pydantic import BaseModel

# Local
from dagster_crypto_data.defs.models import OHLCVData

# TYPE_CHECKING imports (avoid circular imports)
if TYPE_CHECKING:
    from sqlalchemy import Engine
```

### Docstrings

Use **Google-style docstrings**:

```python
def extract_ohlcv(
    exchange: str,
    symbol: str,
    timeframe: str = "1h",
) -> list[list[float]]:
    """Extract OHLCV data from cryptocurrency exchange.

    Args:
        exchange: Exchange identifier (e.g., 'binance', 'bybit')
        symbol: Trading pair symbol (e.g., 'BTC/USDT')
        timeframe: Candle timeframe (default: '1h')

    Returns:
        List of OHLCV candles, each containing:
        [timestamp, open, high, low, close, volume]

    Raises:
        NetworkError: If network connectivity fails
        ExchangeError: If exchange API returns an error

    Example:
        >>> data = extract_ohlcv("binance", "BTC/USDT", "1h")
        >>> len(data)
        100
    """
    ...
```

## Testing

### Test Structure

```
tests/
├── test_extract.py          # Extract asset tests
├── test_transform.py        # Transform asset tests
├── test_database.py         # Database connector tests
├── test_exchange_resource.py # Exchange resource tests
└── test_logger.py           # Logger utility tests
```

### Writing Tests

#### Unit Tests

```python
import pytest
from dagster_crypto_data.defs.models import OHLCVData

@pytest.mark.unit
def test_ohlcv_validation() -> None:
    """Test OHLCV data validation."""
    # Valid data
    data = OHLCVData(
        timestamp=1735689600000,
        open=50000.0,
        high=51000.0,
        low=49000.0,
        close=50500.0,
        volume=100.5,
        exchange="binance",
        symbol="BTC/USDT",
    )
    assert data.open == 50000.0
    
    # Invalid data (negative price)
    with pytest.raises(ValueError):
        OHLCVData(
            timestamp=1735689600000,
            open=-50000.0,  # Invalid
            high=51000.0,
            low=49000.0,
            close=50500.0,
            volume=100.5,
            exchange="binance",
            symbol="BTC/USDT",
        )
```

#### Integration Tests

```python
import pytest
from dagster import build_asset_context
from dagster_crypto_data.defs.assets.extract import extract_binance_ohlcv

@pytest.mark.integration
def test_extract_binance_ohlcv_integration() -> None:
    """Test Binance OHLCV extraction (requires network)."""
    context = build_asset_context()
    result = extract_binance_ohlcv(context)
    
    assert isinstance(result, dict)
    assert "data" in result
    assert len(result["data"]) > 0
```

#### Fixtures

```python
import pytest
from sqlmodel import Session, create_engine

@pytest.fixture
def db_session() -> Session:
    """Create in-memory SQLite database for testing."""
    engine = create_engine("sqlite:///:memory:")
    with Session(engine) as session:
        yield session
```

### Test Markers

Use markers to categorize tests:

```python
@pytest.mark.unit
def test_fast_unit_test() -> None:
    """Fast unit test."""
    pass

@pytest.mark.integration
def test_integration_with_api() -> None:
    """Integration test requiring network."""
    pass

@pytest.mark.slow
def test_slow_end_to_end() -> None:
    """Slow end-to-end test."""
    pass
```

Run specific test categories:

```bash
# Run only unit tests
uv run pytest -m unit

# Run only integration tests
uv run pytest -m integration

# Skip slow tests
uv run pytest -m "not slow"
```

### Coverage Requirements

- **Minimum Coverage**: 80% overall
- **Critical Paths**: 100% coverage for data validation and transformations
- **Exclude**: `__init__.py`, test files, type stubs

```bash
# Generate coverage report
uv run pytest --cov=dagster_crypto_data --cov-report=html

# View HTML report
open htmlcov/index.html
```

## Dagster Development

### Creating Assets

```python
from dagster import asset, AssetExecutionContext

@asset(
    group_name="extract",
    compute_kind="python",
    description="Extract OHLCV data from Binance",
)
def extract_binance_ohlcv(
    context: AssetExecutionContext,
) -> dict[str, Any]:
    """Extract OHLCV candles from Binance API."""
    context.log.info("Starting Binance OHLCV extraction")
    
    # Implementation
    data = fetch_from_binance()
    
    context.log.info(f"Extracted {len(data)} candles")
    return {"data": data, "metadata": {...}}
```

### Asset Dependencies

```python
from dagster import asset, AssetIn

@asset(
    ins={
        "raw_data": AssetIn(key="extract_binance_ohlcv"),
    },
)
def transform_binance_ohlcv(
    context: AssetExecutionContext,
    raw_data: dict[str, Any],
) -> list[OHLCVData]:
    """Transform raw OHLCV data."""
    context.log.info("Transforming OHLCV data")
    
    # Transform
    transformed = transform_data(raw_data)
    
    return transformed
```

### Resources

```python
from dagster import ConfigurableResource
from pydantic import Field

class ExchangeResource(ConfigurableResource):
    """CCXT exchange resource."""
    
    exchange_id: str = Field(description="Exchange identifier")
    api_key: str | None = Field(default=None, description="API key")
    secret: str | None = Field(default=None, description="API secret")
    
    def get_client(self) -> ccxt.Exchange:
        """Get CCXT exchange client."""
        exchange_class = getattr(ccxt, self.exchange_id)
        return exchange_class({
            "apiKey": self.api_key,
            "secret": self.secret,
        })
```

### Testing Assets

```python
from dagster import build_asset_context

def test_extract_binance_ohlcv() -> None:
    """Test Binance OHLCV extraction asset."""
    context = build_asset_context()
    result = extract_binance_ohlcv(context)
    
    assert isinstance(result, dict)
    assert "data" in result
```

## Debugging

### Local Debugging

```python
# Add breakpoint
import pdb; pdb.set_trace()

# Or use built-in breakpoint()
breakpoint()
```

### Dagster UI Debugging

1. Go to http://localhost:3000
2. Navigate to **Assets** → Select asset
3. Click **Materialize** to run
4. View logs in **Run Details**

### Logging

```python
from dagster_crypto_data.defs.utils import get_logger

logger = get_logger(__name__)

# Structured logging
logger.info("Processing data", exchange="binance", symbol="BTC/USDT")
logger.error("Failed to fetch data", error=str(e), retry_count=3)
```

## Pre-commit Hooks

Install pre-commit hooks to automatically check code before committing:

```bash
# Install pre-commit
uv add --group testing pre-commit

# Install hooks
uv run pre-commit install

# Run manually
uv run pre-commit run --all-files
```

`.pre-commit-config.yaml`:

```yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.8.4
    hooks:
      - id: ruff
        args: [--fix]
      - id: ruff-format

  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.13.0
    hooks:
      - id: mypy
        additional_dependencies: [types-all]
```

## Continuous Integration

GitHub Actions workflow (`.github/workflows/ci.yml`):

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Install uv
        uses: astral-sh/setup-uv@v1
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.14'
      
      - name: Install dependencies
        run: uv sync
      
      - name: Run linter
        run: uv run ruff check .
      
      - name: Run type checker
        run: uv run mypy src
      
      - name: Run tests
        run: uv run pytest --cov=dagster_crypto_data
      
      - name: Upload coverage
        uses: codecov/codecov-action@v4
```

## Best Practices

### 1. Keep Functions Small

```python
# ✅ GOOD - Single responsibility
def fetch_ohlcv(exchange: str, symbol: str) -> list[list[float]]:
    """Fetch OHLCV data."""
    client = get_exchange_client(exchange)
    return client.fetch_ohlcv(symbol)

def validate_ohlcv(data: list[list[float]]) -> list[OHLCVData]:
    """Validate OHLCV data."""
    return [OHLCVData.from_list(row) for row in data]

# ❌ BAD - Too many responsibilities
def fetch_and_validate_and_save_ohlcv(...):
    """Do everything."""
    ...
```

### 2. Use Type Hints

```python
# ✅ GOOD
def process_data(data: list[dict[str, Any]]) -> list[OHLCVData]:
    ...

# ❌ BAD
def process_data(data):
    ...
```

### 3. Handle Errors Gracefully

```python
# ✅ GOOD
try:
    data = fetch_data()
except NetworkError as e:
    logger.error("Network error", error=str(e))
    raise
except ExchangeError as e:
    logger.error("Exchange error", error=str(e))
    return []  # Return empty list instead of crashing

# ❌ BAD
try:
    data = fetch_data()
except Exception:
    pass  # Silent failure
```

### 4. Write Testable Code

```python
# ✅ GOOD - Dependency injection
def process_data(data: list[dict], validator: Callable) -> list[OHLCVData]:
    return [validator(item) for item in data]

# ❌ BAD - Hard to test
def process_data(data: list[dict]) -> list[OHLCVData]:
    # Hardcoded dependency
    validator = OHLCVValidator()
    return [validator.validate(item) for item in data]
```

## Common Pitfalls

### 1. Forgetting Type Hints

```python
# ❌ Will fail mypy check
def process(data):
    return data

# ✅ Correct
def process(data: dict[str, Any]) -> dict[str, Any]:
    return data
```

### 2. Not Using Context Managers

```python
# ❌ BAD - Resource leak
session = Session(engine)
session.query(...)
session.close()

# ✅ GOOD - Automatic cleanup
with Session(engine) as session:
    session.query(...)
```

### 3. Mutable Default Arguments

```python
# ❌ BAD - Mutable default
def process(data: list[str] = []) -> list[str]:
    data.append("item")
    return data

# ✅ GOOD - Immutable default
def process(data: list[str] | None = None) -> list[str]:
    if data is None:
        data = []
    data.append("item")
    return data
```

---

**Next Steps**: See [Deployment Guide](deployment.md) for production deployment instructions.
