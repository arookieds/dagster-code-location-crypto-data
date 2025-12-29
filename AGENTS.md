# AGENTS.md - Development Guide for AI Coding Agents

This guide provides essential information for AI coding agents working in the **dagster-crypto-data** repository.

## Project Overview

This is a **Dagster code location** for orchestrating crypto data pipelines. The project:
- Extracts market data from centralized exchanges (Binance, ByBit, etc.)
- Loads raw JSON data into MinIO (Object Storage)
- Transforms data into a relational format in PostgreSQL
- Deploys as containerized code locations on Kubernetes using K8sRunLauncher

**Tech Stack:** Python 3.14+, Dagster, Pydantic, SQLModel, CCXT, Boto3, PostgreSQL

---

## Build, Lint, and Test Commands

### Package Management
This project uses **uv** for dependency management. Never use `pip` directly.

```bash
# Install dependencies
uv sync

# Add a new dependency
uv add <package-name>

# Add a development dependency
uv add --group testing <package-name>

# Update dependencies
uv lock --upgrade
```

### Linting and Formatting

```bash
# Run ruff linter (check for issues)
uv run ruff check .

# Run ruff linter with auto-fix
uv run ruff check --fix .

# Run ruff formatter (check formatting)
uv run ruff format --check .

# Run ruff formatter (apply formatting)
uv run ruff format .

# Run type checking with mypy
uv run mypy src

# Run type checking with pyright
uv run pyright src
```

### Testing

```bash
# Run all tests
uv run pytest

# Run tests with verbose output
uv run pytest -v

# Run a single test file
uv run pytest tests/test_file.py

# Run a single test function
uv run pytest tests/test_file.py::test_function_name

# Run tests by marker
uv run pytest -m unit
uv run pytest -m integration
uv run pytest -m slow

# Run tests by keyword expression
uv run pytest -k "test_extract"

# Run tests with coverage report
uv run pytest --cov=src.definitions --cov-report=term-missing

# Run tests and stop at first failure
uv run pytest -x

# Run tests and show local variables on failure
uv run pytest -l
```

### Dagster Development

```bash
# Start Dagster development server
uv run dagster dev

# Validate Dagster definitions
uv run dagster definitions validate

# List all assets
uv run dagster asset list

# Materialize a specific asset
uv run dagster asset materialize <asset_name>
```

---

## Code Style Guidelines

### Line Length and Formatting
- **Maximum line length:** 89 characters (enforced by Black and Ruff)
- Use **Ruff** for both linting and formatting (configured in `pyproject.toml`)
- Run `uv run ruff format .` before committing

### Imports
Follow the **isort** profile for Black:

```python
# Standard library imports
import os
import sys
from typing import Any, Optional

# Third-party imports
import dagster
from dagster import asset, AssetExecutionContext
from pydantic import BaseModel
from sqlmodel import Field, Session

# Local/application imports
from dagster_crypto_data.models import CryptoData
from dagster_crypto_data.utils import fetch_data
```

**Rules:**
- Group imports: standard library → third-party → local
- Sort alphabetically within each group
- Use absolute imports for local modules (e.g., `from src.dagster_crypto_data`)
- Unused imports in `__init__.py` files are allowed (Ruff ignores F401)

### Type Annotations
**CRITICAL:** All functions MUST have complete type annotations.

```python
# ✅ CORRECT - All parameters and return types annotated
def process_data(
    context: AssetExecutionContext,
    raw_data: dict[str, Any],
    timestamp: int | None = None,
) -> list[CryptoData]:
    """Process raw crypto data into structured format."""
    ...

# ❌ INCORRECT - Missing type annotations
def process_data(context, raw_data, timestamp=None):
    ...
```

**Configuration in `pyproject.toml`:**
- `disallow_untyped_defs = true` - All functions must have types
- `disallow_incomplete_defs = true` - No partial type hints
- `warn_return_any = true` - Be explicit with Any types
- `no_implicit_optional = true` - Use `Optional[T]` or `T | None` explicitly

**Type checking:**
- Use `uv run mypy src` to verify type correctness
- Use `uv run pyright src` for additional type checking
- Ignore missing imports for third-party libraries: `dagster.*`, `dlt.*`, `ccxt.*`

### Naming Conventions

```python
# Variables and functions: snake_case
user_data = fetch_user_data()
def calculate_total_volume(prices: list[float]) -> float:
    ...

# Classes: PascalCase
class CryptoExchangeClient:
    ...

# Constants: UPPER_SNAKE_CASE
MAX_RETRY_ATTEMPTS = 3
API_TIMEOUT_SECONDS = 30

# Dagster assets: snake_case
@asset
def raw_binance_ohlcv(context: AssetExecutionContext) -> dict[str, Any]:
    ...

# Private methods/variables: leading underscore
def _internal_helper() -> None:
    ...
```

### Error Handling

```python
# Use specific exception types
try:
    data = fetch_exchange_data(exchange="binance")
except ccxt.NetworkError as e:
    context.log.error(f"Network error fetching data: {e}")
    raise
except ccxt.ExchangeError as e:
    context.log.error(f"Exchange API error: {e}")
    raise

# Use context managers for resources
from contextlib import contextmanager

@contextmanager
def get_db_session() -> Session:
    session = Session(engine)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
```

### Docstrings
Use **Google-style docstrings** for public functions:

```python
def extract_ohlcv_data(
    exchange: str,
    symbol: str,
    timeframe: str,
    since: int | None = None,
) -> list[list[float]]:
    """Extract OHLCV data from a cryptocurrency exchange.

    Args:
        exchange: Exchange identifier (e.g., 'binance', 'bybit')
        symbol: Trading pair symbol (e.g., 'BTC/USDT')
        timeframe: Candle timeframe (e.g., '1m', '1h', '1d')
        since: Unix timestamp to fetch data from (optional)

    Returns:
        List of OHLCV candles, each containing [timestamp, open, high, low, close, volume]

    Raises:
        NetworkError: If network connectivity fails
        ExchangeError: If the exchange API returns an error
    """
    ...
```

---

## Important Configuration Notes

### Python Version
- **Required:** Python >= 3.14
- Check with: `python --version` or `cat .python-version`

### Ruff Linter Rules (Enabled)
- `E` - pycodestyle errors
- `W` - pycodestyle warnings
- `F` - pyflakes
- `I` - isort (import sorting)
- `B` - flake8-bugbear
- `C4` - flake8-comprehensions
- `UP` - pyupgrade
- `ARG` - flake8-unused-arguments
- `SIM` - flake8-simplify
- `TCH` - flake8-type-checking

### Ruff Linter Ignores
- `E501` - Line too long (handled by formatter)
- `B008` - Function calls in argument defaults (common in Pydantic/FastAPI)
- `W191` - Indentation contains tabs

### Test Markers
Use markers to categorize tests:
```python
import pytest

@pytest.mark.unit
def test_data_transformation() -> None:
    ...

@pytest.mark.integration
def test_database_connection() -> None:
    ...

@pytest.mark.slow
def test_full_pipeline() -> None:
    ...
```

---

## Common Patterns

### Dagster Asset Definition
```python
from dagster import asset, AssetExecutionContext

@asset
def raw_exchange_data(context: AssetExecutionContext) -> dict[str, Any]:
    """Extract raw data from exchange API."""
    context.log.info("Fetching exchange data")
    # Implementation
    return data
```

### Pydantic Models
```python
from pydantic import BaseModel, Field

class OHLCVData(BaseModel):
    timestamp: int = Field(..., description="Unix timestamp in milliseconds")
    open: float = Field(..., gt=0)
    high: float = Field(..., gt=0)
    low: float = Field(..., gt=0)
    close: float = Field(..., gt=0)
    volume: float = Field(..., ge=0)
```

---

## File Organization

```
project/
├── src/
│   ├── definitions.py          # Dagster Definitions entrypoint
│   └── dagster_crypto_data/
│       └── __init__.py
├── tests/                       # Test files (currently empty)
├── docs/                        # Documentation and diagrams
├── pyproject.toml               # Project configuration
├── uv.lock                      # Locked dependencies (DO NOT edit manually)
└── .python-version              # Python version specification
```

---

## DO NOT

1. ❌ Commit files containing secrets (`.env`, `credentials.json`, etc.)
2. ❌ Use `pip` directly - always use `uv`
3. ❌ Edit `uv.lock` manually - regenerate with `uv lock`
4. ❌ Skip type annotations on functions
5. ❌ Use relative imports for application code
6. ❌ Ignore linter errors without good reason

---

## Quick Reference Card

| Task | Command |
|------|---------|
| Install deps | `uv sync` |
| Run linter | `uv run ruff check .` |
| Auto-fix lint | `uv run ruff check --fix .` |
| Format code | `uv run ruff format .` |
| Type check | `uv run mypy src` |
| Run all tests | `uv run pytest` |
| Run single test | `uv run pytest tests/test_file.py::test_name` |
| Start Dagster UI | `uv run dagster dev` |
| Validate defs | `uv run dagster definitions validate` |

---

**Last Updated:** 2025-12-28  
**Dagster Module:** `src.definitions`  
**Python Version:** 3.14+
