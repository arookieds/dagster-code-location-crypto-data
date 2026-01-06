# AGENTS.md - Development Guide for AI Coding Agents

This guide provides essential information for AI coding agents working in the **dagster-crypto-data** repository.

## Project Overview

This is a **Dagster code location** for orchestrating crypto data pipelines. The project:
- Extracts market data from centralized exchanges (Binance, ByBit, etc.)
- Loads raw JSON data into MinIO (Object Storage)
- Transforms data into a relational format in PostgreSQL
- Deploys as containerized code locations on Kubernetes using K8sRunLauncher

**Tech Stack:** Python 3.14+, Dagster, Pydantic, SQLModel, CCXT, Boto3, PostgreSQL, Structlog

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
uv run pytest tests/test_database.py

# Run a single test function
uv run pytest tests/test_database.py::test_url_generation_sqlite

# Run tests by marker
uv run pytest -m unit
uv run pytest -m integration
uv run pytest -m slow

# Run tests by keyword expression
uv run pytest -k "test_extract"

# Run tests with coverage report
uv run pytest --cov=dagster_crypto_data --cov-report=term-missing

# Run tests and stop at first failure
uv run pytest -x

# Run tests and show local variables on failure
uv run pytest -l

# Run tests without coverage (faster for development)
uv run pytest --no-cov
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

### Documentation

```bash
# Serve documentation with live reload (recommended)
uv run mkdocs serve --livereload

# Note: --livereload flag is required due to Click library compatibility issue
# Without it, file watching may not work properly

# Serve on custom port
uv run mkdocs serve --livereload -a localhost:9000

# Build static site
uv run mkdocs build
```

### Docker & Kubernetes

```bash
# Build Docker image
docker build -t ghcr.io/arookieds/dagster-code-location-crypto-data:latest .

# Push to registry
docker push ghcr.io/arookieds/dagster-code-location-crypto-data:latest

# Deploy to Kubernetes (uses Helm chart via Kustomize)
kubectl apply -k manifests/

# Check deployment status
kubectl get pods -n dagster -l app=crypto-data

# View logs
kubectl logs -n dagster -l app=crypto-data -f

# Restart deployment
kubectl rollout restart deployment/crypto-data-crypto-data -n dagster
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
# Use __future__ imports when needed for forward references
from __future__ import annotations

# Standard library imports
import os
import sys
from typing import TYPE_CHECKING, Any

# Third-party imports
import dagster
from dagster import asset, AssetExecutionContext
from pydantic import BaseModel
from sqlmodel import Field, Session

# Local/application imports
from dagster_crypto_data.defs.models import CryptoData
from dagster_crypto_data.defs.utils import fetch_data

# TYPE_CHECKING imports (for type hints only, not runtime)
if TYPE_CHECKING:
    from sqlalchemy import Engine
```

**Rules:**
- Use `from __future__ import annotations` for forward references
- Group imports: standard library → third-party → local
- Sort alphabetically within each group
- Use absolute imports for local modules (e.g., `from dagster_crypto_data`)
- Unused imports in `__init__.py` files are allowed (Ruff ignores F401)
- Use `TYPE_CHECKING` for imports only needed for type hints to avoid circular imports

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
- **Required:** Python 3.14 (specified in `.python-version`)
- Check with: `python --version` or `cat .python-version`
- Managed by `uv` - automatically uses correct version

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
from pydantic import BaseModel, Field, field_validator, computed_field

class OHLCVData(BaseModel):
    timestamp: int = Field(..., description="Unix timestamp in milliseconds")
    open: float = Field(..., gt=0)
    high: float = Field(..., gt=0)
    low: float = Field(..., gt=0)
    close: float = Field(..., gt=0)
    volume: float = Field(..., ge=0)

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v: int) -> int:
        """Validate timestamp is positive."""
        if v <= 0:
            raise ValueError("Timestamp must be positive")
        return v

    @computed_field  # type: ignore[prop-decorator]
    @property
    def price_range(self) -> float:
        """Calculate price range (high - low)."""
        return self.high - self.low
```

### Logging
Use **structlog** for structured logging:

```python
from dagster_crypto_data.defs.utils import get_logger

# Get logger instance
logger = get_logger(__name__, log_level="INFO", use_json=False)

# Log with context
logger.info("Processing data", exchange="binance", symbol="BTC/USDT")
logger.error("Failed to fetch data", error=str(e), retry_count=3)
```

---

## File Organization

```
project/
├── src/
│   └── dagster_crypto_data/
│       ├── definitions.py          # Dagster Definitions entrypoint
│       ├── defs/
│       │   ├── assets/             # Asset factories (extract, transform)
│       │   ├── connectors/         # Database connectors
│       │   ├── io_managers/        # Custom IO managers
│       │   ├── models/             # Pydantic/SQLModel models
│       │   ├── resources/          # Dagster resources (CCXT exchange)
│       │   └── utils/              # Utilities (logger, settings)
│       └── __init__.py
├── tests/                          # Test files (110 tests)
├── docs/                           # Documentation and diagrams
├── pyproject.toml                  # Project configuration
├── uv.lock                         # Locked dependencies (DO NOT edit manually)
└── .python-version                 # Python version specification
```

---

## Security Best Practices

1. **Never hardcode credentials** - Use environment variables or Pydantic Settings
2. **Use SecretStr** for sensitive fields in Pydantic models
3. **Validate user inputs** - Use Pydantic validators to prevent injection attacks
4. **Override `__repr__` and `__str__`** - Don't expose passwords in string representations
5. **Use `PrivateAttr`** for internal state in Pydantic models

```python
from pydantic import BaseModel, Field, SecretStr, PrivateAttr

class DatabaseConfig(BaseModel):
    username: str
    password: SecretStr = Field(default=SecretStr(""))
    _engine: Engine | None = PrivateAttr(default=None)

    def __repr__(self) -> str:
        """Don't expose password in repr."""
        return f"DatabaseConfig(username={self.username!r})"
```

---

## Git Workflow and Branch Management

### CRITICAL: Mandatory Git Workflow for All Changes

**NEVER push directly to target branches (especially `main`) under any circumstances.** All changes MUST go through the Pull Request workflow, even for single-user development. This maintains best practices, ensures CI validation, and creates proper audit trails.

#### Understanding User Commands

When the user says:
- **"push to main"** (while on `dev` branch) → means "merge `dev` to `main`"
- **"push to dev"** (while on `feature1` branch) → means "merge `feature1` to `dev`"
- **"merge feature1 to dev"** → explicitly merge source branch `feature1` into target branch `dev`
- **"merge feature1 to main"** → explicitly merge source branch `feature1` into target branch `main`

The general pattern is: **"merge <source-branch> to <target-branch>"** or contextually infer from current branch.

#### Required Workflow for Merging Any Branch

When instructed to merge changes (e.g., "push to main", "merge feature1 to dev"), you MUST follow this exact sequence:

**Step 1: Identify Source and Target Branches**
- **Source branch**: The branch containing the new changes (e.g., `dev`, `feature1`)
- **Target branch**: The branch to merge into (e.g., `main`, `dev`)

**Step 2: Commit and Push Source Branch**
```bash
# Ensure you're on the source branch
git checkout <source-branch>

# Commit any pending changes
git add <files>
git commit -m "descriptive message"

# Push source branch to remote
git push origin <source-branch>
```

**Step 3: Create Pull Request**
```bash
gh pr create \
  --title "Title describing the changes" \
  --body "Detailed description" \
  --head <source-branch> \
  --base <target-branch>
```

**Step 4: Wait for CI Checks to Pass**
```bash
gh pr checks <PR_NUMBER> --watch
```

**Step 5: Merge Pull Request** (do NOT approve your own PR - merge only)
```bash
gh pr merge <PR_NUMBER> --merge --delete-branch=false
```

**Step 6: Synchronize Branches After Merge**
```bash
# Update local target branch
git checkout <target-branch>
git pull origin <target-branch>

# Delete old source branch (local and remote)
git branch -D <source-branch>
git push origin --delete <source-branch>

# Recreate source branch from updated target
git checkout -b <source-branch>
git push origin <source-branch>
```

#### Why This Workflow is Mandatory

- **CI/CD Safety**: Ensures all changes pass automated tests before reaching the target branch
- **Audit Trail**: PRs provide reviewable history of what changed and why
- **Production Safety**: In corporate environments, direct pushes to `main` can trigger immediate deployments, break integration pipelines, or violate compliance requirements (SOC2, ISO, etc.)
- **Branch Protection**: Simulates real-world branch protection rules that would prevent direct pushes
- **Professional Discipline**: Builds proper Git habits that transfer to team/enterprise environments
- **Flexibility**: Works for any branch strategy (feature → dev → main, hotfix → main, etc.)

#### What NOT to Do

- ❌ **NEVER** run `git push origin <target-branch>` after committing directly to the target branch
- ❌ **NEVER** commit directly to the target branch and push (bypasses PR workflow)
- ❌ **NEVER** bypass the PR workflow "because it's faster"
- ❌ **NEVER** merge without waiting for CI checks to complete
- ❌ **NEVER** force-push to protected branches like `main`

#### Examples: Correct Workflows

**Example 1: User says "push to main" (while on `dev` branch)**
```bash
# ✅ CORRECT - Merge dev → main via PR
git checkout dev
git add .
git commit -m "feat: add new feature"
git push origin dev
gh pr create --title "feat: add new feature" --body "Description" --head dev --base main
gh pr checks <PR#> --watch
gh pr merge <PR#> --merge --delete-branch=false
git checkout main && git pull origin main
git branch -D dev && git push origin --delete dev
git checkout -b dev && git push origin dev

# ❌ INCORRECT - DO NOT DO THIS
git checkout main
git merge dev
git push origin main  # ❌ FORBIDDEN! Bypasses PR workflow!
```

**Example 2: User says "merge feature1 to dev"**
```bash
# ✅ CORRECT - Merge feature1 → dev via PR
git checkout feature1
git add .
git commit -m "feat: implement feature1"
git push origin feature1
gh pr create --title "feat: implement feature1" --body "Description" --head feature1 --base dev
gh pr checks <PR#> --watch
gh pr merge <PR#> --merge --delete-branch=false
git checkout dev && git pull origin dev
git branch -D feature1 && git push origin --delete feature1
git checkout -b feature1 && git push origin feature1

# ❌ INCORRECT - DO NOT DO THIS
git checkout dev
git merge feature1
git push origin dev  # ❌ FORBIDDEN! Bypasses PR workflow!
```

**Example 3: User says "merge feature1 to main" (bypassing dev)**
```bash
# ✅ CORRECT - Merge feature1 → main via PR (rare, but valid for hotfixes)
git checkout feature1
git add .
git commit -m "hotfix: critical security patch"
git push origin feature1
gh pr create --title "hotfix: critical security patch" --body "Description" --head feature1 --base main
gh pr checks <PR#> --watch
gh pr merge <PR#> --merge --delete-branch=false
git checkout main && git pull origin main
git branch -D feature1 && git push origin --delete feature1
git checkout -b feature1 && git push origin feature1
```

---

## DO NOT

1. ❌ Commit files containing secrets (`.env`, `credentials.json`, etc.)
2. ❌ Use `pip` directly - always use `uv`
3. ❌ Edit `uv.lock` manually - regenerate with `uv lock`
4. ❌ Skip type annotations on functions
5. ❌ Use relative imports for application code
6. ❌ Ignore linter errors without good reason
7. ❌ Expose passwords in `__repr__`, `__str__`, or logs
8. ❌ **Perform Git operations (commit, push, tag, merge) without explicit user request**
9. ❌ **Push directly to target branches - ALWAYS use the PR workflow documented above**
10. ❌ **Bypass CI checks or merge PRs before all checks pass**

---

## Quick Reference Card

| Task | Command |
|------|---------|
| Install deps | `uv sync` |
| Add dependency | `uv add <package>` |
| Add dev dependency | `uv add --group testing <package>` |
| Run linter | `uv run ruff check .` |
| Auto-fix lint | `uv run ruff check --fix .` |
| Format code | `uv run ruff format .` |
| Type check (mypy) | `uv run mypy src` |
| Type check (pyright) | `uv run pyright src` |
| Run all tests | `uv run pytest` |
| Run single test | `uv run pytest tests/test_database.py::test_url_generation_sqlite` |
| Run tests (no cov) | `uv run pytest --no-cov` |
| Start Dagster UI | `uv run dagster dev` |
| Validate defs | `uv run dagster definitions validate` |

---

**Last Updated:** 2026-01-05  
**Dagster Module:** `src.definitions`  
**Python Version:** 3.14+
