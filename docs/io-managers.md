# IO Managers

IO Managers handle how Dagster assets store and load data. This project provides multiple IO managers for different environments and use cases.

## Overview

Assets in this project follow a simple contract:
- **Extract assets** return `dict[str, Any]`
- **Transform assets** return `polars.DataFrame`

IO Managers handle the storage and retrieval of these data types, allowing you to switch backends without changing asset code.

## Available IO Managers

### 1. FilesystemIOManager

**Purpose**: Local development and testing  
**Storage**: JSON files on local filesystem  
**Best For**: Quick prototyping, debugging

```python
from dagster import Definitions
from dagster_crypto_data.io_managers import FilesystemIOManager

defs = Definitions(
    assets=[...],
    resources={
        "extract_io_manager": FilesystemIOManager(base_path="./data/raw"),
        "transform_io_manager": FilesystemIOManager(base_path="./data/processed"),
    },
)
```

**Features**:
- Stores dicts as JSON files
- Automatic directory creation
- Human-readable output for debugging

---

### 2. S3IOManager

**Purpose**: Production object storage (MinIO, AWS S3)  
**Storage**: JSON files in S3-compatible storage  
**Best For**: Production raw data storage, data lake

```python
from dagster import Definitions, EnvVar
from dagster_crypto_data.io_managers import S3IOManager

# MinIO (local/staging)
defs = Definitions(
    assets=[...],
    resources={
        "extract_io_manager": S3IOManager(
            endpoint_url="http://localhost:9000",
            access_key=EnvVar("MINIO_ACCESS_KEY"),
            secret_key=EnvVar("MINIO_SECRET_KEY"),
            bucket="crypto-raw",
            use_ssl=False,
        ),
    },
)

# AWS S3 (production)
defs = Definitions(
    assets=[...],
    resources={
        "extract_io_manager": S3IOManager(
            endpoint_url=None,  # Use default AWS S3
            access_key=EnvVar("AWS_ACCESS_KEY_ID"),
            secret_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
            bucket="my-production-bucket",
            region="us-west-2",
        ),
    },
)
```

**Features**:
- S3-compatible (MinIO, AWS S3, DigitalOcean Spaces, etc.)
- Automatic bucket creation
- Versioning support (if enabled on bucket)

---

### 3. DuckDBIOManager

**Purpose**: Local analytics and testing  
**Storage**: DuckDB database file  
**Best For**: Local development, fast analytics

```python
from dagster import Definitions
from dagster_crypto_data.io_managers import DuckDBIOManager

defs = Definitions(
    assets=[...],
    resources={
        "transform_io_manager": DuckDBIOManager(
            db_path="./data/crypto.duckdb",
            schema="analytics",
        ),
    },
)
```

**Features**:
- In-process analytical database
- Fast columnar storage
- SQL query support
- Polars integration

---

### 4. KuzuDBIOManager

**Purpose**: Graph database experimentation  
**Storage**: KuzuDB embedded graph database  
**Best For**: Testing graph queries, relationship analysis

```python
from dagster import Definitions
from dagster_crypto_data.io_managers import KuzuDBIOManager

defs = Definitions(
    assets=[...],
    resources={
        "transform_io_manager": KuzuDBIOManager(
            db_path="./data/graph.kuzu",
        ),
    },
)
```

**Features**:
- Embedded graph database
- Cypher-like query language
- OLAP-style graph analytics
- Experimental for testing graph capabilities

**Example Usage**:
```python
import kuzu

# After materializing assets
db = kuzu.Database("./data/graph.kuzu")
conn = kuzu.Connection(db)

# Query graph
result = conn.execute(
    "MATCH (n:ohlcv_data) WHERE n.symbol = 'BTC/USDT' RETURN n"
)
print(result.get_as_df())
```

---

### 5. SQLIOManager

**Purpose**: Production relational database storage  
**Storage**: PostgreSQL or SQLite  
**Best For**: Production data warehouse, analytics

```python
from dagster import Definitions, EnvVar
from dagster_crypto_data.io_managers import SQLIOManager

# PostgreSQL (production)
defs = Definitions(
    assets=[...],
    resources={
        "transform_io_manager": SQLIOManager(
            db_type="postgresql",
            host=EnvVar("POSTGRESQL_HOST"),
            port=5432,
            db_name=EnvVar("POSTGRESQL_DATABASE"),
            username=EnvVar("POSTGRESQL_USER"),
            password=EnvVar("POSTGRESQL_PASSWORD"),
            schema="analytics",
        ),
    },
)

# SQLite (local)
defs = Definitions(
    assets=[...],
    resources={
        "transform_io_manager": SQLIOManager(
            db_type="sqlite",
            db_name="./data/local",
        ),
    },
)
```

**Features**:
- Supports PostgreSQL and SQLite
- Uses existing DatabaseManagement connector
- ACID transactions
- Production-ready

---

## Environment-Specific Configuration

### Development Environment

```python
# Local development with filesystem and DuckDB
io_managers = {
    "extract_io_manager": FilesystemIOManager(base_path="./data/raw"),
    "transform_io_manager": DuckDBIOManager(db_path="./data/local.duckdb"),
}
```

### Staging Environment

```python
# Staging with MinIO and PostgreSQL
io_managers = {
    "extract_io_manager": S3IOManager(
        endpoint_url="http://minio.staging:9000",
        access_key=EnvVar("MINIO_ACCESS_KEY"),
        secret_key=EnvVar("MINIO_SECRET_KEY"),
        bucket="crypto-raw-staging",
        use_ssl=False,
    ),
    "transform_io_manager": SQLIOManager(
        db_type="postgresql",
        host="postgres.staging",
        port=5432,
        db_name="crypto_staging",
        username=EnvVar("POSTGRESQL_USER"),
        password=EnvVar("POSTGRESQL_PASSWORD"),
    ),
}
```

### Production Environment

```python
# Production with AWS S3 and PostgreSQL
io_managers = {
    "extract_io_manager": S3IOManager(
        endpoint_url=None,  # AWS S3
        access_key=EnvVar("AWS_ACCESS_KEY_ID"),
        secret_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
        bucket="crypto-raw-prod",
        region="us-west-2",
    ),
    "transform_io_manager": SQLIOManager(
        db_type="postgresql",
        host=EnvVar("POSTGRESQL_HOST"),
        port=5432,
        db_name="crypto_prod",
        username=EnvVar("POSTGRESQL_USER"),
        password=EnvVar("POSTGRESQL_PASSWORD"),
        schema="public",
    ),
}
```

---

## Using IO Managers in Assets

### Extract Asset (Returns Dict)

```python
from dagster import asset, AssetExecutionContext

@asset(
    io_manager_key="extract_io_manager",  # Uses extract IO manager
)
def extract_binance_ohlcv(context: AssetExecutionContext) -> dict[str, Any]:
    """Extract OHLCV data - IO manager handles storage."""
    data = fetch_from_binance()
    return {"data": data, "metadata": {"source": "binance", "timestamp": ...}}
```

### Transform Asset (Returns DataFrame)

```python
import polars as pl
from dagster import asset, AssetExecutionContext

@asset(
    io_manager_key="transform_io_manager",  # Uses transform IO manager
)
def transform_ohlcv(
    context: AssetExecutionContext,
    extract_binance_ohlcv: dict[str, Any],  # Loaded by extract_io_manager
) -> pl.DataFrame:
    """Transform data - IO manager handles storage."""
    df = pl.DataFrame(extract_binance_ohlcv["data"])
    # Transform logic
    return df
```

---

## Testing with In-Memory IO Manager

For unit tests, you can create an in-memory IO manager:

```python
from dagster import ConfigurableIOManager, InputContext, OutputContext

class InMemoryIOManager(ConfigurableIOManager):
    """In-memory IO manager for testing."""
    
    def __init__(self):
        self.storage = {}
    
    def handle_output(self, context: OutputContext, obj):
        key = "/".join(context.asset_key.path)
        self.storage[key] = obj
    
    def load_input(self, context: InputContext):
        key = "/".join(context.asset_key.path)
        return self.storage[key]

# Use in tests
defs = Definitions(
    assets=[...],
    resources={"io_manager": InMemoryIOManager()},
)
```

---

## Comparison Matrix

| IO Manager | Storage | Use Case | Pros | Cons |
|------------|---------|----------|------|------|
| **FilesystemIOManager** | Local files | Development | Simple, debuggable | Not scalable |
| **S3IOManager** | S3/MinIO | Production raw data | Scalable, versioned | Network latency |
| **DuckDBIOManager** | DuckDB file | Local analytics | Fast, SQL support | Single-node only |
| **KuzuDBIOManager** | KuzuDB | Graph experiments | Graph queries | Experimental |
| **SQLIOManager** | PostgreSQL/SQLite | Production warehouse | ACID, mature | Requires DB server |

---

## Best Practices

1. **Use different IO managers for different layers**:
   - Extract → S3IOManager (raw data lake)
   - Transform → SQLIOManager (analytics warehouse)

2. **Match IO manager to data type**:
   - Dicts → FilesystemIOManager or S3IOManager
   - DataFrames → DuckDBIOManager or SQLIOManager

3. **Environment-specific configuration**:
   - Development: Filesystem + DuckDB
   - Staging: MinIO + PostgreSQL
   - Production: S3 + PostgreSQL

4. **Test with lightweight IO managers**:
   - Use FilesystemIOManager or InMemoryIOManager for tests
   - Avoid network dependencies in unit tests

---

**Last Updated**: 2026-01-02  
**Related**: [Architecture](architecture.md), [Development Guide](development.md)
