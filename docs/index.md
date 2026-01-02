# Dagster Crypto Data Pipeline

Welcome to the **Dagster Crypto Data Pipeline** documentation. This project orchestrates the extraction, transformation, and loading (ETL) of cryptocurrency market data from centralized exchanges into a structured data warehouse.

## 🎯 Project Overview

This Dagster code location implements a production-grade data pipeline that:

1. **Extracts** market data (OHLCV, tickers, order books) from cryptocurrency exchanges (Binance, ByBit, Gate.io, etc.)
2. **Loads** raw JSON data into **MinIO** object storage for data lake persistence
3. **Transforms** raw data into structured, validated formats using **Narwhals/Polars**
4. **Stores** transformed data in **PostgreSQL** for analytics and reporting

## 🏗️ Architecture

The pipeline follows a **medallion architecture** pattern with clear separation of concerns:

```
┌─────────────────┐
│   Exchanges     │  Binance, ByBit, Gate.io, etc.
│  (Public APIs)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Extract Assets │  CCXT-based extraction with retry logic
│   (Bronze)      │  Pydantic validation, error handling
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  MinIO Storage  │  Raw JSON data (immutable, versioned)
│   (Data Lake)   │  S3-compatible object storage
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Transform Assets│  Narwhals/Polars transformations
│   (Silver)      │  Data quality checks, schema validation
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   PostgreSQL    │  Structured relational data (Gold)
│  (Data Warehouse)│  Analytics-ready tables
└─────────────────┘
```

See the [Architecture](architecture.md) page for detailed diagrams and design decisions.

## ✨ Key Features

- **Type-Safe**: Comprehensive Pydantic models with validation
- **Tested**: 110+ unit and integration tests with >90% coverage
- **Observable**: Structured logging with contextual information
- **Resilient**: Retry logic, error handling, and graceful degradation
- **Scalable**: Kubernetes-native with K8sRunLauncher for distributed execution
- **Maintainable**: Clean code, comprehensive documentation, and best practices

## 🚀 Quick Start

### Prerequisites

- Python 3.14+
- PostgreSQL 14+
- MinIO (or S3-compatible storage)
- Kubernetes cluster (for production deployment)

### Local Development

```bash
# Clone the repository
git clone <repository-url>
cd dagster-code-location-crypto-data

# Install dependencies using uv
uv sync

# Set up environment variables
cp .env.example .env
# Edit .env with your configuration

# Run tests
uv run pytest

# Start Dagster development server
uv run dagster dev
```

Visit http://localhost:3000 to access the Dagster UI.

### Running the Pipeline

1. **Manual Execution**: Click "Materialize All" in the Dagster UI
2. **Scheduled Execution**: Configure sensors/schedules in `src/definitions.py`
3. **CLI Execution**: `uv run dagster asset materialize --select extract_binance_ohlcv`

## 📚 Documentation Structure

- **[Architecture](architecture.md)**: System design, data flow, and infrastructure
- **[Setup Guide](setup.md)**: Environment setup and configuration
- **[Development Guide](development.md)**: Development workflow, testing, and best practices
- **[Deployment Guide](deployment.md)**: Kubernetes deployment and operations
- **[API Reference](api/index.md)**: Code documentation and data contracts
- **[Troubleshooting](troubleshooting.md)**: Common issues and solutions

## 🛠️ Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Orchestration** | Dagster 1.12+ | Pipeline orchestration and scheduling |
| **Data Processing** | Narwhals + Polars | Fast, memory-efficient DataFrame operations |
| **Data Validation** | Pydantic 2.12+ | Type-safe data models and validation |
| **Exchange API** | CCXT 4.5+ | Unified cryptocurrency exchange API |
| **Object Storage** | MinIO / S3 | Raw data persistence (data lake) |
| **Database** | PostgreSQL 14+ | Structured data warehouse |
| **ORM** | SQLModel 0.0.30+ | Type-safe database models |
| **Testing** | Pytest 9.0+ | Unit and integration testing |
| **Logging** | Structlog | Structured, contextual logging |
| **Container Runtime** | Kubernetes | Production deployment and scaling |

## 📊 Pipeline Assets

### Extract Assets (Bronze Layer)

- `extract_binance_ohlcv`: Extract OHLCV candles from Binance
- `extract_bybit_tickers`: Extract ticker data from ByBit
- `extract_gateio_orderbook`: Extract order book snapshots from Gate.io

### Transform Assets (Silver Layer)

- `transform_ohlcv`: Clean and validate OHLCV data
- `transform_tickers`: Normalize ticker data across exchanges
- `transform_orderbook`: Process order book snapshots

### Load Assets (Gold Layer)

- `load_ohlcv_to_postgres`: Load OHLCV data to warehouse
- `load_tickers_to_postgres`: Load ticker data to warehouse

## 🔒 Security & Compliance

- **Secrets Management**: Environment variables, Kubernetes secrets, SealedSecrets
- **Least Privilege**: IAM roles and service accounts with minimal permissions
- **Encryption**: Data encrypted at rest (MinIO) and in transit (TLS)
- **Audit Logging**: All pipeline runs logged with metadata
- **Data Quality**: Validation at every stage to ensure data integrity

## 📈 Monitoring & Observability

- **Dagster UI**: Real-time pipeline monitoring and execution history
- **Structured Logs**: JSON-formatted logs with correlation IDs
- **Metrics**: Pipeline duration, success/failure rates, data volume
- **Alerts**: Configurable alerts for pipeline failures and data quality issues

## 🤝 Contributing

See [Development Guide](development.md) for:
- Code style guidelines (PEP 8, Ruff, Black)
- Testing requirements (unit, integration, coverage)
- Pull request process
- Commit message conventions

## 📝 License

This project is licensed under the Apache License 2.0. See [LICENSE](../LICENSE) for details.

## 🔗 Related Resources

- [Dagster Documentation](https://docs.dagster.io/)
- [CCXT Documentation](https://docs.ccxt.com/)
- [Narwhals Documentation](https://narwhals-dev.github.io/narwhals/)
- [Pydantic Documentation](https://docs.pydantic.dev/)

---

**Last Updated**: 2026-01-01  
**Version**: 0.1.0  
**Status**: Active Development
