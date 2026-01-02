# Architecture

This document describes the architecture of the Dagster Crypto Data Pipeline, including system design, data flow, infrastructure, and key design decisions.

## System Architecture

### High-Level Overview

The pipeline is deployed as a **Dagster Code Location** on Kubernetes, using a hybrid deployment strategy:

1. **Code Location Pod**: Long-running gRPC server that defines assets and jobs
2. **Ephemeral Job Pods**: Launched by K8sRunLauncher for each pipeline run
3. **Dagster Instance**: Shared infrastructure (webserver, daemon, PostgreSQL)

![Architecture Diagram](diagrams/architecture.svg)

### Architecture Diagram (D2)

The complete architecture diagram is available in [diagrams/architecture.d2](diagrams/architecture.d2).

Key components:

- **Public Internet**: Cryptocurrency exchanges (Binance, ByBit, Gate.io, etc.)
- **Kubernetes Cluster**: Dagster instance, code locations, and job pods
- **PostgreSQL**: Dagster metadata, logs, and transformed data
- **MinIO (LXC)**: Object storage for raw JSON data

## Data Flow

### Extract → Load → Transform (ELT Pattern)

We use an **ELT (Extract, Load, Transform)** pattern rather than traditional ETL:

```
┌──────────────────────────────────────────────────────────────┐
│                     EXTRACT (Bronze)                         │
│  - Fetch data from exchange APIs (CCXT)                     │
│  - Minimal transformation (JSON serialization)               │
│  - Pydantic validation for API responses                    │
│  - Error handling and retry logic                           │
└──────────────────┬───────────────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────────────┐
│                      LOAD (Bronze)                           │
│  - Write raw JSON to MinIO (S3-compatible)                  │
│  - Partitioned by: exchange/symbol/date/timestamp.json      │
│  - Immutable, versioned data lake                           │
│  - Enables data replay and auditing                         │
└──────────────────┬───────────────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────────────┐
│                   TRANSFORM (Silver)                         │
│  - Read JSON from MinIO                                     │
│  - Parse and validate with Pydantic                         │
│  - Transform with Narwhals/Polars (fast, memory-efficient) │
│  - Data quality checks (nulls, ranges, types)              │
│  - Schema evolution handling                                │
└──────────────────┬───────────────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────────────┐
│                     LOAD (Gold)                              │
│  - Write to PostgreSQL (SQLModel ORM)                       │
│  - Normalized relational schema                             │
│  - Indexes for query performance                            │
│  - Analytics-ready tables                                   │
└──────────────────────────────────────────────────────────────┘
```

### Why ELT Instead of ETL?

1. **Immutable Raw Data**: Original data preserved in MinIO for replay/audit
2. **Flexibility**: Can re-transform data without re-extracting from APIs
3. **Separation of Concerns**: Extract logic independent of transformation logic
4. **Cost Efficiency**: Avoid rate limits by caching raw data
5. **Data Lineage**: Clear provenance from raw → transformed → warehouse

## Medallion Architecture

We implement a **medallion architecture** with three layers:

### Bronze Layer (Raw Data)

- **Storage**: MinIO object storage
- **Format**: JSON (as received from APIs)
- **Schema**: Flexible, schema-on-read
- **Purpose**: Immutable source of truth

**Example Path**:
```
s3://crypto-raw/binance/BTC-USDT/ohlcv/2026/01/01/1735689600000.json
```

### Silver Layer (Cleaned Data)

- **Storage**: In-memory (Polars DataFrames)
- **Format**: Validated Pydantic models
- **Schema**: Strict, enforced by Pydantic
- **Purpose**: Cleaned, validated, ready for analytics

**Transformations**:
- Remove duplicates
- Handle nulls and missing values
- Type conversions and validation
- Data quality checks

### Gold Layer (Analytics-Ready)

- **Storage**: PostgreSQL
- **Format**: Relational tables (SQLModel)
- **Schema**: Normalized, indexed
- **Purpose**: Optimized for queries and reporting

**Tables**:
- `ohlcv`: OHLCV candles (time-series)
- `tickers`: Ticker snapshots
- `order_books`: Order book snapshots

## Infrastructure

### Kubernetes Deployment

```yaml
# Deployment: Code Location (gRPC server)
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dagster-crypto-location
  namespace: dagster
spec:
  replicas: 1
  template:
    spec:
      containers:
      - name: dagster-crypto
        image: your-registry/dagster-crypto:latest
        resources:
          requests:
            cpu: 100m
            memory: 256Mi
          limits:
            cpu: 500m
            memory: 512Mi
```

### K8sRunLauncher Configuration

Ephemeral job pods are launched with:

```python
# dagster.yaml (Dagster instance configuration)
run_launcher:
  module: dagster_k8s
  class: K8sRunLauncher
  config:
    job_image: your-registry/dagster-crypto:latest
    image_pull_policy: Always
    env_config_maps:
      - dagster-crypto-config
    env_secrets:
      - dagster-crypto-secrets
```

### Resource Allocation

| Component | CPU Request | Memory Request | CPU Limit | Memory Limit |
|-----------|-------------|----------------|-----------|--------------|
| Code Location | 100m | 256Mi | 500m | 512Mi |
| Extract Job | 200m | 512Mi | 1000m | 1Gi |
| Transform Job | 500m | 1Gi | 2000m | 4Gi |

## Data Models

### Pydantic Models (Validation)

```python
from pydantic import BaseModel, Field

class OHLCVData(BaseModel):
    """OHLCV candle data from exchange."""
    timestamp: int = Field(..., description="Unix timestamp (ms)")
    open: float = Field(..., gt=0)
    high: float = Field(..., gt=0)
    low: float = Field(..., gt=0)
    close: float = Field(..., gt=0)
    volume: float = Field(..., ge=0)
    exchange: str
    symbol: str
```

### SQLModel Models (Database)

```python
from sqlmodel import SQLModel, Field

class OHLCV(SQLModel, table=True):
    """OHLCV table in PostgreSQL."""
    __tablename__ = "ohlcv"
    
    id: int | None = Field(default=None, primary_key=True)
    timestamp: int = Field(index=True)
    open: float
    high: float
    low: float
    close: float
    volume: float
    exchange: str = Field(index=True)
    symbol: str = Field(index=True)
```

## Design Decisions

### ADR-001: Use Narwhals Instead of Pandas

**Status**: Accepted  
**Date**: 2025-12-30

**Context**: Need fast, memory-efficient DataFrame library for transformations.

**Decision**: Use Narwhals with Polars backend instead of Pandas.

**Rationale**:
- **Performance**: Polars is 5-10x faster than Pandas for most operations
- **Memory**: Polars uses ~50% less memory (columnar storage)
- **Portability**: Narwhals provides Pandas-like API with multiple backends
- **Type Safety**: Better type inference and validation

**Consequences**:
- Learning curve for developers familiar with Pandas
- Smaller ecosystem compared to Pandas
- Need to ensure Polars compatibility for all operations

### ADR-002: Use MinIO for Raw Data Storage

**Status**: Accepted  
**Date**: 2025-12-27

**Context**: Need object storage for raw JSON data.

**Decision**: Use MinIO (S3-compatible) instead of PostgreSQL JSONB.

**Rationale**:
- **Scalability**: Object storage scales better than JSONB columns
- **Cost**: Cheaper storage for large volumes of raw data
- **Immutability**: S3 versioning provides data lineage
- **Portability**: S3-compatible, can migrate to AWS S3 if needed

**Consequences**:
- Additional infrastructure component to manage
- Network latency for data access
- Need to implement partitioning strategy

### ADR-003: Use K8sRunLauncher for Job Execution

**Status**: Accepted  
**Date**: 2025-12-14

**Context**: Need scalable, isolated job execution.

**Decision**: Use K8sRunLauncher instead of in-process execution.

**Rationale**:
- **Isolation**: Each job runs in its own pod (resource isolation)
- **Scalability**: Can run multiple jobs in parallel
- **Resilience**: Job failures don't affect code location
- **Resource Management**: Fine-grained resource allocation per job

**Consequences**:
- Increased complexity (Kubernetes required)
- Slower startup time (pod creation overhead)
- Need to manage Docker images and registries

## Security Architecture

### Secrets Management

```
┌─────────────────────────────────────────────────────────┐
│  Developer Workstation                                  │
│  - .env file (local development, NOT committed)         │
│  - sops for encryption                                  │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│  Kubernetes Cluster                                     │
│  - SealedSecrets (encrypted in Git)                    │
│  - Kubernetes Secrets (runtime)                        │
│  - Environment variables injected into pods            │
└─────────────────────────────────────────────────────────┘
```

### Network Security

- **TLS**: All external communication encrypted (HTTPS, TLS)
- **Network Policies**: Restrict pod-to-pod communication
- **Service Mesh**: (Future) Istio for mTLS and observability

### Access Control

- **RBAC**: Kubernetes role-based access control
- **Service Accounts**: Minimal permissions per component
- **IAM Roles**: (Future) AWS IAM for S3 access

## Monitoring & Observability

### Logging

- **Structured Logging**: JSON-formatted logs with correlation IDs
- **Log Aggregation**: Logs sent to PostgreSQL (Dagster logs table)
- **Log Levels**: DEBUG (dev), INFO (prod), ERROR (alerts)

### Metrics

- **Pipeline Metrics**: Success rate, duration, data volume
- **System Metrics**: CPU, memory, disk usage (Kubernetes metrics)
- **Custom Metrics**: (Future) Prometheus metrics for business KPIs

### Tracing

- **Dagster UI**: Built-in execution tracing and lineage
- **Distributed Tracing**: (Future) OpenTelemetry for cross-service tracing

## Scalability Considerations

### Current Scale

- **Exchanges**: 3-5 exchanges
- **Symbols**: 10-50 trading pairs
- **Data Volume**: ~1GB/day raw data
- **Pipeline Runs**: 100-500 runs/day

### Future Scale (Target)

- **Exchanges**: 10-20 exchanges
- **Symbols**: 100-500 trading pairs
- **Data Volume**: ~10GB/day raw data
- **Pipeline Runs**: 1000-5000 runs/day

### Scaling Strategy

1. **Horizontal Scaling**: Increase K8s job pod replicas
2. **Partitioning**: Partition data by exchange/symbol/date
3. **Caching**: Cache frequently accessed data (Redis)
4. **Incremental Processing**: Only process new/changed data
5. **Distributed Processing**: (Future) Spark for large-scale transformations

## Disaster Recovery

### Backup Strategy

- **PostgreSQL**: Daily pg_dump backups, 30-day retention
- **MinIO**: S3 versioning enabled, 90-day retention
- **Kubernetes**: GitOps (manifests in Git), infrastructure as code

### Recovery Procedures

1. **Data Loss**: Restore from MinIO (raw data) and re-transform
2. **Database Corruption**: Restore from pg_dump backup
3. **Cluster Failure**: Redeploy from Git (infrastructure as code)

### RTO/RPO Targets

- **RTO (Recovery Time Objective)**: 4 hours
- **RPO (Recovery Point Objective)**: 24 hours

---

**Last Updated**: 2026-01-01  
**Reviewed By**: Staff Data Engineer Agent
