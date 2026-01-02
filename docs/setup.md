# Setup Guide

This guide walks you through setting up the Dagster Crypto Data Pipeline for local development and production deployment.

## Prerequisites

### Required Software

- **Python**: 3.14 or higher
- **uv**: Package manager (recommended) or pip
- **PostgreSQL**: 14 or higher
- **MinIO**: S3-compatible object storage (or AWS S3)
- **Docker**: For containerization (production)
- **Kubernetes**: For production deployment (optional)

### Optional Tools

- **kubectl**: Kubernetes CLI
- **kustomize**: Kubernetes manifest management
- **sops**: Secrets encryption
- **d2**: Diagram generation

## Local Development Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd dagster-code-location-crypto-data
```

### 2. Install Dependencies

Using **uv** (recommended):

```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install project dependencies
uv sync

# Install development dependencies
uv sync --group testing
```

Using **pip**:

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -e .
pip install -e ".[testing]"
```

### 3. Set Up PostgreSQL

#### Option A: Local PostgreSQL

```bash
# Install PostgreSQL (macOS)
brew install postgresql@14
brew services start postgresql@14

# Create database and user
createdb crypto_data
psql -d crypto_data -c "CREATE USER crypto_user WITH PASSWORD 'your_password';"
psql -d crypto_data -c "GRANT ALL PRIVILEGES ON DATABASE crypto_data TO crypto_user;"
```

#### Option B: Docker PostgreSQL

```bash
docker run -d \
  --name postgres-crypto \
  -e POSTGRES_DB=crypto_data \
  -e POSTGRES_USER=crypto_user \
  -e POSTGRES_PASSWORD=your_password \
  -p 5432:5432 \
  postgres:14
```

### 4. Set Up MinIO

#### Option A: Local MinIO

```bash
# Install MinIO (macOS)
brew install minio/stable/minio

# Start MinIO server
minio server ~/minio-data --console-address ":9001"

# Access MinIO Console at http://localhost:9001
# Default credentials: minioadmin / minioadmin
```

#### Option B: Docker MinIO

```bash
docker run -d \
  --name minio-crypto \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  -p 9000:9000 \
  -p 9001:9001 \
  minio/minio server /data --console-address ":9001"
```

Create bucket:

```bash
# Install MinIO client
brew install minio/stable/mc

# Configure MinIO client
mc alias set local http://localhost:9000 minioadmin minioadmin

# Create bucket
mc mb local/crypto-raw
```

### 5. Configure Environment Variables

Create `.env` file in project root:

```bash
# Database Configuration
POSTGRESQL_HOST=localhost
POSTGRESQL_PORT=5432
POSTGRESQL_USER=crypto_user
POSTGRESQL_PASSWORD=your_password
POSTGRESQL_DATABASE=crypto_data

# MinIO Configuration
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=crypto-raw
MINIO_SECURE=false

# Dagster Configuration
DAGSTER_HOME=~/.dagster
ENVIRONMENT=development

# Logging
LOG_LEVEL=INFO
LOG_JSON=false
```

**⚠️ IMPORTANT**: Never commit `.env` to Git! It's already in `.gitignore`.

### 6. Initialize Database Schema

```bash
# Run Alembic migrations (if available)
uv run alembic upgrade head

# Or manually create tables using SQLModel
uv run python -c "from dagster_crypto_data.connectors.database import init_db; init_db()"
```

### 7. Verify Installation

```bash
# Run tests
uv run pytest

# Check linting
uv run ruff check .

# Check type hints
uv run mypy src

# Validate Dagster definitions
uv run dagster definitions validate
```

### 8. Start Dagster Development Server

```bash
uv run dagster dev
```

Visit http://localhost:3000 to access the Dagster UI.

## Production Setup

### 1. Build Docker Image

```bash
# Build image
docker build -t your-registry/dagster-crypto:latest .

# Test image locally
docker run --rm \
  --env-file .env \
  your-registry/dagster-crypto:latest \
  dagster definitions validate

# Push to registry
docker push your-registry/dagster-crypto:latest
```

### 2. Set Up Kubernetes Secrets

#### Using SealedSecrets (Recommended)

```bash
# Install kubeseal
brew install kubeseal

# Create .env file with production secrets
cat > .env.prod <<EOF
POSTGRESQL_PASSWORD=<strong-password>
MINIO_SECRET_KEY=<strong-secret>
EOF

# Generate SealedSecret
sops exec-env .env.prod "nu create_sealed_secrets.nu"

# Apply to cluster
kubectl apply -f apps/dagster/overlays/prod/sealed-secret.yaml
```

#### Using kubectl (Less Secure)

```bash
kubectl create secret generic dagster-crypto-secrets \
  --from-literal=POSTGRESQL_PASSWORD=<password> \
  --from-literal=MINIO_SECRET_KEY=<secret> \
  -n dagster
```

### 3. Deploy to Kubernetes

```bash
# Deploy using Kustomize
kubectl apply -k apps/dagster/overlays/prod

# Verify deployment
kubectl get pods -n dagster -l app=dagster-crypto

# Check logs
kubectl logs -n dagster -l app=dagster-crypto -f
```

### 4. Verify Deployment

```bash
# Check code location status
kubectl exec -n dagster deployment/dagster-webserver -- \
  dagster code-location list

# Test asset materialization
kubectl exec -n dagster deployment/dagster-webserver -- \
  dagster asset materialize --select extract_binance_ohlcv
```

## Configuration

### Dagster Instance Configuration

Create `~/.dagster/dagster.yaml` for local development:

```yaml
# Storage
storage:
  postgres:
    postgres_url: postgresql://crypto_user:your_password@localhost:5432/crypto_data

# Run Launcher (local)
run_launcher:
  module: dagster.core.launcher
  class: DefaultRunLauncher

# Compute Logs
compute_logs:
  module: dagster.core.storage.local_compute_log_manager
  class: LocalComputeLogManager
  config:
    base_dir: ~/.dagster/logs
```

For production (Kubernetes), use K8sRunLauncher:

```yaml
# Run Launcher (Kubernetes)
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

### Application Configuration

Configuration is managed via Pydantic Settings in `src/dagster_crypto_data/utils/settings.py`:

```python
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Database
    postgresql_host: str = "localhost"
    postgresql_port: int = 5432
    postgresql_user: str = "crypto_user"
    postgresql_password: str
    postgresql_database: str = "crypto_data"
    
    # MinIO
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str
    minio_secret_key: str
    minio_bucket: str = "crypto-raw"
    minio_secure: bool = False
    
    # Logging
    log_level: str = "INFO"
    log_json: bool = False
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
```

## Troubleshooting

### Common Issues

#### 1. Database Connection Errors

```bash
# Test PostgreSQL connection
psql -h localhost -U crypto_user -d crypto_data -c "SELECT 1;"

# Check PostgreSQL logs
tail -f /usr/local/var/log/postgresql@14.log
```

#### 2. MinIO Connection Errors

```bash
# Test MinIO connection
mc ls local/crypto-raw

# Check MinIO logs
docker logs minio-crypto
```

#### 3. Import Errors

```bash
# Ensure virtual environment is activated
source .venv/bin/activate

# Reinstall dependencies
uv sync --force
```

#### 4. Dagster Definition Errors

```bash
# Validate definitions
uv run dagster definitions validate

# Check for syntax errors
uv run ruff check src

# Check for type errors
uv run mypy src
```

### Getting Help

- **Dagster Slack**: https://dagster.io/slack
- **GitHub Issues**: <repository-url>/issues
- **Documentation**: https://docs.dagster.io/

---

**Next Steps**: See [Development Guide](development.md) for development workflow and best practices.
