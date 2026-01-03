# Troubleshooting

Common issues and solutions for the Dagster Crypto Data Pipeline.

## Table of Contents

- [Installation Issues](#installation-issues)
- [Database Issues](#database-issues)
- [MinIO/S3 Issues](#minios3-issues)
- [Dagster Issues](#dagster-issues)
- [Kubernetes Issues](#kubernetes-issues)
- [Pipeline Execution Issues](#pipeline-execution-issues)
- [Performance Issues](#performance-issues)

## Installation Issues

### uv sync fails

**Symptom**: `uv sync` fails with dependency resolution errors

**Solution**:

```bash
# Clear uv cache
uv cache clean

# Remove lock file and regenerate
rm uv.lock
uv sync

# If still failing, check Python version
python --version  # Should be 3.14+
```

### Import errors after installation

**Symptom**: `ModuleNotFoundError` when running code

**Solution**:

```bash
# Ensure virtual environment is activated
source .venv/bin/activate

# Reinstall in editable mode
uv sync --force

# Verify installation
uv run python -c "import dagster_crypto_data; print('OK')"
```

## Database Issues

### Cannot connect to PostgreSQL

**Symptom**: `psycopg2.OperationalError: could not connect to server`

**Diagnosis**:

```bash
# Check if PostgreSQL is running
pg_isready -h localhost -p 5432

# Test connection manually
psql -h localhost -U crypto_user -d crypto_data -c "SELECT 1;"
```

**Solutions**:

1. **PostgreSQL not running**:
   ```bash
   # macOS
   brew services start postgresql@14
   
   # Linux
   sudo systemctl start postgresql
   
   # Docker
   docker start postgres-crypto
   ```

2. **Wrong credentials**:
   ```bash
   # Check .env file
   cat .env | grep POSTGRESQL
   
   # Reset password
   psql -d postgres -c "ALTER USER crypto_user WITH PASSWORD 'new_password';"
   ```

3. **Database doesn't exist**:
   ```bash
   # Create database
   createdb crypto_data
   
   # Grant permissions
   psql -d crypto_data -c "GRANT ALL PRIVILEGES ON DATABASE crypto_data TO crypto_user;"
   ```

### Database schema errors

**Symptom**: `sqlalchemy.exc.ProgrammingError: relation "ohlcv" does not exist`

**Solution**:

```bash
# Run migrations
uv run alembic upgrade head

# Or create tables manually
uv run python -c "from dagster_crypto_data.defs.connectors.database import init_db; init_db()"
```

### Database connection pool exhausted

**Symptom**: `sqlalchemy.exc.TimeoutError: QueuePool limit exceeded`

**Solution**:

```python
# Increase pool size in database.py
engine = create_engine(
    url,
    pool_size=20,  # Increase from default 5
    max_overflow=40,  # Increase from default 10
)
```

## MinIO/S3 Issues

### Cannot connect to MinIO

**Symptom**: `botocore.exceptions.EndpointConnectionError`

**Diagnosis**:

```bash
# Check if MinIO is running
curl http://localhost:9000/minio/health/live

# Test with MinIO client
mc ls local/crypto-raw
```

**Solutions**:

1. **MinIO not running**:
   ```bash
   # Start MinIO
   minio server ~/minio-data --console-address ":9001"
   
   # Or with Docker
   docker start minio-crypto
   ```

2. **Wrong endpoint**:
   ```bash
   # Check .env file
   cat .env | grep MINIO_ENDPOINT
   
   # Should be: localhost:9000 (not http://localhost:9000)
   ```

3. **Bucket doesn't exist**:
   ```bash
   # Create bucket
   mc mb local/crypto-raw
   ```

### Access denied errors

**Symptom**: `botocore.exceptions.ClientError: Access Denied`

**Solution**:

```bash
# Check credentials
cat .env | grep MINIO

# Create new access key in MinIO Console
# http://localhost:9001 → Access Keys → Create New Access Key

# Update .env with new credentials
```

### SSL/TLS errors

**Symptom**: `SSL: CERTIFICATE_VERIFY_FAILED`

**Solution**:

```bash
# For local development, disable SSL
# In .env:
MINIO_SECURE=false

# For production, use proper certificates
MINIO_SECURE=true
```

## Dagster Issues

### Dagster definitions not loading

**Symptom**: `dagster.core.errors.DagsterInvariantViolationError`

**Diagnosis**:

```bash
# Validate definitions
uv run dagster definitions validate

# Check for syntax errors
uv run ruff check src

# Check for type errors
uv run mypy src
```

**Solutions**:

1. **Syntax errors**: Fix Python syntax errors
2. **Import errors**: Ensure all dependencies are installed
3. **Type errors**: Fix type annotation issues

### Asset materialization fails

**Symptom**: Asset fails with error in Dagster UI

**Diagnosis**:

```bash
# Check logs in Dagster UI
# Go to: Runs → Select failed run → Logs

# Or check logs in terminal
uv run dagster asset materialize --select <asset_name>
```

**Common causes**:

1. **Network errors**: Check exchange API connectivity
2. **Validation errors**: Check Pydantic model validation
3. **Database errors**: Check PostgreSQL connection
4. **Storage errors**: Check MinIO connection

### Dagster webserver won't start

**Symptom**: `dagster dev` fails to start

**Solution**:

```bash
# Check if port 3000 is already in use
lsof -i :3000

# Kill process using port 3000
kill -9 <PID>

# Or use different port
uv run dagster dev -p 3001
```

### Code location not reloading

**Symptom**: Changes to code not reflected in Dagster UI

**Solution**:

1. **Reload in UI**: Deployment → Code Locations → Reload
2. **Restart server**: Stop `dagster dev` and restart
3. **Clear cache**: Remove `~/.dagster` and restart

## Kubernetes Issues

### Code location pod not starting

**Symptom**: Pod stuck in `Pending` or `CrashLoopBackOff`

**Diagnosis**:

```bash
# Check pod status
kubectl get pods -n dagster -l app=dagster-crypto

# Describe pod
kubectl describe pod -n dagster <pod-name>

# Check logs
kubectl logs -n dagster <pod-name>

# Check events
kubectl get events -n dagster --sort-by='.lastTimestamp'
```

**Common causes**:

1. **ImagePullBackOff**: Image not found in registry
   ```bash
   # Check image exists
   docker pull your-registry/dagster-crypto:latest
   
   # Check image pull secrets
   kubectl get secrets -n dagster
   ```

2. **Insufficient resources**: Not enough CPU/memory
   ```bash
   # Check node resources
   kubectl top nodes
   
   # Reduce resource requests in deployment.yaml
   ```

3. **ConfigMap/Secret missing**:
   ```bash
   # Check ConfigMap
   kubectl get configmap dagster-crypto-config -n dagster
   
   # Check Secret
   kubectl get secret dagster-crypto-secrets -n dagster
   ```

### Job pods not launching

**Symptom**: Runs stuck in "Starting" state

**Diagnosis**:

```bash
# Check job pods
kubectl get pods -n dagster -l dagster/job

# Check K8sRunLauncher logs
kubectl logs -n dagster deployment/dagster-daemon
```

**Solutions**:

1. **Check run launcher config** in `dagster.yaml`
2. **Verify job image** is accessible
3. **Check namespace permissions**

### Network connectivity issues

**Symptom**: Pods can't reach PostgreSQL or MinIO

**Diagnosis**:

```bash
# Test from pod
kubectl exec -n dagster deployment/dagster-crypto-location -- \
  ping postgresql.database.svc.cluster.local

# Test DNS resolution
kubectl exec -n dagster deployment/dagster-crypto-location -- \
  nslookup postgresql.database.svc.cluster.local
```

**Solutions**:

1. **Check service names** in ConfigMap
2. **Verify network policies** allow traffic
3. **Check firewall rules**

## Pipeline Execution Issues

### Exchange API rate limiting

**Symptom**: `ccxt.RateLimitExceeded` errors

**Solution**:

```python
# Add rate limiting in extract assets
import time

def extract_with_retry(exchange, symbol):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            return exchange.fetch_ohlcv(symbol)
        except ccxt.RateLimitExceeded:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                raise
```

### Data validation errors

**Symptom**: `pydantic.ValidationError` during transformation

**Diagnosis**:

```python
# Check raw data
print(raw_data)

# Validate manually
from dagster_crypto_data.defs.defs.models import OHLCVData
try:
    OHLCVData(**raw_data)
except ValidationError as e:
    print(e.errors())
```

**Solutions**:

1. **Fix data source**: Update API call
2. **Update model**: Adjust Pydantic model validation
3. **Add data cleaning**: Clean data before validation

### Memory errors

**Symptom**: `MemoryError` or pod OOMKilled

**Solutions**:

1. **Process data in chunks**:
   ```python
   # Instead of loading all data at once
   for chunk in read_json_chunks(file_path, chunk_size=1000):
       process_chunk(chunk)
   ```

2. **Increase memory limits**:
   ```yaml
   # In deployment.yaml
   resources:
     limits:
       memory: 4Gi  # Increase from 1Gi
   ```

3. **Use streaming**:
   ```python
   # Use Polars lazy API
   import polars as pl
   df = pl.scan_parquet("data.parquet").filter(...).collect()
   ```

## Performance Issues

### Slow transformations

**Symptom**: Transform assets take too long

**Solutions**:

1. **Use Polars instead of Pandas**:
   ```python
   # Already using Narwhals with Polars backend ✓
   ```

2. **Optimize queries**:
   ```python
   # Use lazy evaluation
   df = nw.from_native(pl.scan_parquet("data.parquet"))
   result = df.filter(...).select(...).collect()
   ```

3. **Add indexes**:
   ```sql
   CREATE INDEX idx_ohlcv_timestamp ON ohlcv(timestamp);
   CREATE INDEX idx_ohlcv_symbol ON ohlcv(symbol);
   ```

### Slow database writes

**Symptom**: Loading data to PostgreSQL is slow

**Solutions**:

1. **Use bulk inserts**:
   ```python
   # Instead of inserting one row at a time
   session.bulk_insert_mappings(OHLCV, data)
   session.commit()
   ```

2. **Disable indexes during load**:
   ```sql
   DROP INDEX idx_ohlcv_timestamp;
   -- Load data
   CREATE INDEX idx_ohlcv_timestamp ON ohlcv(timestamp);
   ```

3. **Use COPY instead of INSERT**:
   ```python
   # Use PostgreSQL COPY command
   from io import StringIO
   csv_buffer = StringIO()
   df.to_csv(csv_buffer, index=False)
   csv_buffer.seek(0)
   cursor.copy_from(csv_buffer, 'ohlcv', sep=',')
   ```

## Getting Help

If you can't resolve the issue:

1. **Check logs**: Always check logs first
2. **Search issues**: Search GitHub issues for similar problems
3. **Ask for help**:
   - Dagster Slack: https://dagster.io/slack
   - GitHub Issues: <repository-url>/issues
   - Stack Overflow: Tag with `dagster`

### Providing Information

When asking for help, include:

- **Error message**: Full error traceback
- **Environment**: Python version, OS, Dagster version
- **Steps to reproduce**: Minimal example that reproduces the issue
- **Logs**: Relevant logs from Dagster UI or kubectl
- **Configuration**: Relevant parts of `.env`, `dagster.yaml`, etc.

---

**Last Updated**: 2026-01-01
