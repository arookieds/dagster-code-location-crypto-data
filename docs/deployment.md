# Deployment Guide

This guide covers deploying the Dagster Crypto Data Pipeline to production environments, including Kubernetes deployment, secrets management, and operational procedures.

## Deployment Overview

The pipeline uses a **containerized deployment** on Kubernetes with the following components:

- **Code Location Pod**: Long-running gRPC server (1 replica)
- **Ephemeral Job Pods**: Launched by K8sRunLauncher for each run
- **Shared Services**: PostgreSQL, MinIO, Dagster instance

## Prerequisites

- Kubernetes cluster (1.24+)
- kubectl configured with cluster access
- Docker registry access
- Helm 3.x (optional, for Dagster instance)
- kustomize (for manifest management)

## Build and Push Docker Image

### 1. Build Image

```bash
# Build for production
docker build -t your-registry/dagster-crypto:v0.1.0 .

# Tag as latest
docker tag your-registry/dagster-crypto:v0.1.0 your-registry/dagster-crypto:latest

# Test image locally
docker run --rm your-registry/dagster-crypto:v0.1.0 dagster definitions validate
```

### 2. Push to Registry

```bash
# Login to registry
docker login your-registry

# Push versioned tag
docker push your-registry/dagster-crypto:v0.1.0

# Push latest tag
docker push your-registry/dagster-crypto:latest
```

### 3. Verify Image

```bash
# Pull and test
docker pull your-registry/dagster-crypto:v0.1.0
docker run --rm your-registry/dagster-crypto:v0.1.0 python --version
```

## Secrets Management

### Using SealedSecrets (Recommended)

SealedSecrets encrypt secrets so they can be safely stored in Git.

#### 1. Install Sealed Secrets Controller

```bash
# Install controller
kubectl apply -f https://github.com/bitnami-labs/sealed-secrets/releases/download/v0.24.0/controller.yaml

# Install kubeseal CLI
brew install kubeseal
```

#### 2. Create Secrets File

Create `.env.prod` (DO NOT commit):

```bash
POSTGRESQL_PASSWORD=<strong-password>
MINIO_SECRET_KEY=<strong-secret>
EXCHANGE_API_KEY=<api-key>
EXCHANGE_API_SECRET=<api-secret>
```

#### 3. Generate SealedSecret

```bash
# Create Kubernetes secret (not applied)
kubectl create secret generic dagster-crypto-secrets \
  --from-env-file=.env.prod \
  --dry-run=client \
  -o yaml > secret.yaml

# Seal the secret
kubeseal --format=yaml < secret.yaml > sealed-secret.yaml

# Apply sealed secret
kubectl apply -f sealed-secret.yaml -n dagster

# Clean up temporary files
rm secret.yaml .env.prod
```

### Using kubectl Secrets (Less Secure)

```bash
# Create secret directly (not stored in Git)
kubectl create secret generic dagster-crypto-secrets \
  --from-literal=POSTGRESQL_PASSWORD=<password> \
  --from-literal=MINIO_SECRET_KEY=<secret> \
  -n dagster
```

## Kubernetes Deployment

### Directory Structure

```
apps/dagster/
├── base/
│   ├── kustomization.yaml
│   ├── deployment.yaml
│   ├── service.yaml
│   └── configmap.yaml
└── overlays/
    ├── dev/
    │   └── kustomization.yaml
    └── prod/
        ├── kustomization.yaml
        ├── sealed-secret.yaml
        └── resource-limits.yaml
```

### 1. Create ConfigMap

`apps/dagster/base/configmap.yaml`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: dagster-crypto-config
data:
  POSTGRESQL_HOST: postgresql.database.svc.cluster.local
  POSTGRESQL_PORT: "5432"
  POSTGRESQL_DATABASE: crypto_data
  MINIO_ENDPOINT: minio.lxc.svc.cluster.local:9000
  MINIO_BUCKET: crypto-raw
  MINIO_SECURE: "false"
  LOG_LEVEL: INFO
  LOG_JSON: "true"
  ENVIRONMENT: production
```

### 2. Create Deployment

`apps/dagster/base/deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dagster-crypto-location
  labels:
    app: dagster-crypto
spec:
  replicas: 1
  selector:
    matchLabels:
      app: dagster-crypto
  template:
    metadata:
      labels:
        app: dagster-crypto
    spec:
      containers:
      - name: dagster-crypto
        image: your-registry/dagster-crypto:latest
        imagePullPolicy: Always
        ports:
        - containerPort: 3030
          name: grpc
        envFrom:
        - configMapRef:
            name: dagster-crypto-config
        - secretRef:
            name: dagster-crypto-secrets
        resources:
          requests:
            cpu: 100m
            memory: 256Mi
          limits:
            cpu: 500m
            memory: 512Mi
        livenessProbe:
          exec:
            command:
            - dagster
            - api
            - grpc-health-check
            - -p
            - "3030"
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          exec:
            command:
            - dagster
            - api
            - grpc-health-check
            - -p
            - "3030"
          initialDelaySeconds: 10
          periodSeconds: 5
```

### 3. Create Service

`apps/dagster/base/service.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: dagster-crypto-location
spec:
  selector:
    app: dagster-crypto
  ports:
  - port: 3030
    targetPort: 3030
    name: grpc
  type: ClusterIP
```

### 4. Create Kustomization

`apps/dagster/base/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

namespace: dagster

resources:
  - deployment.yaml
  - service.yaml
  - configmap.yaml

commonLabels:
  app.kubernetes.io/name: dagster-crypto
  app.kubernetes.io/component: code-location
```

### 5. Production Overlay

`apps/dagster/overlays/prod/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

namespace: dagster

bases:
  - ../../base

resources:
  - sealed-secret.yaml

patchesStrategicMerge:
  - resource-limits.yaml

images:
  - name: your-registry/dagster-crypto
    newTag: v0.1.0
```

### 6. Deploy to Cluster

```bash
# Validate manifests
kubectl kustomize apps/dagster/overlays/prod

# Apply to cluster
kubectl apply -k apps/dagster/overlays/prod

# Verify deployment
kubectl get pods -n dagster -l app=dagster-crypto
kubectl logs -n dagster -l app=dagster-crypto -f
```

## Dagster Instance Configuration

The Dagster instance (webserver, daemon) must be configured to use K8sRunLauncher.

### dagster.yaml (Kubernetes ConfigMap)

```yaml
# Storage
storage:
  postgres:
    postgres_url:
      env: DAGSTER_POSTGRES_URL

# Run Launcher
run_launcher:
  module: dagster_k8s
  class: K8sRunLauncher
  config:
    job_image: your-registry/dagster-crypto:v0.1.0
    image_pull_policy: Always
    env_config_maps:
      - dagster-crypto-config
    env_secrets:
      - dagster-crypto-secrets
    job_namespace: dagster
    resources:
      requests:
        cpu: 200m
        memory: 512Mi
      limits:
        cpu: 2000m
        memory: 4Gi

# Compute Logs
compute_logs:
  module: dagster_postgres.compute_log_manager
  class: PostgresComputeLogManager
  config:
    postgres_url:
      env: DAGSTER_POSTGRES_URL
```

## Monitoring and Observability

### Health Checks

```bash
# Check code location health
kubectl exec -n dagster deployment/dagster-crypto-location -- \
  dagster api grpc-health-check -p 3030

# Check Dagster instance
kubectl exec -n dagster deployment/dagster-webserver -- \
  dagster code-location list
```

### Logs

```bash
# View code location logs
kubectl logs -n dagster -l app=dagster-crypto -f

# View job pod logs
kubectl logs -n dagster -l dagster/job=extract_binance_ohlcv -f

# View all Dagster logs
kubectl logs -n dagster -l app.kubernetes.io/name=dagster -f --tail=100
```

### Metrics

```bash
# Get pod resource usage
kubectl top pods -n dagster -l app=dagster-crypto

# Get node resource usage
kubectl top nodes
```

## Scaling

### Horizontal Scaling (Code Location)

```bash
# Scale code location replicas (not recommended, use 1 replica)
kubectl scale deployment dagster-crypto-location -n dagster --replicas=1
```

### Vertical Scaling (Job Pods)

Update resource limits in `dagster.yaml`:

```yaml
run_launcher:
  config:
    resources:
      requests:
        cpu: 500m
        memory: 1Gi
      limits:
        cpu: 4000m
        memory: 8Gi
```

## Rollback

### Rollback Deployment

```bash
# View deployment history
kubectl rollout history deployment/dagster-crypto-location -n dagster

# Rollback to previous version
kubectl rollout undo deployment/dagster-crypto-location -n dagster

# Rollback to specific revision
kubectl rollout undo deployment/dagster-crypto-location -n dagster --to-revision=2
```

### Rollback Image

```bash
# Update image tag in kustomization.yaml
images:
  - name: your-registry/dagster-crypto
    newTag: v0.0.9  # Previous version

# Apply
kubectl apply -k apps/dagster/overlays/prod
```

## Troubleshooting

### Code Location Not Loading

```bash
# Check pod status
kubectl get pods -n dagster -l app=dagster-crypto

# Check logs
kubectl logs -n dagster -l app=dagster-crypto --tail=100

# Describe pod
kubectl describe pod -n dagster -l app=dagster-crypto

# Check events
kubectl get events -n dagster --sort-by='.lastTimestamp'
```

### Job Pods Not Starting

```bash
# Check job pods
kubectl get pods -n dagster -l dagster/job

# Check events
kubectl get events -n dagster --field-selector involvedObject.kind=Pod

# Check image pull
kubectl describe pod -n dagster <job-pod-name>
```

### Database Connection Issues

```bash
# Test PostgreSQL connection from pod
kubectl exec -n dagster deployment/dagster-crypto-location -- \
  psql -h postgresql.database.svc.cluster.local -U crypto_user -d crypto_data -c "SELECT 1;"

# Check secrets
kubectl get secret dagster-crypto-secrets -n dagster -o yaml
```

### MinIO Connection Issues

```bash
# Test MinIO connection
kubectl exec -n dagster deployment/dagster-crypto-location -- \
  python -c "import boto3; s3 = boto3.client('s3', endpoint_url='http://minio.lxc.svc.cluster.local:9000'); print(s3.list_buckets())"
```

## Maintenance

### Update Application

```bash
# 1. Build and push new image
docker build -t your-registry/dagster-crypto:v0.2.0 .
docker push your-registry/dagster-crypto:v0.2.0

# 2. Update kustomization.yaml
images:
  - name: your-registry/dagster-crypto
    newTag: v0.2.0

# 3. Apply changes
kubectl apply -k apps/dagster/overlays/prod

# 4. Verify rollout
kubectl rollout status deployment/dagster-crypto-location -n dagster
```

### Restart Deployment

```bash
# Restart code location
kubectl rollout restart deployment/dagster-crypto-location -n dagster

# Force reload code location in Dagster UI
# Go to Deployment > Code Locations > Reload
```

### Clean Up Old Job Pods

```bash
# Delete completed job pods
kubectl delete pods -n dagster -l dagster/job --field-selector=status.phase=Succeeded

# Delete failed job pods
kubectl delete pods -n dagster -l dagster/job --field-selector=status.phase=Failed
```

## Backup and Restore

### Backup PostgreSQL

```bash
# Backup database
kubectl exec -n database deployment/postgresql -- \
  pg_dump -U crypto_user crypto_data > backup-$(date +%Y%m%d).sql

# Upload to S3
aws s3 cp backup-$(date +%Y%m%d).sql s3://backups/dagster-crypto/
```

### Backup MinIO

```bash
# Mirror bucket to backup location
mc mirror minio-prod/crypto-raw s3://backups/crypto-raw/
```

### Restore

```bash
# Restore PostgreSQL
kubectl exec -i -n database deployment/postgresql -- \
  psql -U crypto_user crypto_data < backup-20260101.sql

# Restore MinIO
mc mirror s3://backups/crypto-raw/ minio-prod/crypto-raw/
```

---

**Next Steps**: See [Troubleshooting](troubleshooting.md) for common issues and solutions.
