# Kubernetes Deployment for Dagster Crypto Data Code Location

This directory contains Kubernetes manifests for deploying the Dagster crypto data code location using the **Helm chart override pattern** with Kustomize.

## 📋 Prerequisites

- Kubernetes cluster (1.24+)
- Main Dagster instance deployed (see [dagster-deployment](https://github.com/arookieds/dagster-deployment))
- PostgreSQL database accessible at `postgresql.database.svc.cluster.local`
- MinIO accessible at `minio.lxc.local:9000`
- `kubectl` configured to access your cluster
- `kubeseal` for sealed secrets
- `sops` for secret management

## 🏗️ Architecture

This code location uses the **dagster-user-deployments** Helm chart, following the same pattern as your main Dagster deployment:

```
┌─────────────────────────────────────────┐
│   Main Dagster Instance (dagster ns)   │
│  ┌──────────────┐   ┌───────────────┐  │
│  │  Webserver   │   │    Daemon     │  │
│  └──────┬───────┘   └───────┬───────┘  │
│         │                   │          │
│         └───────┬───────────┘          │
│                 │ gRPC                 │
│         ┌───────▼───────────┐          │
│         │  Code Location    │          │
│         │  (Helm Chart)     │          │
│         │  Port: 3030       │          │
│         └───────────────────┘          │
└─────────────────────────────────────────┘
```

**Key Differences from Raw Manifests:**
- ✅ Uses Helm chart for standardized deployment
- ✅ No manual Deployment/Service YAML files
- ✅ Helm handles service creation and pod management
- ✅ Consistent with Dagster best practices

## 🚀 Quick Start

### 1. Create Sealed Secrets

```bash
# Create .env file with your credentials
cp .env.example .env
# Edit .env with actual values (DB_USERNAME, DB_PASSWORD, S3_USER, S3_PASSWORD)

# Generate sealed secrets
sops exec-env .env "nu create_sealed_secrets.nu"

# Update kustomization.yaml to reference sealed secrets
# Uncomment the line: - sealed-secrets/dagster-crypto-secrets-sealed.yaml
```

### 2. Build and Push Docker Image

```bash
# Build the image
docker build -t ghcr.io/arookieds/dagster-code-location-crypto-data:latest .

# Push to GitHub Container Registry
docker push ghcr.io/arookieds/dagster-code-location-crypto-data:latest
```

**Note:** GitHub Container Registry (ghcr.io) is free for public repositories. You'll need to:
1. Create a Personal Access Token (PAT) with `write:packages` scope
2. Login: `echo $GITHUB_TOKEN | docker login ghcr.io -u USERNAME --password-stdin`

### 3. Deploy to Kubernetes

```bash
# Deploy using Kustomize with Helm chart
kubectl apply -k manifests/

# Verify deployment
kubectl get pods -n dagster -l app=crypto-data
kubectl logs -n dagster -l app=crypto-data -f
```

### 4. Register Code Location with Main Dagster Instance

Update the main Dagster deployment's `kustomization.yaml` workspace configuration:

```yaml
# In ~/dev/devops/homelab/talos/dagster-deployment/manifests/kustomization.yaml

dagster-webserver:
  workspace:
    servers:
      - host: "crypto-data-crypto-data"  # Format: {releaseName}-{deploymentName}
        port: 3030
        name: "crypto-data"
```

Then redeploy the main Dagster instance:

```bash
cd ~/dev/devops/homelab/talos/dagster-deployment
kubectl apply -k manifests/
```

## 📁 File Structure

```
manifests/
├── kustomization.yaml              # Helm chart override configuration
├── configmap.yaml                  # Non-sensitive configuration
├── create_sealed_secrets.nu        # Script to generate sealed secrets
├── .env.example                    # Example environment variables
├── README.md                       # This file
└── sealed-secrets/                 # Generated sealed secrets (gitignored)
    └── dagster-crypto-secrets-sealed.yaml
```

**What's Different from Raw Manifests:**
- ❌ No `deployment.yaml` - Helm chart creates it
- ❌ No `service.yaml` - Helm chart creates it
- ❌ No `secrets.yaml` - Replaced by sealed secrets
- ✅ Single `kustomization.yaml` with Helm overrides

## 🔧 Configuration

### Environment Variables (ConfigMap)

Mapped to `Settings` class in `src/dagster_crypto_data/defs/utils/settings.py`:

| ConfigMap Key | Settings Field | Description | Default |
|---------------|----------------|-------------|---------|
| `LOG_LEVEL` | `log_level` | Logging verbosity | `INFO` |
| `IS_PRODUCTION` | `is_production` | Environment flag | `true` |
| `DB_HOST` | `db_host` | PostgreSQL hostname | `postgresql.database.svc.cluster.local` |
| `DB_PORT` | `db_port` | PostgreSQL port | `5432` |
| `DB_NAME` | `db_name` | Database name | `crypto` |
| `S3_URL` | `s3_url` | MinIO endpoint | `http://minio.lxc.local:9000` |
| `S3_BUCKET` | `s3_bucket` | S3 bucket name | `crypto-raw-data` |

### Secrets (Sealed)

| Secret Key | Settings Field | Description |
|------------|----------------|-------------|
| `DB_USERNAME` | `db_username` | PostgreSQL username |
| `DB_PASSWORD` | `db_password` | PostgreSQL password (SecretStr) |
| `S3_USER` | `s3_user` | MinIO access key |
| `S3_PASSWORD` | `s3_password` | MinIO secret key (SecretStr) |

## 🔍 Troubleshooting

### Pod Not Starting

```bash
# Check pod status
kubectl get pods -n dagster -l app=crypto-data

# View pod events
kubectl describe pod -n dagster -l app=crypto-data

# Check logs
kubectl logs -n dagster -l app=crypto-data --tail=100
```

### Code Location Not Appearing in Dagster UI

1. **Check service name format:**
   ```bash
   kubectl get svc -n dagster | grep crypto-data
   ```
   Service name should be: `crypto-data-crypto-data` (format: `{releaseName}-{deploymentName}`)

2. **Verify gRPC server is running:**
   ```bash
   kubectl exec -n dagster -it deployment/crypto-data-crypto-data -- \
     python -c "import dagster; print('OK')"
   ```

3. **Check main Dagster workspace configuration:**
   ```bash
   kubectl logs -n dagster -l app.kubernetes.io/component=dagster-webserver | grep workspace
   ```

### Helm Chart Issues

```bash
# View generated manifests without applying
kubectl kustomize manifests/

# Check Helm chart values
kubectl get deployment -n dagster crypto-data-crypto-data -o yaml
```

## 🔐 Security Best Practices

1. **Never commit plain secrets** - Use sealed secrets exclusively
2. **Use least privilege** - Database user should only access `crypto` database
3. **Scan images** - Run `docker scan` before pushing to registry
4. **Resource limits** - Always set CPU/memory limits (already configured)
5. **Non-root user** - Runs as UID 1000 (already configured)

## 📊 Resource Requirements

| Resource | Request | Limit |
|----------|---------|-------|
| CPU | 100m | 500m |
| Memory | 256Mi | 512Mi |

Adjust in `kustomization.yaml` under `resources` section if needed.

## 🔄 Updates and Maintenance

### Update Image

```bash
# Build new image with version tag
docker build -t ghcr.io/arookieds/dagster-code-location-crypto-data:v1.0.0 .
docker push ghcr.io/arookieds/dagster-code-location-crypto-data:v1.0.0

# Update kustomization.yaml image tag
# Change: tag: "latest" to tag: "v1.0.0"

# Apply changes
kubectl apply -k manifests/
```

### Rollback

```bash
# View rollout history
kubectl rollout history deployment/crypto-data-crypto-data -n dagster

# Rollback to previous version
kubectl rollout undo deployment/crypto-data-crypto-data -n dagster
```

### Update Helm Chart Version

```bash
# Edit kustomization.yaml and change version: 1.12.6 to newer version
# Then apply
kubectl apply -k manifests/
```

## 📚 References

- [Dagster User Code Deployments](https://docs.dagster.io/deployment/guides/kubernetes/deploying-with-helm-advanced#user-code-deployments)
- [Dagster Helm Charts](https://github.com/dagster-io/dagster/tree/master/helm)
- [Kustomize with Helm](https://kubectl.docs.kubernetes.io/references/kustomize/builtins/#_helmchartinflationgenerator_)
- [GitHub Container Registry](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry)
- [Sealed Secrets](https://github.com/bitnami-labs/sealed-secrets)

## 📝 License

Apache 2.0 - See [LICENSE](../LICENSE) for details.
