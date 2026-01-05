# GitHub Actions Workflows

This directory contains CI/CD workflows for the Dagster Crypto Data project.

## Workflows

### `ci.yml` - CI/CD Pipeline

**Purpose:** Comprehensive pipeline for linting, testing, and building Docker images.

**Triggers:**
- Push to `main` or `dev` branches
- Pull requests to `main` or `dev` branches
- Version tags (e.g., `v1.0.0`, `v1.2.3`)

**Jobs:**

```
┌─────────┐
│  Lint   │  Ruff + Mypy
└────┬────┘
     │
     ▼
┌─────────┐
│  Test   │  Pytest with coverage
└────┬────┘
     │
     ▼
┌─────────┐
│  Build  │  Docker image (only on tags)
└─────────┘
```

#### 1. Lint Job
- **Runs on:** All pushes and PRs
- **Steps:**
  - Ruff linter check
  - Ruff formatter check
  - Mypy type checking
- **Fails if:** Any linting or type errors

#### 2. Test Job
- **Runs on:** All pushes and PRs (only if lint passes)
- **Depends on:** `lint` job
- **Steps:**
  - Run pytest with coverage
  - Upload coverage to Codecov (optional)
- **Fails if:** Any tests fail

#### 3. Build Image Job
- **Runs on:** Version tags only (e.g., `v1.0.0`)
- **Depends on:** `test` job
- **Steps:**
  - Build Docker image
  - Push to GitHub Container Registry (ghcr.io)
  - Tag with version and `latest`
- **Fails if:** Build fails or push fails

## Image Tagging Strategy

When you create a version tag (e.g., `v1.2.3`), the workflow creates multiple tags:

| Tag Pattern | Example | Description |
|-------------|---------|-------------|
| `{{version}}` | `1.2.3` | Full semantic version |
| `{{major}}.{{minor}}` | `1.2` | Major + minor version |
| `{{major}}` | `1` | Major version only |
| `latest` | `latest` | Latest stable release |

**Example:**
```bash
git tag v1.2.3
git push origin v1.2.3
```

This creates images:
- `ghcr.io/arookieds/dagster-code-location-crypto-data:1.2.3`
- `ghcr.io/arookieds/dagster-code-location-crypto-data:1.2`
- `ghcr.io/arookieds/dagster-code-location-crypto-data:1`
- `ghcr.io/arookieds/dagster-code-location-crypto-data:latest`

## How to Trigger a Build

### For Development (No Image Build)
```bash
# Push to dev branch - runs lint + test only
git push origin dev
```

### For Production Release
```bash
# 1. Ensure you're on main branch
git checkout main
git pull origin main

# 2. Create a version tag
git tag v1.0.0

# 3. Push the tag
git push origin v1.0.0

# This triggers: lint → test → build-image
```

## Secrets Required

### Automatic (Provided by GitHub)
- `GITHUB_TOKEN` - Used to push images to GHCR (automatically available)

### Optional
- `CODECOV_TOKEN` - For uploading coverage reports to Codecov
  - Get from: https://codecov.io/
  - Add to: Repository Settings → Secrets → Actions → New repository secret

## Permissions

The workflow requires these permissions (already configured):
- `contents: read` - Read repository code
- `packages: write` - Push to GitHub Container Registry

## Monitoring Workflow Runs

1. Go to: https://github.com/arookieds/dagster-code-location-crypto-data/actions
2. Click on a workflow run to see details
3. Each job shows logs and status

## Troubleshooting

### Lint Job Fails
```bash
# Run locally to fix issues
uv run ruff check --fix .
uv run ruff format .
uv run mypy src
```

### Test Job Fails
```bash
# Run locally to debug
uv run pytest -v
```

### Build Job Fails
```bash
# Test build locally
podman build -t test:latest .

# Check if image works
podman run --rm test:latest python -c "import dagster; print('OK')"
```

### Image Not Appearing in GHCR
1. Check workflow logs for errors
2. Verify package visibility is "Public" in GitHub
3. Ensure tag format is correct (must start with `v`)

## Best Practices

1. **Always test locally first:**
   ```bash
   uv run ruff check .
   uv run mypy src
   uv run pytest
   ```

2. **Use semantic versioning:**
   - `v1.0.0` - Major release (breaking changes)
   - `v1.1.0` - Minor release (new features)
   - `v1.1.1` - Patch release (bug fixes)

3. **Create releases on GitHub:**
   - After tagging, create a GitHub Release
   - Add release notes describing changes
   - Link to the Docker image

4. **Monitor workflow runs:**
   - Check Actions tab after pushing
   - Fix failures immediately
   - Don't merge PRs with failing checks

## Future Enhancements

Potential improvements for later:

- [ ] Split into separate workflow files (lint.yml, test.yml, build.yml)
- [ ] Add deployment job for Kubernetes
- [ ] Add security scanning (Trivy, Snyk)
- [ ] Add SBOM generation
- [ ] Add multi-platform builds (arm64)
- [ ] Add workflow for manual deployments
- [ ] Add scheduled dependency updates (Dependabot)

## References

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Docker Build Push Action](https://github.com/docker/build-push-action)
- [GitHub Container Registry](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry)
