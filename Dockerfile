# 1. NATIVE BUILDER (Mac/ARM64)
FROM --platform=$BUILDPLATFORM ghcr.io/astral-sh/uv:latest AS uv_bin
FROM --platform=$BUILDPLATFORM python:3.11-slim AS builder

WORKDIR /app
COPY --from=uv_bin /uv /bin/uv
COPY pyproject.toml uv.lock ./

# Download AMD64 wheels natively; skip project installation to avoid build errors
RUN uv sync --frozen --no-dev --no-install-project --python-platform x86_64-unknown-linux-gnu

# 2. TARGET RUNNER (Linux/AMD64)
FROM --platform=$BUILDPLATFORM python:3.11-slim

WORKDIR /app

# Copy the venv (the libraries are fine, but the 'bin' folder is broken)
COPY --from=builder /app/.venv /app/.venv
COPY src ./src

# CRITICAL: Do NOT use the venv's PATH for binaries. 
# Instead, point the system Python to the venv's library folder.
ENV PYTHONPATH="/app/.venv/lib/python3.11/site-packages" \
    PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

# Use a non-root user for security (UID 1000 is standard)
USER 1000
