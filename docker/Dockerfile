# docker/Dockerfile
# ------------------
# Multi-stage build for the ingestion pipeline.
# Stage 1 (builder): installs dependencies into a virtual environment.
# Stage 2 (runtime): copies only the venv — no build tools in production image.

# ─── Stage 1: builder ────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# Install build dependencies (some Python packages need gcc)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --upgrade pip \
    && /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

# ─── Stage 2: runtime ────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

# Security: run as non-root user
RUN useradd --create-home --shell /bin/bash pipeline
USER pipeline
WORKDIR /app

# Copy venv from builder (no gcc or build tools here)
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy application source
COPY --chown=pipeline:pipeline ingestion/ ./ingestion/
COPY --chown=pipeline:pipeline dbt_project/ ./dbt_project/

# Default entrypoint: run ingestion pipeline for yesterday
ENTRYPOINT ["python", "-m", "ingestion.pipeline"]
CMD []

# Usage examples:
#   docker build -f docker/Dockerfile -t raw-to-gold .
#   docker run --env-file .env raw-to-gold
#   docker run --env-file .env raw-to-gold --date 2024-06-01 --dry-run
