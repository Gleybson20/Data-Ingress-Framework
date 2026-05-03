FROM python:3.11-slim AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --upgrade pip \
    && /opt/venv/bin/pip install --no-cache-dir -r requirements.txt
FROM python:3.11-slim AS runtime
RUN useradd --create-home --shell /bin/bash pipeline
USER pipeline
WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
COPY --chown=pipeline:pipeline ingestion/ ./ingestion/
COPY --chown=pipeline:pipeline dbt_project/ ./dbt_project/
ENTRYPOINT ["python", "-m", "ingestion.pipeline"]
CMD []
