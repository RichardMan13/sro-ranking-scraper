FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    HOME=/home/appuser \
    PREFECT_HOME=/home/appuser/.prefect

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        gcc \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src/ ./src/

RUN pip install --no-cache-dir .

COPY alembic.ini ./
COPY alembic/ ./alembic/

RUN groupadd --gid 1000 appuser \
    && useradd --uid 1000 --gid 1000 --create-home --shell /bin/bash appuser \
    && mkdir -p /app/quarantine /home/appuser/.prefect \
    && chown -R appuser:appuser /app /home/appuser

USER appuser

CMD ["python", "-m", "src.flows.orchestrator", "--mode", "serve"]
