# Multi-stage build for DIRA Pipeline
FROM python:3.13-slim as base

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY . .

# Install Python dependencies
RUN python -m pip install --upgrade pip && \
    python -m pip install hatch && \
    python -m hatch env create

FROM base as development

# Development image with test dependencies
RUN python -m pip install pytest pytest-cov ruff mypy

EXPOSE 8000 5432

CMD ["/bin/bash"]

FROM base as production

# Production image - minimal runtime
RUN python -m pip install gunicorn

EXPOSE 8000

CMD ["python", "-m", "apps.pipeline.main"]
