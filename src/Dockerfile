# syntax=docker/dockerfile:1
FROM python:3.12-slim AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files first for layer caching
COPY pyproject.toml .
COPY pg_mcp/ pg_mcp/

# Install the package
RUN pip install --no-cache-dir -e .

# Runtime stage
FROM python:3.12-slim AS runtime

WORKDIR /app

# Create non-root user
RUN groupadd -r pg-mcp && useradd -r -g pg-mcp pg-mcp

# Copy installed package from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin/pg-mcp /usr/local/bin/pg-mcp
COPY --from=builder /app/pg_mcp /app/pg_mcp

# Switch to non-root user
USER pg-mcp

EXPOSE 8000

ENTRYPOINT ["pg-mcp"]
CMD ["--transport", "sse"]
